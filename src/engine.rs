use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use memchr::{memchr, memmem};
use regex::bytes::RegexBuilder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::index::{self, BuildProgress, IndexConfig, IndexError, LineIndex, SourceDescriptor};

const SEARCH_READER_BYTES: usize = 1024 * 1024;
const EXPORT_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const REGEX_COMPILED_SIZE_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub index_dir: PathBuf,
    pub export_root: PathBuf,
    pub checkpoint_bytes: u64,
    pub max_read_lines: u64,
    pub max_search_lines: u64,
    pub max_matches: u64,
    pub max_content_bytes: usize,
    pub max_line_bytes: usize,
    pub max_pattern_bytes: usize,
    pub max_export_lines: u64,
    pub query_concurrency: usize,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{0}")]
    Index(#[from] IndexError),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("file is not ready: {0}")]
    NotReady(String),
    #[error("blocking task failed: {0}")]
    Join(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct OpenFileRequest {
    /// Path of the single file to make active.
    pub path: String,
    /// Ignore an existing valid index and build it again.
    #[serde(default)]
    pub force_rebuild: bool,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ReadLinesRequest {
    /// One-based first line.
    pub start_line: u64,
    /// Maximum number of lines to return.
    pub line_count: u64,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SearchLinesRequest {
    /// Regex or literal text to find.
    pub pattern: String,
    /// One-based first line to inspect.
    pub start_line: u64,
    /// Maximum number of lines to inspect.
    pub max_scan_lines: u64,
    /// Maximum matching lines to return. Defaults to 100.
    pub max_matches: Option<u64>,
    /// Interpret pattern as a Rust regex. Defaults to false (literal search).
    pub regex: Option<bool>,
    /// Defaults to true.
    pub case_sensitive: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ExportLinesRequest {
    /// One-based first line.
    pub start_line: u64,
    /// Maximum number of lines to copy.
    pub line_count: u64,
    /// Absolute path under export_root, or a path relative to export_root.
    pub output_path: String,
    /// Atomically replace an existing regular file.
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct FileStatusResponse {
    pub state: String,
    pub path: Option<String>,
    pub index_path: Option<String>,
    pub source_size: Option<u64>,
    pub bytes_indexed: Option<u64>,
    pub progress_percent: Option<f64>,
    pub lines_seen: Option<u64>,
    pub line_count: Option<u64>,
    pub checkpoint_count: Option<u64>,
    pub elapsed_seconds: Option<f64>,
    pub bytes_per_second: Option<f64>,
    pub eta_seconds: Option<f64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct OpenFileResponse {
    pub index_reused: bool,
    pub status: FileStatusResponse,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ReadLinesResponse {
    pub start_line: u64,
    pub end_line: Option<u64>,
    pub returned_lines: u64,
    pub source_bytes: u64,
    pub content: String,
    pub lossy_utf8: bool,
    pub next_line: Option<u64>,
    pub eof: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SearchMatch {
    pub line: u64,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SearchLinesResponse {
    pub mode: String,
    pub start_line: u64,
    pub scanned_lines: u64,
    pub matches: Vec<SearchMatch>,
    pub lossy_utf8: bool,
    pub next_line: Option<u64>,
    pub eof: bool,
    pub scan_limit_reached: bool,
    pub match_limit_reached: bool,
    pub content_limit_reached: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ExportLinesResponse {
    pub start_line: u64,
    pub end_line: Option<u64>,
    pub exported_lines: u64,
    pub bytes_written: u64,
    pub output_path: String,
    pub eof: bool,
}

#[derive(Debug)]
struct OpenedFile {
    source: SourceDescriptor,
    index: Arc<LineIndex>,
}

#[derive(Debug, Clone)]
struct IndexingState {
    job_id: u64,
    source: SourceDescriptor,
    index_path: PathBuf,
    progress: Arc<BuildProgress>,
    started: Instant,
}

#[derive(Debug, Clone)]
struct FailedState {
    source: Option<SourceDescriptor>,
    index_path: Option<PathBuf>,
    error: String,
}

#[derive(Debug, Clone)]
enum ActiveState {
    Empty,
    Indexing(IndexingState),
    Ready(Arc<OpenedFile>),
    Failed(FailedState),
}

pub struct FileEngine {
    config: EngineConfig,
    state: RwLock<ActiveState>,
    transition: Mutex<()>,
    next_job_id: AtomicU64,
    query_slots: Semaphore,
}

impl FileEngine {
    pub fn new(config: EngineConfig) -> Self {
        let query_concurrency = config.query_concurrency;
        Self {
            config,
            state: RwLock::new(ActiveState::Empty),
            transition: Mutex::new(()),
            next_job_id: AtomicU64::new(1),
            query_slots: Semaphore::new(query_concurrency),
        }
    }

    pub async fn open_file(self: &Arc<Self>, request: OpenFileRequest) -> Result<OpenFileResponse> {
        if request.path.trim().is_empty() {
            return Err(EngineError::InvalidRequest(
                "path must not be empty".to_owned(),
            ));
        }

        let _transition = self.transition.lock().await;
        let requested_path = PathBuf::from(request.path);
        let source = run_blocking(move || index::inspect_source(&requested_path)).await??;

        match self.state.read().await.clone() {
            ActiveState::Indexing(current) => {
                if current.source.canonical_path == source.canonical_path && !request.force_rebuild
                {
                    return Ok(OpenFileResponse {
                        index_reused: false,
                        status: self.status().await,
                    });
                }
                return Err(EngineError::InvalidRequest(format!(
                    "{} is already being indexed; call close_file before opening another file",
                    current.source.canonical_path.display()
                )));
            }
            ActiveState::Ready(current) => {
                if current.source.canonical_path != source.canonical_path {
                    return Err(EngineError::InvalidRequest(format!(
                        "{} is already open; call close_file before opening another file",
                        current.source.canonical_path.display()
                    )));
                }
                if current.source.identity == source.identity && !request.force_rebuild {
                    return Ok(OpenFileResponse {
                        index_reused: true,
                        status: self.status().await,
                    });
                }
            }
            ActiveState::Empty | ActiveState::Failed(_) => {}
        }

        let index_config = self.index_config();
        if !request.force_rebuild {
            let source_for_load = source.clone();
            let config_for_load = index_config.clone();
            match run_blocking(move || index::load_existing(&source_for_load, &config_for_load))
                .await?
            {
                Ok(Some(index)) => {
                    *self.state.write().await = ActiveState::Ready(Arc::new(OpenedFile {
                        source,
                        index: Arc::new(index),
                    }));
                    return Ok(OpenFileResponse {
                        index_reused: true,
                        status: self.status().await,
                    });
                }
                Ok(None) => {}
                Err(IndexError::Corrupt(error)) => {
                    tracing::warn!(%error, "valid source has a corrupt index; rebuilding");
                }
                Err(error) => return Err(error.into()),
            }
        }

        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let cancellation = CancellationToken::new();
        let progress = Arc::new(BuildProgress::new(cancellation));
        let index_path = index::index_paths(&source, &index_config).final_path;
        *self.state.write().await = ActiveState::Indexing(IndexingState {
            job_id,
            source: source.clone(),
            index_path,
            progress: progress.clone(),
            started: Instant::now(),
        });

        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let source_for_build = source.clone();
            let progress_for_build = progress.clone();
            let result = run_blocking(move || {
                index::build_or_load(
                    &source_for_build,
                    &index_config,
                    &progress_for_build,
                    request.force_rebuild,
                )
            })
            .await;
            engine.finish_index(job_id, source, result).await;
        });

        Ok(OpenFileResponse {
            index_reused: false,
            status: self.status().await,
        })
    }

    pub async fn close_file(&self) -> FileStatusResponse {
        let _transition = self.transition.lock().await;
        let previous = std::mem::replace(&mut *self.state.write().await, ActiveState::Empty);
        if let ActiveState::Indexing(indexing) = previous {
            indexing.progress.cancellation_token().cancel();
        }
        self.status().await
    }

    pub async fn shutdown(&self) {
        self.close_file().await;
    }

    pub async fn status(&self) -> FileStatusResponse {
        match self.state.read().await.clone() {
            ActiveState::Empty => FileStatusResponse {
                state: "no_file".to_owned(),
                path: None,
                index_path: None,
                source_size: None,
                bytes_indexed: None,
                progress_percent: None,
                lines_seen: None,
                line_count: None,
                checkpoint_count: None,
                elapsed_seconds: None,
                bytes_per_second: None,
                eta_seconds: None,
                error: None,
            },
            ActiveState::Indexing(indexing) => {
                let bytes = indexing.progress.bytes_indexed();
                let size = indexing.source.identity.size;
                let elapsed = indexing.started.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    Some(bytes as f64 / elapsed)
                } else {
                    None
                };
                let eta = rate.and_then(|rate| {
                    if rate > 0.0 && bytes < size {
                        Some((size - bytes) as f64 / rate)
                    } else {
                        None
                    }
                });
                FileStatusResponse {
                    state: "indexing".to_owned(),
                    path: Some(path_string(&indexing.source.canonical_path)),
                    index_path: Some(path_string(&indexing.index_path)),
                    source_size: Some(size),
                    bytes_indexed: Some(bytes),
                    progress_percent: Some(if size == 0 {
                        100.0
                    } else {
                        (bytes as f64 * 100.0 / size as f64).clamp(0.0, 100.0)
                    }),
                    lines_seen: Some(indexing.progress.lines_seen()),
                    line_count: None,
                    checkpoint_count: Some(indexing.progress.checkpoints()),
                    elapsed_seconds: Some(elapsed),
                    bytes_per_second: rate,
                    eta_seconds: eta,
                    error: None,
                }
            }
            ActiveState::Ready(opened) => FileStatusResponse {
                state: "ready".to_owned(),
                path: Some(path_string(&opened.source.canonical_path)),
                index_path: Some(path_string(&opened.index.index_path)),
                source_size: Some(opened.source.identity.size),
                bytes_indexed: Some(opened.source.identity.size),
                progress_percent: Some(100.0),
                lines_seen: Some(opened.index.line_count),
                line_count: Some(opened.index.line_count),
                checkpoint_count: Some(opened.index.entries.len() as u64),
                elapsed_seconds: None,
                bytes_per_second: None,
                eta_seconds: None,
                error: None,
            },
            ActiveState::Failed(failed) => FileStatusResponse {
                state: "failed".to_owned(),
                path: failed
                    .source
                    .as_ref()
                    .map(|source| path_string(&source.canonical_path)),
                index_path: failed.index_path.as_ref().map(|path| path_string(path)),
                source_size: failed.source.as_ref().map(|source| source.identity.size),
                bytes_indexed: None,
                progress_percent: None,
                lines_seen: None,
                line_count: None,
                checkpoint_count: None,
                elapsed_seconds: None,
                bytes_per_second: None,
                eta_seconds: None,
                error: Some(failed.error),
            },
        }
    }

    pub async fn read_lines(&self, request: ReadLinesRequest) -> Result<ReadLinesResponse> {
        if request.start_line == 0 {
            return Err(EngineError::InvalidRequest(
                "start_line is one-based and must be at least 1".to_owned(),
            ));
        }
        if request.line_count > self.config.max_read_lines {
            return Err(EngineError::InvalidRequest(format!(
                "line_count exceeds server limit {}",
                self.config.max_read_lines
            )));
        }

        let opened = self.ready_file().await?;
        let _permit = self
            .query_slots
            .acquire()
            .await
            .map_err(|_| EngineError::NotReady("server is shutting down".to_owned()))?;
        let max_content_bytes = self.config.max_content_bytes;
        let opened_for_work = opened.clone();
        let result =
            run_blocking(move || read_lines_blocking(&opened_for_work, request, max_content_bytes))
                .await?;
        self.finish_query(&opened, result).await
    }

    pub async fn search_lines(&self, request: SearchLinesRequest) -> Result<SearchLinesResponse> {
        if request.start_line == 0 {
            return Err(EngineError::InvalidRequest(
                "start_line is one-based and must be at least 1".to_owned(),
            ));
        }
        if request.max_scan_lines > self.config.max_search_lines {
            return Err(EngineError::InvalidRequest(format!(
                "max_scan_lines exceeds server limit {}",
                self.config.max_search_lines
            )));
        }
        if request.pattern.is_empty() {
            return Err(EngineError::InvalidRequest(
                "pattern must not be empty".to_owned(),
            ));
        }
        if request.pattern.len() > self.config.max_pattern_bytes {
            return Err(EngineError::InvalidRequest(format!(
                "pattern exceeds server limit {} bytes",
                self.config.max_pattern_bytes
            )));
        }
        let requested_matches = request.max_matches.unwrap_or(100);
        if requested_matches == 0 || requested_matches > self.config.max_matches {
            return Err(EngineError::InvalidRequest(format!(
                "max_matches must be between 1 and {}",
                self.config.max_matches
            )));
        }

        let opened = self.ready_file().await?;
        let _permit = self
            .query_slots
            .acquire()
            .await
            .map_err(|_| EngineError::NotReady("server is shutting down".to_owned()))?;
        let max_line_bytes = self.config.max_line_bytes;
        let max_content_bytes = self.config.max_content_bytes;
        let opened_for_work = opened.clone();
        let result = run_blocking(move || {
            search_lines_blocking(
                &opened_for_work,
                request,
                requested_matches,
                max_line_bytes,
                max_content_bytes,
            )
        })
        .await?;
        self.finish_query(&opened, result).await
    }

    pub async fn export_lines(&self, request: ExportLinesRequest) -> Result<ExportLinesResponse> {
        if request.start_line == 0 {
            return Err(EngineError::InvalidRequest(
                "start_line is one-based and must be at least 1".to_owned(),
            ));
        }
        if self.config.max_export_lines != 0 && request.line_count > self.config.max_export_lines {
            return Err(EngineError::InvalidRequest(format!(
                "line_count exceeds server export limit {}",
                self.config.max_export_lines
            )));
        }
        if request.output_path.trim().is_empty() {
            return Err(EngineError::InvalidRequest(
                "output_path must not be empty".to_owned(),
            ));
        }

        let opened = self.ready_file().await?;
        let _permit = self
            .query_slots
            .acquire()
            .await
            .map_err(|_| EngineError::NotReady("server is shutting down".to_owned()))?;
        let export_root = self.config.export_root.clone();
        let opened_for_work = opened.clone();
        let result =
            run_blocking(move || export_lines_blocking(&opened_for_work, request, &export_root))
                .await?;
        self.finish_query(&opened, result).await
    }

    fn index_config(&self) -> IndexConfig {
        IndexConfig {
            directory: self.config.index_dir.clone(),
            checkpoint_bytes: self.config.checkpoint_bytes,
        }
    }

    async fn finish_index(
        &self,
        job_id: u64,
        source: SourceDescriptor,
        task_result: Result<index::Result<LineIndex>>,
    ) {
        let result = match task_result {
            Ok(result) => result.map_err(EngineError::from),
            Err(error) => Err(error),
        };
        let mut state = self.state.write().await;
        let matches_job = matches!(
            &*state,
            ActiveState::Indexing(indexing) if indexing.job_id == job_id
        );
        if !matches_job {
            return;
        }

        match result {
            Ok(index) => {
                tracing::info!(
                    path = %source.canonical_path.display(),
                    lines = index.line_count,
                    checkpoints = index.entries.len(),
                    "file index is ready"
                );
                *state = ActiveState::Ready(Arc::new(OpenedFile {
                    source,
                    index: Arc::new(index),
                }));
            }
            Err(error) => {
                tracing::error!(path = %source.canonical_path.display(), %error, "file indexing failed");
                let index_path = index::index_paths(&source, &self.index_config()).final_path;
                *state = ActiveState::Failed(FailedState {
                    source: Some(source),
                    index_path: Some(index_path),
                    error: error.to_string(),
                });
            }
        }
    }

    async fn ready_file(&self) -> Result<Arc<OpenedFile>> {
        let opened = match self.state.read().await.clone() {
            ActiveState::Ready(opened) => opened,
            ActiveState::Empty => {
                return Err(EngineError::NotReady(
                    "no file is open; call open_file first".to_owned(),
                ));
            }
            ActiveState::Indexing(_) => {
                return Err(EngineError::NotReady(
                    "the first-pass index is still being built; poll get_file_status".to_owned(),
                ));
            }
            ActiveState::Failed(failed) => {
                return Err(EngineError::NotReady(format!(
                    "the active file failed: {}",
                    failed.error
                )));
            }
        };

        let source = opened.source.clone();
        let validation = run_blocking(move || index::ensure_source_unchanged(&source)).await?;
        match validation {
            Ok(()) => Ok(opened),
            Err(error) => {
                self.mark_failed_if_current(&opened, error.to_string())
                    .await;
                Err(error.into())
            }
        }
    }

    async fn finish_query<T>(&self, opened: &Arc<OpenedFile>, result: Result<T>) -> Result<T> {
        if matches!(&result, Err(EngineError::Index(IndexError::SourceChanged))) {
            self.mark_failed_if_current(opened, "the source file changed".to_owned())
                .await;
        }
        result
    }

    async fn mark_failed_if_current(&self, opened: &Arc<OpenedFile>, error: String) {
        let mut state = self.state.write().await;
        if matches!(&*state, ActiveState::Ready(current) if Arc::ptr_eq(current, opened)) {
            *state = ActiveState::Failed(FailedState {
                source: Some(opened.source.clone()),
                index_path: Some(opened.index.index_path.clone()),
                error,
            });
        }
    }
}

async fn run_blocking<F, T>(work: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| EngineError::Join(error.to_string()))
}

fn read_lines_blocking(
    opened: &OpenedFile,
    request: ReadLinesRequest,
    max_content_bytes: usize,
) -> Result<ReadLinesResponse> {
    index::ensure_source_unchanged(&opened.source)?;
    let selected = selected_line_count(
        opened.index.line_count,
        request.start_line,
        request.line_count,
    )?;
    let end_exclusive = request.start_line + selected;
    let mut file = index::open_random(&opened.source.canonical_path)?;
    let start_offset = offset_for_line_or_eof(&mut file, &opened.index, request.start_line)?;
    let end_offset = if selected == 0 {
        start_offset
    } else {
        index::locate_line_offset(&mut file, &opened.index, end_exclusive)?
    };
    let byte_count = end_offset - start_offset;
    if byte_count > max_content_bytes as u64 {
        return Err(EngineError::InvalidRequest(format!(
            "selected content is {byte_count} bytes, above the MCP response limit {max_content_bytes}; use export_lines or request fewer lines"
        )));
    }

    file.seek(SeekFrom::Start(start_offset))?;
    let mut bytes = vec![0_u8; byte_count as usize];
    file.read_exact(&mut bytes)?;
    index::ensure_source_unchanged(&opened.source)?;
    let (content, lossy_utf8) = decode_lossy(bytes);
    let next_line = if end_exclusive <= opened.index.line_count {
        Some(end_exclusive)
    } else {
        None
    };

    Ok(ReadLinesResponse {
        start_line: request.start_line,
        end_line: selected
            .checked_sub(1)
            .map(|delta| request.start_line + delta),
        returned_lines: selected,
        source_bytes: byte_count,
        content,
        lossy_utf8,
        next_line,
        eof: next_line.is_none(),
    })
}

fn search_lines_blocking(
    opened: &OpenedFile,
    request: SearchLinesRequest,
    max_matches: u64,
    max_line_bytes: usize,
    max_content_bytes: usize,
) -> Result<SearchLinesResponse> {
    index::ensure_source_unchanged(&opened.source)?;
    let selected = selected_line_count(
        opened.index.line_count,
        request.start_line,
        request.max_scan_lines,
    )?;
    let scan_end = request.start_line + selected;
    let regex_mode = request.regex.unwrap_or(false);
    let case_sensitive = request.case_sensitive.unwrap_or(true);
    let literal_finder =
        (!regex_mode && case_sensitive).then(|| memmem::Finder::new(request.pattern.as_bytes()));
    let regex_matcher = if literal_finder.is_none() {
        let expression = if regex_mode {
            request.pattern.clone()
        } else {
            regex::escape(&request.pattern)
        };
        let mut builder = RegexBuilder::new(&expression);
        builder
            .case_insensitive(!case_sensitive)
            .size_limit(REGEX_COMPILED_SIZE_LIMIT);
        Some(
            builder
                .build()
                .map_err(|error| EngineError::InvalidRequest(format!("invalid regex: {error}")))?,
        )
    } else {
        None
    };

    let mut file = index::open_random(&opened.source.canonical_path)?;
    let start_offset = offset_for_line_or_eof(&mut file, &opened.index, request.start_line)?;
    file.seek(SeekFrom::Start(start_offset))?;
    let mut reader = BufReader::with_capacity(SEARCH_READER_BYTES, file);
    let mut line_bytes = Vec::new();
    let mut matches = Vec::new();
    let mut line_number = request.start_line;
    let mut scanned_lines = 0_u64;
    let mut content_bytes = 0_usize;
    let mut lossy_utf8 = false;
    let mut match_limit_reached = false;
    let mut content_limit_reached = false;
    let mut resume_line = None;

    while line_number < scan_end {
        let bytes_read =
            read_bounded_line(&mut reader, &mut line_bytes, max_line_bytes).map_err(|error| {
                if error.kind() == io::ErrorKind::InvalidData {
                    EngineError::InvalidRequest(format!(
                        "line {line_number} exceeds max-line-bytes {max_line_bytes}"
                    ))
                } else {
                    EngineError::Io(error)
                }
            })?;
        if bytes_read == 0 {
            return Err(IndexError::SourceChanged.into());
        }

        let content_bytes_slice = without_line_ending(&line_bytes);
        let is_match = literal_finder
            .as_ref()
            .is_some_and(|finder| finder.find(content_bytes_slice).is_some())
            || regex_matcher
                .as_ref()
                .is_some_and(|regex| regex.is_match(content_bytes_slice));

        if is_match {
            let line_was_lossy = std::str::from_utf8(content_bytes_slice).is_err();
            let content = String::from_utf8_lossy(content_bytes_slice).into_owned();
            if content_bytes.saturating_add(content.len()) > max_content_bytes {
                if matches.is_empty() {
                    return Err(EngineError::InvalidRequest(format!(
                        "matching line {line_number} exceeds max-content-bytes {max_content_bytes}"
                    )));
                }
                content_limit_reached = true;
                resume_line = Some(line_number);
                break;
            }
            content_bytes += content.len();
            lossy_utf8 |= line_was_lossy;
            matches.push(SearchMatch {
                line: line_number,
                content,
            });
        }

        scanned_lines += 1;
        line_number += 1;
        if matches.len() as u64 >= max_matches {
            match_limit_reached = line_number <= opened.index.line_count;
            break;
        }
    }

    index::ensure_source_unchanged(&opened.source)?;
    let next_line = resume_line.or_else(|| {
        if line_number <= opened.index.line_count {
            Some(line_number)
        } else {
            None
        }
    });
    let eof = next_line.is_none();
    let scan_limit_reached =
        !eof && !match_limit_reached && !content_limit_reached && line_number == scan_end;

    Ok(SearchLinesResponse {
        mode: if regex_mode {
            "regex".to_owned()
        } else {
            "literal".to_owned()
        },
        start_line: request.start_line,
        scanned_lines,
        matches,
        lossy_utf8,
        next_line,
        eof,
        scan_limit_reached,
        match_limit_reached,
        content_limit_reached,
    })
}

fn export_lines_blocking(
    opened: &OpenedFile,
    request: ExportLinesRequest,
    export_root: &Path,
) -> Result<ExportLinesResponse> {
    index::ensure_source_unchanged(&opened.source)?;
    let selected = selected_line_count(
        opened.index.line_count,
        request.start_line,
        request.line_count,
    )?;
    let end_exclusive = request.start_line + selected;
    let target = resolve_export_target(
        export_root,
        Path::new(&request.output_path),
        &opened.source.canonical_path,
    )?;
    if target.exists() && !request.overwrite {
        return Err(EngineError::InvalidRequest(format!(
            "output file already exists: {}",
            target.display()
        )));
    }

    let mut source_file = index::open_random(&opened.source.canonical_path)?;
    let start_offset = offset_for_line_or_eof(&mut source_file, &opened.index, request.start_line)?;
    let end_offset = if selected == 0 {
        start_offset
    } else {
        index::locate_line_offset(&mut source_file, &opened.index, end_exclusive)?
    };
    let bytes_to_copy = end_offset - start_offset;
    source_file.seek(SeekFrom::Start(start_offset))?;

    let parent = target.parent().ok_or_else(|| {
        EngineError::InvalidRequest("output path has no parent directory".to_owned())
    })?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    let mut buffer = vec![0_u8; EXPORT_BUFFER_BYTES];
    let mut remaining = bytes_to_copy;
    while remaining > 0 {
        let wanted = remaining.min(buffer.len() as u64) as usize;
        let bytes_read = source_file.read(&mut buffer[..wanted])?;
        if bytes_read == 0 {
            return Err(IndexError::SourceChanged.into());
        }
        temporary.write_all(&buffer[..bytes_read])?;
        remaining -= bytes_read as u64;
    }
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    index::ensure_source_unchanged(&opened.source)?;

    if request.overwrite {
        temporary
            .persist(&target)
            .map_err(|error| EngineError::Io(error.error))?;
    } else {
        temporary
            .persist_noclobber(&target)
            .map_err(|error| EngineError::Io(error.error))?;
    }

    let next_line = if end_exclusive <= opened.index.line_count {
        Some(end_exclusive)
    } else {
        None
    };
    Ok(ExportLinesResponse {
        start_line: request.start_line,
        end_line: selected
            .checked_sub(1)
            .map(|delta| request.start_line + delta),
        exported_lines: selected,
        bytes_written: bytes_to_copy,
        output_path: path_string(&target),
        eof: next_line.is_none(),
    })
}

fn selected_line_count(total_lines: u64, start_line: u64, requested: u64) -> Result<u64> {
    let eof_line = total_lines.saturating_add(1);
    if start_line > eof_line {
        return Err(EngineError::InvalidRequest(format!(
            "start_line {start_line} is past EOF; the file has {total_lines} lines"
        )));
    }
    if start_line == eof_line || requested == 0 {
        return Ok(0);
    }
    Ok(requested.min(total_lines - start_line + 1))
}

fn offset_for_line_or_eof(file: &mut File, index: &LineIndex, line: u64) -> Result<u64> {
    if index.line_count == 0 && line == 1 {
        Ok(0)
    } else {
        Ok(index::locate_line_offset(file, index, line)?)
    }
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    output: &mut Vec<u8>,
    max_bytes: usize,
) -> io::Result<usize> {
    output.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(output.len());
        }
        let newline = memchr(b'\n', available);
        let take = newline.map_or(available.len(), |position| position + 1);
        if output.len().saturating_add(take) > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "line exceeds configured byte limit",
            ));
        }
        output.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(output.len());
        }
    }
}

fn without_line_ending(bytes: &[u8]) -> &[u8] {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    bytes.strip_suffix(b"\r").unwrap_or(bytes)
}

fn decode_lossy(bytes: Vec<u8>) -> (String, bool) {
    match String::from_utf8(bytes) {
        Ok(content) => (content, false),
        Err(error) => (String::from_utf8_lossy(error.as_bytes()).into_owned(), true),
    }
}

fn resolve_export_target(root: &Path, requested: &Path, source: &Path) -> Result<PathBuf> {
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let file_name = candidate
        .file_name()
        .ok_or_else(|| EngineError::InvalidRequest("output_path must name a file".to_owned()))?;
    let parent = candidate.parent().ok_or_else(|| {
        EngineError::InvalidRequest("output_path has no parent directory".to_owned())
    })?;
    let canonical_parent = std::fs::canonicalize(parent).map_err(|error| {
        EngineError::InvalidRequest(format!(
            "output parent directory must already exist ({}): {error}",
            parent.display()
        ))
    })?;
    if !canonical_parent.starts_with(root) {
        return Err(EngineError::InvalidRequest(format!(
            "output_path must stay under export root {}",
            root.display()
        )));
    }
    let target = canonical_parent.join(file_name);
    if target.exists() {
        let metadata = std::fs::metadata(&target)?;
        if !metadata.is_file() {
            return Err(EngineError::InvalidRequest(format!(
                "output_path is not a regular file: {}",
                target.display()
            )));
        }
        let canonical_target = std::fs::canonicalize(&target)?;
        if !canonical_target.starts_with(root) {
            return Err(EngineError::InvalidRequest(
                "output_path resolves outside export_root".to_owned(),
            ));
        }
        if canonical_target == source {
            return Err(EngineError::InvalidRequest(
                "refusing to overwrite the active source file".to_owned(),
            ));
        }
    }
    Ok(target)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn opens_reads_searches_and_exports() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("sample.log");
        std::fs::write(
            &source_path,
            b"alpha\nbeta ERROR\ngamma 123\ndelta ERROR 42\nomega",
        )
        .unwrap();
        let index_dir = temp.path().join("indexes");
        std::fs::create_dir(&index_dir).unwrap();
        let engine = Arc::new(FileEngine::new(EngineConfig {
            index_dir: std::fs::canonicalize(index_dir).unwrap(),
            export_root: std::fs::canonicalize(temp.path()).unwrap(),
            checkpoint_bytes: 16,
            max_read_lines: 100,
            max_search_lines: 100,
            max_matches: 100,
            max_content_bytes: 1024 * 1024,
            max_line_bytes: 1024 * 1024,
            max_pattern_bytes: 1024,
            max_export_lines: 100,
            query_concurrency: 2,
        }));

        engine
            .open_file(OpenFileRequest {
                path: source_path.to_string_lossy().into_owned(),
                force_rebuild: false,
            })
            .await
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = engine.status().await;
            if status.state == "ready" {
                break;
            }
            assert_ne!(status.state, "failed", "{:?}", status.error);
            assert!(Instant::now() < deadline, "indexing timed out");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let read = engine
            .read_lines(ReadLinesRequest {
                start_line: 2,
                line_count: 2,
            })
            .await
            .unwrap();
        assert_eq!(read.content, "beta ERROR\ngamma 123\n");
        assert_eq!(read.end_line, Some(3));

        let regex = engine
            .search_lines(SearchLinesRequest {
                pattern: r"ERROR \d+".to_owned(),
                start_line: 2,
                max_scan_lines: 3,
                max_matches: Some(10),
                regex: Some(true),
                case_sensitive: Some(true),
            })
            .await
            .unwrap();
        assert_eq!(regex.matches.len(), 1);
        assert_eq!(regex.matches[0].line, 4);

        let literal = engine
            .search_lines(SearchLinesRequest {
                pattern: "error".to_owned(),
                start_line: 1,
                max_scan_lines: 5,
                max_matches: Some(10),
                regex: Some(false),
                case_sensitive: Some(false),
            })
            .await
            .unwrap();
        assert_eq!(literal.matches.len(), 2);

        let exported = engine
            .export_lines(ExportLinesRequest {
                start_line: 3,
                line_count: 2,
                output_path: "exported.log".to_owned(),
                overwrite: false,
            })
            .await
            .unwrap();
        assert_eq!(exported.exported_lines, 2);
        assert_eq!(
            std::fs::read(temp.path().join("exported.log")).unwrap(),
            b"gamma 123\ndelta ERROR 42\n"
        );

        engine
            .export_lines(ExportLinesRequest {
                start_line: 1,
                line_count: 1,
                output_path: "exported.log".to_owned(),
                overwrite: true,
            })
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(temp.path().join("exported.log")).unwrap(),
            b"alpha\n"
        );

        engine.shutdown().await;
    }
}
