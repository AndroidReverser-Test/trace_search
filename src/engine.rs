use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, sync_channel},
    },
    time::Instant,
};

use memchr::{memchr_iter, memmem};
use regex::bytes::{Regex, RegexBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::index::{self, BuildProgress, IndexConfig, IndexError, LineIndex, SourceDescriptor};

const SEARCH_READER_BYTES: usize = 4 * 1024 * 1024;
const MIN_PARALLEL_SEARCH_BYTES: u64 = 8 * 1024 * 1024;
const EXPORT_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const REGEX_COMPILED_SIZE_LIMIT: usize = 16 * 1024 * 1024;
pub const MAX_SEARCH_THREADS: usize = 32;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub index_dir: PathBuf,
    pub export_root: PathBuf,
    pub checkpoint_bytes: u64,
    pub max_read_lines: u64,
    pub search_threads: usize,
    pub search_memory_budget_bytes: usize,
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
    #[error("search was cancelled")]
    Cancelled,
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
    /// Maximum number of lines to inspect, supplied entirely by the caller.
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
    query_slots: Arc<Semaphore>,
}

struct CancelSearchOnDrop(Arc<AtomicBool>);

impl Drop for CancelSearchOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl FileEngine {
    pub fn new(config: EngineConfig) -> Self {
        let query_concurrency = config.query_concurrency;
        Self {
            config,
            state: RwLock::new(ActiveState::Empty),
            transition: Mutex::new(()),
            next_job_id: AtomicU64::new(1),
            query_slots: Arc::new(Semaphore::new(query_concurrency)),
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

    pub async fn read_lines(
        self: &Arc<Self>,
        request: ReadLinesRequest,
    ) -> Result<ReadLinesResponse> {
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

        let permit = self
            .query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EngineError::NotReady("server is shutting down".to_owned()))?;
        let opened = self.ready_file().await?;
        let max_content_bytes = self.config.max_content_bytes;
        let engine = Arc::clone(self);
        run_blocking(move || {
            let _permit = permit;
            let result = read_lines_blocking(&opened, request, max_content_bytes);
            engine.finish_query_blocking(&opened, result)
        })
        .await?
    }

    pub async fn search_lines(
        self: &Arc<Self>,
        request: SearchLinesRequest,
    ) -> Result<SearchLinesResponse> {
        if request.start_line == 0 {
            return Err(EngineError::InvalidRequest(
                "start_line is one-based and must be at least 1".to_owned(),
            ));
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

        let permit = self
            .query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EngineError::NotReady("server is shutting down".to_owned()))?;
        let opened = self.ready_file().await?;
        let cancellation = Arc::new(AtomicBool::new(false));
        let _cancel_on_drop = CancelSearchOnDrop(cancellation.clone());
        let max_line_bytes = self.config.max_line_bytes;
        let max_content_bytes = self.config.max_content_bytes;
        let search_threads = self.effective_search_threads();
        let engine = Arc::clone(self);
        run_blocking(move || {
            let _permit = permit;
            let result = search_lines_blocking(
                &opened,
                request,
                requested_matches,
                max_line_bytes,
                max_content_bytes,
                search_threads,
                &cancellation,
            );
            engine.finish_query_blocking(&opened, result)
        })
        .await?
    }

    pub async fn export_lines(
        self: &Arc<Self>,
        request: ExportLinesRequest,
    ) -> Result<ExportLinesResponse> {
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

        let permit = self
            .query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EngineError::NotReady("server is shutting down".to_owned()))?;
        let opened = self.ready_file().await?;
        let export_root = self.config.export_root.clone();
        let engine = Arc::clone(self);
        run_blocking(move || {
            let _permit = permit;
            let result = export_lines_blocking(&opened, request, &export_root);
            engine.finish_query_blocking(&opened, result)
        })
        .await?
    }

    fn index_config(&self) -> IndexConfig {
        IndexConfig {
            directory: self.config.index_dir.clone(),
            checkpoint_bytes: self.config.checkpoint_bytes,
        }
    }

    fn effective_search_threads(&self) -> usize {
        let configured = self.config.search_threads.clamp(1, MAX_SEARCH_THREADS);
        let per_query_budget =
            self.config.search_memory_budget_bytes / self.config.query_concurrency.max(1);
        let worst_case_worker_bytes = SEARCH_READER_BYTES
            .saturating_add(self.config.max_line_bytes)
            .saturating_add(self.config.max_content_bytes);
        let memory_limited = (per_query_budget / worst_case_worker_bytes.max(1)).max(1);
        configured.min(memory_limited)
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

    fn finish_query_blocking<T>(&self, opened: &Arc<OpenedFile>, result: Result<T>) -> Result<T> {
        if let Err(EngineError::Index(error)) = &result {
            let mut state = self.state.blocking_write();
            if matches!(&*state, ActiveState::Ready(current) if Arc::ptr_eq(current, opened)) {
                *state = ActiveState::Failed(FailedState {
                    source: Some(opened.source.clone()),
                    index_path: Some(opened.index.index_path.clone()),
                    error: error.to_string(),
                });
            }
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

#[derive(Clone)]
enum SearchMatcher<'pattern> {
    Literal(Arc<memmem::Finder<'pattern>>),
    Regex(Regex),
}

impl SearchMatcher<'_> {
    fn is_match(&self, bytes: &[u8]) -> bool {
        match self {
            Self::Literal(finder) => finder.find(bytes).is_some(),
            Self::Regex(regex) => regex.is_match(bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchChunk {
    start_line: u64,
    end_line: u64,
    start_offset: u64,
    end_offset: u64,
}

#[derive(Debug, Clone, Copy)]
struct SearchScanOptions {
    source_size: u64,
    max_line_bytes: usize,
    max_content_bytes: usize,
}

enum SearchEvent {
    Match {
        line: u64,
        content: String,
        lossy_utf8: bool,
    },
    OversizedMatch {
        line: u64,
    },
}

enum SearchWorkerEvent {
    Search(SearchEvent),
    Complete,
    Error(EngineError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchScanStatus {
    Complete,
    Stopped,
}

struct SearchAccumulator {
    start_line: u64,
    scan_end: u64,
    total_lines: u64,
    max_matches: u64,
    max_content_bytes: usize,
    scanned_lines: u64,
    matches: Vec<SearchMatch>,
    content_bytes: usize,
    lossy_utf8: bool,
    next_line: Option<u64>,
    stopped_early: bool,
    match_limit_reached: bool,
    content_limit_reached: bool,
}

impl SearchAccumulator {
    fn new(
        start_line: u64,
        scan_end: u64,
        total_lines: u64,
        max_matches: u64,
        max_content_bytes: usize,
    ) -> Self {
        Self {
            start_line,
            scan_end,
            total_lines,
            max_matches,
            max_content_bytes,
            scanned_lines: 0,
            matches: Vec::new(),
            content_bytes: 0,
            lossy_utf8: false,
            next_line: None,
            stopped_early: false,
            match_limit_reached: false,
            content_limit_reached: false,
        }
    }

    fn accept(&mut self, event: SearchEvent) -> Result<bool> {
        match event {
            SearchEvent::Match {
                line,
                content,
                lossy_utf8,
            } => {
                let next_content_bytes = self.content_bytes.saturating_add(content.len());
                if next_content_bytes > self.max_content_bytes {
                    if self.matches.is_empty() {
                        return Err(EngineError::InvalidRequest(format!(
                            "matching line {line} exceeds max-content-bytes {}",
                            self.max_content_bytes
                        )));
                    }
                    self.stop_for_content_limit(line);
                    return Ok(false);
                }

                self.content_bytes = next_content_bytes;
                self.lossy_utf8 |= lossy_utf8;
                self.matches.push(SearchMatch { line, content });
                if self.matches.len() as u64 >= self.max_matches {
                    self.scanned_lines = line - self.start_line + 1;
                    self.next_line = (line < self.total_lines).then_some(line + 1);
                    self.stopped_early = true;
                    self.match_limit_reached = self.next_line.is_some();
                    return Ok(false);
                }
            }
            SearchEvent::OversizedMatch { line } => {
                if self.matches.is_empty() {
                    return Err(EngineError::InvalidRequest(format!(
                        "matching line {line} exceeds max-content-bytes {}",
                        self.max_content_bytes
                    )));
                }
                self.stop_for_content_limit(line);
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn stop_for_content_limit(&mut self, line: u64) {
        self.scanned_lines = line - self.start_line;
        self.next_line = Some(line);
        self.stopped_early = true;
        self.content_limit_reached = true;
    }

    fn complete_chunk(&mut self, end_line: u64) {
        self.scanned_lines = end_line - self.start_line;
    }

    fn into_response(self, regex_mode: bool) -> SearchLinesResponse {
        let next_line = if self.stopped_early {
            self.next_line
        } else if self.scan_end <= self.total_lines {
            Some(self.scan_end)
        } else {
            None
        };
        let eof = next_line.is_none();

        SearchLinesResponse {
            mode: if regex_mode {
                "regex".to_owned()
            } else {
                "literal".to_owned()
            },
            start_line: self.start_line,
            scanned_lines: self.scanned_lines,
            matches: self.matches,
            lossy_utf8: self.lossy_utf8,
            next_line,
            eof,
            scan_limit_reached: !self.stopped_early && !eof,
            match_limit_reached: self.match_limit_reached,
            content_limit_reached: self.content_limit_reached,
        }
    }
}

fn search_lines_blocking(
    opened: &OpenedFile,
    request: SearchLinesRequest,
    max_matches: u64,
    max_line_bytes: usize,
    max_content_bytes: usize,
    search_threads: usize,
    cancellation: &AtomicBool,
) -> Result<SearchLinesResponse> {
    index::ensure_source_unchanged(&opened.source)?;
    let result = search_lines_snapshot(
        opened,
        request,
        max_matches,
        max_line_bytes,
        max_content_bytes,
        search_threads,
        cancellation,
    );
    match result {
        Ok(response) => {
            index::ensure_source_unchanged(&opened.source)?;
            Ok(response)
        }
        Err(error) => match index::ensure_source_unchanged(&opened.source) {
            Ok(()) => Err(error),
            Err(source_error) => Err(source_error.into()),
        },
    }
}

fn search_lines_snapshot(
    opened: &OpenedFile,
    request: SearchLinesRequest,
    max_matches: u64,
    max_line_bytes: usize,
    max_content_bytes: usize,
    search_threads: usize,
    cancellation: &AtomicBool,
) -> Result<SearchLinesResponse> {
    if cancellation.load(Ordering::Relaxed) {
        return Err(EngineError::Cancelled);
    }
    let selected = selected_line_count(
        opened.index.line_count,
        request.start_line,
        request.max_scan_lines,
    )?;
    let scan_end = request
        .start_line
        .checked_add(selected)
        .ok_or_else(|| EngineError::InvalidRequest("search line range overflows u64".to_owned()))?;
    let regex_mode = request.regex.unwrap_or(false);
    let case_sensitive = request.case_sensitive.unwrap_or(true);
    let matcher = if !regex_mode && case_sensitive {
        SearchMatcher::Literal(Arc::new(memmem::Finder::new(request.pattern.as_bytes())))
    } else {
        let expression = if regex_mode {
            request.pattern.clone()
        } else {
            regex::escape(&request.pattern)
        };
        let mut builder = RegexBuilder::new(&expression);
        builder
            .case_insensitive(!case_sensitive)
            .size_limit(REGEX_COMPILED_SIZE_LIMIT);
        SearchMatcher::Regex(
            builder
                .build()
                .map_err(|error| EngineError::InvalidRequest(format!("invalid regex: {error}")))?,
        )
    };
    let mut accumulator = SearchAccumulator::new(
        request.start_line,
        scan_end,
        opened.index.line_count,
        max_matches,
        max_content_bytes,
    );

    if selected != 0 {
        let mut file = index::open_random(&opened.source.canonical_path)?;
        let start_offset = offset_for_search_line_or_eof(
            &mut file,
            &opened.index,
            request.start_line,
            cancellation,
        )?;
        let end_offset =
            locate_search_line_offset(&mut file, &opened.index, scan_end, cancellation)?;
        if cancellation.load(Ordering::Relaxed) {
            return Err(EngineError::Cancelled);
        }
        let chunks = plan_search_chunks(
            &opened.index,
            request.start_line,
            scan_end,
            start_offset,
            end_offset,
            search_threads.max(1),
        );
        let options = SearchScanOptions {
            source_size: opened.source.identity.size,
            max_line_bytes,
            max_content_bytes,
        };

        if chunks.len() == 1 {
            let chunk = chunks[0];
            let status = scan_search_chunk(
                &opened.source.canonical_path,
                chunk,
                &matcher,
                options,
                cancellation,
                |event| accumulator.accept(event),
            )?;
            if status == SearchScanStatus::Complete {
                accumulator.complete_chunk(chunk.end_line);
            } else if !accumulator.stopped_early {
                return Err(EngineError::Cancelled);
            }
        } else {
            accumulator = search_chunks_parallel(
                &opened.source.canonical_path,
                &chunks,
                &matcher,
                options,
                accumulator,
                cancellation,
            )?;
        }
    }

    Ok(accumulator.into_response(regex_mode))
}

fn plan_search_chunks(
    index: &LineIndex,
    start_line: u64,
    end_line: u64,
    start_offset: u64,
    end_offset: u64,
    max_threads: usize,
) -> Vec<SearchChunk> {
    let total_bytes = end_offset - start_offset;
    let byte_limited_chunks = total_bytes.div_ceil(MIN_PARALLEL_SEARCH_BYTES).max(1);
    let desired_chunks = max_threads
        .max(1)
        .min(usize::try_from(byte_limited_chunks).unwrap_or(usize::MAX));
    if desired_chunks == 1 {
        return vec![SearchChunk {
            start_line,
            end_line,
            start_offset,
            end_offset,
        }];
    }

    let mut boundaries = Vec::with_capacity(desired_chunks + 1);
    boundaries.push((start_line, start_offset));
    let mut entry_index = index
        .entries
        .partition_point(|entry| entry.offset <= start_offset);

    for part in 1..desired_chunks {
        let target_offset =
            start_offset + ((total_bytes as u128 * part as u128) / desired_chunks as u128) as u64;
        while entry_index < index.entries.len() && index.entries[entry_index].offset < target_offset
        {
            entry_index += 1;
        }
        while entry_index < index.entries.len() {
            let entry = index.entries[entry_index];
            if entry.line >= end_line || entry.offset >= end_offset {
                break;
            }
            let (last_line, last_offset) = *boundaries.last().expect("initial boundary");
            if entry.line > last_line && entry.offset > last_offset {
                boundaries.push((entry.line, entry.offset));
                entry_index += 1;
                break;
            }
            entry_index += 1;
        }
    }
    boundaries.push((end_line, end_offset));

    boundaries
        .windows(2)
        .map(|boundary| SearchChunk {
            start_line: boundary[0].0,
            end_line: boundary[1].0,
            start_offset: boundary[0].1,
            end_offset: boundary[1].1,
        })
        .collect()
}

fn search_chunks_parallel(
    source_path: &Path,
    chunks: &[SearchChunk],
    matcher: &SearchMatcher<'_>,
    options: SearchScanOptions,
    mut accumulator: SearchAccumulator,
    cancellation: &AtomicBool,
) -> Result<SearchAccumulator> {
    std::thread::scope(|scope| {
        let mut receivers = Vec::with_capacity(chunks.len());
        for &chunk in chunks {
            let (sender, receiver) = sync_channel(0);
            receivers.push(receiver);
            let matcher = matcher.clone();
            scope.spawn(move || {
                let result = scan_search_chunk(
                    source_path,
                    chunk,
                    &matcher,
                    options,
                    cancellation,
                    |event| {
                        if cancellation.load(Ordering::Relaxed) {
                            return Ok(false);
                        }
                        Ok(sender.send(SearchWorkerEvent::Search(event)).is_ok())
                    },
                );
                if cancellation.load(Ordering::Relaxed) {
                    return;
                }
                match result {
                    Ok(SearchScanStatus::Complete) => {
                        let _ = sender.send(SearchWorkerEvent::Complete);
                    }
                    Ok(SearchScanStatus::Stopped) => {}
                    Err(error) => {
                        let _ = sender.send(SearchWorkerEvent::Error(error));
                    }
                }
            });
        }

        let result = consume_search_workers(chunks, &receivers, &mut accumulator, cancellation);
        cancellation.store(true, Ordering::Relaxed);
        drop(receivers);
        result.map(|()| accumulator)
    })
}

fn consume_search_workers(
    chunks: &[SearchChunk],
    receivers: &[Receiver<SearchWorkerEvent>],
    accumulator: &mut SearchAccumulator,
    cancellation: &AtomicBool,
) -> Result<()> {
    for (&chunk, receiver) in chunks.iter().zip(receivers) {
        loop {
            match receiver.recv() {
                Ok(SearchWorkerEvent::Search(event)) => {
                    if !accumulator.accept(event)? {
                        return Ok(());
                    }
                }
                Ok(SearchWorkerEvent::Complete) => {
                    accumulator.complete_chunk(chunk.end_line);
                    break;
                }
                Ok(SearchWorkerEvent::Error(error)) => return Err(error),
                Err(_) => {
                    if cancellation.load(Ordering::Relaxed) {
                        return Err(EngineError::Cancelled);
                    }
                    return Err(EngineError::Join(
                        "parallel search worker stopped unexpectedly".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn scan_search_chunk<F>(
    source_path: &Path,
    chunk: SearchChunk,
    matcher: &SearchMatcher<'_>,
    options: SearchScanOptions,
    cancellation: &AtomicBool,
    mut emit: F,
) -> Result<SearchScanStatus>
where
    F: FnMut(SearchEvent) -> Result<bool>,
{
    let mut file = index::open_sequential(source_path)?;
    file.seek(SeekFrom::Start(chunk.start_offset))?;
    let limited = file.take(chunk.end_offset - chunk.start_offset);
    let mut reader = BufReader::with_capacity(SEARCH_READER_BYTES, limited);
    let mut partial_line = Vec::new();
    let mut line_number = chunk.start_line;

    while line_number < chunk.end_line {
        if cancellation.load(Ordering::Relaxed) {
            return Ok(SearchScanStatus::Stopped);
        }

        let available = reader.fill_buf()?;
        if available.is_empty() {
            if partial_line.is_empty() {
                return Err(IndexError::SourceChanged.into());
            }
            if chunk.end_offset != options.source_size {
                return Err(IndexError::Corrupt(
                    "parallel search chunk ended inside a line".to_owned(),
                )
                .into());
            }
            if !emit_search_line(
                &partial_line,
                line_number,
                matcher,
                options.max_content_bytes,
                &mut emit,
            )? {
                return Ok(SearchScanStatus::Stopped);
            }
            line_number += 1;
            partial_line.clear();
            break;
        }

        let available_len = available.len();
        let mut segment_start = 0_usize;
        let mut completed = false;
        for newline in memchr_iter(b'\n', available) {
            let segment_end = newline + 1;
            let keep_scanning = if partial_line.is_empty() {
                let line = &available[segment_start..segment_end];
                ensure_search_line_size(line.len(), line_number, options.max_line_bytes)?;
                emit_search_line(
                    line,
                    line_number,
                    matcher,
                    options.max_content_bytes,
                    &mut emit,
                )?
            } else {
                append_search_line_bytes(
                    &mut partial_line,
                    &available[segment_start..segment_end],
                    line_number,
                    options.max_line_bytes,
                )?;
                let keep_scanning = emit_search_line(
                    &partial_line,
                    line_number,
                    matcher,
                    options.max_content_bytes,
                    &mut emit,
                )?;
                partial_line.clear();
                keep_scanning
            };
            if !keep_scanning {
                return Ok(SearchScanStatus::Stopped);
            }

            line_number += 1;
            segment_start = segment_end;
            if line_number == chunk.end_line {
                completed = true;
                break;
            }
        }

        if completed {
            let has_unexpected_bytes =
                segment_start != available_len || reader.get_ref().limit() != 0;
            reader.consume(available_len);
            if has_unexpected_bytes {
                return Err(IndexError::Corrupt(
                    "parallel search chunk line range does not match its byte range".to_owned(),
                )
                .into());
            }
            return Ok(SearchScanStatus::Complete);
        }

        if segment_start < available_len {
            append_search_line_bytes(
                &mut partial_line,
                &available[segment_start..],
                line_number,
                options.max_line_bytes,
            )?;
        }
        reader.consume(available_len);
    }

    if line_number == chunk.end_line {
        Ok(SearchScanStatus::Complete)
    } else {
        Err(IndexError::SourceChanged.into())
    }
}

fn ensure_search_line_size(length: usize, line: u64, max_line_bytes: usize) -> Result<()> {
    if length > max_line_bytes {
        return Err(EngineError::InvalidRequest(format!(
            "line {line} exceeds max-line-bytes {max_line_bytes}"
        )));
    }
    Ok(())
}

fn append_search_line_bytes(
    line_bytes: &mut Vec<u8>,
    bytes: &[u8],
    line: u64,
    max_line_bytes: usize,
) -> Result<()> {
    if bytes.len() > max_line_bytes.saturating_sub(line_bytes.len()) {
        return Err(EngineError::InvalidRequest(format!(
            "line {line} exceeds max-line-bytes {max_line_bytes}"
        )));
    }
    line_bytes.extend_from_slice(bytes);
    Ok(())
}

fn emit_search_line<F>(
    line_bytes: &[u8],
    line: u64,
    matcher: &SearchMatcher<'_>,
    max_content_bytes: usize,
    emit: &mut F,
) -> Result<bool>
where
    F: FnMut(SearchEvent) -> Result<bool>,
{
    let content_bytes = without_line_ending(line_bytes);
    if !matcher.is_match(content_bytes) {
        return Ok(true);
    }

    let (content, lossy_utf8) = match std::str::from_utf8(content_bytes) {
        Ok(content) => {
            if content.len() > max_content_bytes {
                emit(SearchEvent::OversizedMatch { line })?;
                return Ok(false);
            }
            (content.to_owned(), false)
        }
        Err(_) => {
            let content = String::from_utf8_lossy(content_bytes);
            if content.len() > max_content_bytes {
                emit(SearchEvent::OversizedMatch { line })?;
                return Ok(false);
            }
            (content.into_owned(), true)
        }
    };
    emit(SearchEvent::Match {
        line,
        content,
        lossy_utf8,
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

fn offset_for_search_line_or_eof(
    file: &mut File,
    index: &LineIndex,
    line: u64,
    cancellation: &AtomicBool,
) -> Result<u64> {
    if index.line_count == 0 && line == 1 {
        Ok(0)
    } else {
        locate_search_line_offset(file, index, line, cancellation)
    }
}

fn locate_search_line_offset(
    file: &mut File,
    index: &LineIndex,
    line: u64,
    cancellation: &AtomicBool,
) -> Result<u64> {
    match index::locate_line_offset_cancellable(file, index, line, cancellation) {
        Ok(offset) => Ok(offset),
        Err(IndexError::Cancelled) => Err(EngineError::Cancelled),
        Err(error) => Err(error.into()),
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

    #[test]
    fn plans_byte_balanced_chunks_on_indexed_line_boundaries() {
        let mib = 1024 * 1024_u64;
        let index = LineIndex {
            index_path: PathBuf::new(),
            source_size: 80 * mib,
            line_count: 1_000,
            checkpoint_bytes: 8 * mib,
            entries: (0..10)
                .map(|checkpoint| index::IndexEntry {
                    line: checkpoint * 100 + 1,
                    offset: checkpoint * 8 * mib,
                })
                .collect(),
        };

        let chunks = plan_search_chunks(&index, 1, 1_001, 0, 80 * mib, 4);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks.first().unwrap().start_line, 1);
        assert_eq!(chunks.first().unwrap().start_offset, 0);
        assert_eq!(chunks.last().unwrap().end_line, 1_001);
        assert_eq!(chunks.last().unwrap().end_offset, 80 * mib);
        for chunks in chunks.windows(2) {
            assert_eq!(chunks[0].end_line, chunks[1].start_line);
            assert_eq!(chunks[0].end_offset, chunks[1].start_offset);
        }
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.end_offset > chunk.start_offset)
        );
    }

    #[test]
    fn search_thread_count_respects_memory_budget() {
        let engine = FileEngine::new(EngineConfig {
            index_dir: PathBuf::new(),
            export_root: PathBuf::new(),
            checkpoint_bytes: 8 * 1024 * 1024,
            max_read_lines: 1,
            search_threads: 8,
            search_memory_budget_bytes: SEARCH_READER_BYTES * 4,
            max_matches: 1,
            max_content_bytes: SEARCH_READER_BYTES,
            max_line_bytes: SEARCH_READER_BYTES,
            max_pattern_bytes: 1,
            max_export_lines: 1,
            query_concurrency: 2,
        });
        assert_eq!(engine.effective_search_threads(), 1);
    }

    #[test]
    fn parallel_search_merges_in_order_and_preserves_limits() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("parallel.log");
        std::fs::write(
            &source_path,
            b"hit one\r\nskip\nhit two\nskip\nhit three\nhit \xff",
        )
        .unwrap();
        let source = index::inspect_source(&source_path).unwrap();
        let index_config = IndexConfig {
            directory: temp.path().join("indexes"),
            checkpoint_bytes: 8,
        };
        let progress = BuildProgress::new(CancellationToken::new());
        let line_index = index::build_or_load(&source, &index_config, &progress, false).unwrap();
        let mut file = index::open_random(&source.canonical_path).unwrap();
        let boundaries = [1, 3, 5, 7].map(|line| {
            (
                line,
                index::locate_line_offset(&mut file, &line_index, line).unwrap(),
            )
        });
        let chunks = boundaries
            .windows(2)
            .map(|boundary| SearchChunk {
                start_line: boundary[0].0,
                end_line: boundary[1].0,
                start_offset: boundary[0].1,
                end_offset: boundary[1].1,
            })
            .collect::<Vec<_>>();
        let pattern = "hit".to_owned();
        let matcher = SearchMatcher::Literal(Arc::new(memmem::Finder::new(pattern.as_bytes())));

        let match_limited_cancellation = AtomicBool::new(false);
        let match_limited = search_chunks_parallel(
            &source.canonical_path,
            &chunks,
            &matcher,
            SearchScanOptions {
                source_size: source.identity.size,
                max_line_bytes: 1024,
                max_content_bytes: 1024,
            },
            SearchAccumulator::new(1, 7, 6, 3, 1024),
            &match_limited_cancellation,
        )
        .unwrap()
        .into_response(false);
        assert_eq!(
            match_limited
                .matches
                .iter()
                .map(|found| found.line)
                .collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        assert_eq!(match_limited.matches[0].content, "hit one");
        assert_eq!(match_limited.scanned_lines, 5);
        assert_eq!(match_limited.next_line, Some(6));
        assert!(match_limited.match_limit_reached);

        let content_limited_cancellation = AtomicBool::new(false);
        let content_limited = search_chunks_parallel(
            &source.canonical_path,
            &chunks,
            &matcher,
            SearchScanOptions {
                source_size: source.identity.size,
                max_line_bytes: 1024,
                max_content_bytes: 10,
            },
            SearchAccumulator::new(1, 7, 6, 10, 10),
            &content_limited_cancellation,
        )
        .unwrap()
        .into_response(false);
        assert_eq!(content_limited.matches.len(), 1);
        assert_eq!(content_limited.scanned_lines, 2);
        assert_eq!(content_limited.next_line, Some(3));
        assert!(content_limited.content_limit_reached);

        let complete_cancellation = AtomicBool::new(false);
        let complete = search_chunks_parallel(
            &source.canonical_path,
            &chunks,
            &matcher,
            SearchScanOptions {
                source_size: source.identity.size,
                max_line_bytes: 1024,
                max_content_bytes: 1024,
            },
            SearchAccumulator::new(1, 7, 6, 10, 1024),
            &complete_cancellation,
        )
        .unwrap()
        .into_response(false);
        assert_eq!(
            complete
                .matches
                .iter()
                .map(|found| found.line)
                .collect::<Vec<_>>(),
            vec![1, 3, 5, 6]
        );
        assert_eq!(complete.scanned_lines, 6);
        assert!(complete.eof);
        assert!(complete.lossy_utf8);
    }

    #[test]
    fn search_handles_lines_crossing_reader_buffers() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("long-line.log");
        let mut content = vec![b'x'; SEARCH_READER_BYTES - 1];
        content.extend_from_slice(b"\r\nneedle");
        std::fs::write(&source_path, &content).unwrap();
        let source = index::inspect_source(&source_path).unwrap();
        let matcher = SearchMatcher::Literal(Arc::new(memmem::Finder::new(b"needle")));
        let chunk = SearchChunk {
            start_line: 1,
            end_line: 3,
            start_offset: 0,
            end_offset: source.identity.size,
        };
        let cancellation = AtomicBool::new(false);
        let mut accumulator = SearchAccumulator::new(1, 3, 2, 10, 1024);
        let options = SearchScanOptions {
            source_size: source.identity.size,
            max_line_bytes: SEARCH_READER_BYTES + 1,
            max_content_bytes: 1024,
        };
        let status = scan_search_chunk(
            &source.canonical_path,
            chunk,
            &matcher,
            options,
            &cancellation,
            |event| accumulator.accept(event),
        )
        .unwrap();
        assert_eq!(status, SearchScanStatus::Complete);
        accumulator.complete_chunk(chunk.end_line);
        let response = accumulator.into_response(false);
        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].line, 2);
        assert_eq!(response.matches[0].content, "needle");
        assert_eq!(response.scanned_lines, 2);
        assert!(response.eof);

        let error = scan_search_chunk(
            &source.canonical_path,
            chunk,
            &matcher,
            SearchScanOptions {
                max_line_bytes: SEARCH_READER_BYTES,
                ..options
            },
            &cancellation,
            |_| Ok(true),
        )
        .unwrap_err();
        assert!(error.to_string().contains("line 1 exceeds max-line-bytes"));
    }

    #[test]
    fn end_to_end_parallel_search_matches_single_thread_results() {
        const LINE_BYTES: usize = 256;
        const LINE_COUNT: u64 = 70_000;

        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("parallel-large.log");
        let mut content = Vec::with_capacity(LINE_BYTES * LINE_COUNT as usize);
        for line in 1..=LINE_COUNT {
            let mut line_bytes = [b'x'; LINE_BYTES];
            if line % 17_000 == 0 {
                line_bytes[..6].copy_from_slice(b"needle");
            }
            line_bytes[LINE_BYTES - 1] = b'\n';
            content.extend_from_slice(&line_bytes);
        }
        content.pop();
        std::fs::write(&source_path, content).unwrap();

        let source = index::inspect_source(&source_path).unwrap();
        let index_config = IndexConfig {
            directory: temp.path().join("indexes"),
            checkpoint_bytes: 1024 * 1024,
        };
        let progress = BuildProgress::new(CancellationToken::new());
        let line_index = index::build_or_load(&source, &index_config, &progress, false).unwrap();
        let opened = OpenedFile {
            source,
            index: Arc::new(line_index),
        };
        let request = SearchLinesRequest {
            pattern: "needle".to_owned(),
            start_line: 1,
            max_scan_lines: u64::MAX,
            max_matches: Some(100),
            regex: Some(false),
            case_sensitive: Some(true),
        };

        let single_cancellation = AtomicBool::new(false);
        let single = search_lines_blocking(
            &opened,
            request.clone(),
            100,
            LINE_BYTES,
            1024 * 1024,
            1,
            &single_cancellation,
        )
        .unwrap();
        let parallel_cancellation = AtomicBool::new(false);
        let parallel = search_lines_blocking(
            &opened,
            request,
            100,
            LINE_BYTES,
            1024 * 1024,
            4,
            &parallel_cancellation,
        )
        .unwrap();

        assert_eq!(parallel.scanned_lines, single.scanned_lines);
        assert_eq!(parallel.next_line, single.next_line);
        assert_eq!(parallel.eof, single.eof);
        assert_eq!(parallel.scan_limit_reached, single.scan_limit_reached);
        assert_eq!(parallel.lossy_utf8, single.lossy_utf8);
        assert_eq!(parallel.matches.len(), single.matches.len());
        for (parallel_match, single_match) in parallel.matches.iter().zip(&single.matches) {
            assert_eq!(parallel_match.line, single_match.line);
            assert_eq!(parallel_match.content, single_match.content);
        }
    }

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
            search_threads: 4,
            search_memory_budget_bytes: 1024 * 1024 * 1024,
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

        let unlimited_server_range = engine
            .search_lines(SearchLinesRequest {
                pattern: "alpha".to_owned(),
                start_line: 1,
                max_scan_lines: u64::MAX,
                max_matches: Some(10),
                regex: Some(false),
                case_sensitive: Some(true),
            })
            .await
            .unwrap();
        assert_eq!(unlimited_server_range.scanned_lines, 5);
        assert!(unlimited_server_range.eof);

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
