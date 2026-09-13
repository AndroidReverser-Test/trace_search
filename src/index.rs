use std::{
    fs::{File, Metadata, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::UNIX_EPOCH,
};

use bytecount::count;
use fs2::FileExt;
use memchr::{memchr, memchr_iter};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const INDEX_MAGIC: &[u8; 8] = b"TSIDX001";
const INDEX_VERSION: u32 = 1;
const HEADER_LEN: usize = 160;
const RECORD_LEN: usize = 20;
const STATE_BUILDING: u32 = 1;
const STATE_COMPLETE: u32 = 2;
const SAMPLE_BYTES: usize = 64 * 1024;
const INDEX_READ_BUFFER_BYTES: usize = 32 * 1024 * 1024;
const LOCATE_BUFFER_BYTES: usize = 1024 * 1024;
const RECORD_FLUSH_BYTES: usize = 64 * 1024;
const DURABLE_PROGRESS_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid source: {0}")]
    InvalidSource(String),
    #[error("corrupt index: {0}")]
    Corrupt(String),
    #[error("the source file changed while it was being used")]
    SourceChanged,
    #[error("index construction was cancelled")]
    Cancelled,
    #[error("another process is already building this index")]
    Busy,
}

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub directory: PathBuf,
    pub checkpoint_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileIdentity {
    pub size: u64,
    pub modified_ns: u64,
    pub path_hash: [u8; 32],
    pub sample_hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct SourceDescriptor {
    pub canonical_path: PathBuf,
    pub identity: FileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub line: u64,
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct LineIndex {
    pub index_path: PathBuf,
    pub source_size: u64,
    pub line_count: u64,
    pub checkpoint_bytes: u64,
    pub entries: Vec<IndexEntry>,
}

impl LineIndex {
    pub fn checkpoint_for_line(&self, target_line: u64) -> IndexEntry {
        let index = self
            .entries
            .partition_point(|entry| entry.line <= target_line)
            .saturating_sub(1);
        self.entries[index]
    }
}

#[derive(Debug)]
pub struct BuildProgress {
    bytes_indexed: AtomicU64,
    lines_seen: AtomicU64,
    checkpoints: AtomicU64,
    cancellation: CancellationToken,
}

impl BuildProgress {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self {
            bytes_indexed: AtomicU64::new(0),
            lines_seen: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
            cancellation,
        }
    }

    pub fn bytes_indexed(&self) -> u64 {
        self.bytes_indexed.load(Ordering::Relaxed)
    }

    pub fn lines_seen(&self) -> u64 {
        self.lines_seen.load(Ordering::Relaxed)
    }

    pub fn checkpoints(&self) -> u64 {
        self.checkpoints.load(Ordering::Relaxed)
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn update(&self, bytes: u64, lines: u64, checkpoints: usize) {
        self.bytes_indexed.store(bytes, Ordering::Relaxed);
        self.lines_seen.store(lines, Ordering::Relaxed);
        self.checkpoints
            .store(checkpoints as u64, Ordering::Relaxed);
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            Err(IndexError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexPaths {
    pub final_path: PathBuf,
    pub partial_path: PathBuf,
    pub lock_path: PathBuf,
}

#[derive(Debug, Clone)]
struct IndexHeader {
    state: u32,
    source_size: u64,
    source_modified_ns: u64,
    checkpoint_bytes: u64,
    line_count: u64,
    entry_count: u64,
    path_hash: [u8; 32],
    sample_hash: [u8; 32],
}

impl IndexHeader {
    fn building(source: &SourceDescriptor, config: &IndexConfig) -> Self {
        Self {
            state: STATE_BUILDING,
            source_size: source.identity.size,
            source_modified_ns: source.identity.modified_ns,
            checkpoint_bytes: config.checkpoint_bytes,
            line_count: 0,
            entry_count: 0,
            path_hash: source.identity.path_hash,
            sample_hash: source.identity.sample_hash,
        }
    }

    fn matches(&self, source: &SourceDescriptor, config: &IndexConfig) -> bool {
        self.source_size == source.identity.size
            && self.source_modified_ns == source.identity.modified_ns
            && self.checkpoint_bytes == config.checkpoint_bytes
            && self.path_hash == source.identity.path_hash
            && self.sample_hash == source.identity.sample_hash
    }

    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0..8].copy_from_slice(INDEX_MAGIC);
        put_u32(&mut bytes, 8, INDEX_VERSION);
        put_u32(&mut bytes, 12, HEADER_LEN as u32);
        put_u32(&mut bytes, 16, self.state);
        put_u64(&mut bytes, 24, self.source_size);
        put_u64(&mut bytes, 32, self.source_modified_ns);
        put_u64(&mut bytes, 40, self.checkpoint_bytes);
        put_u64(&mut bytes, 48, self.line_count);
        put_u64(&mut bytes, 56, self.entry_count);
        bytes[64..96].copy_from_slice(&self.path_hash);
        bytes[96..128].copy_from_slice(&self.sample_hash);
        let checksum = crc32fast::hash(&bytes[..HEADER_LEN - 4]);
        put_u32(&mut bytes, HEADER_LEN - 4, checksum);
        bytes
    }

    fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Self> {
        if &bytes[0..8] != INDEX_MAGIC {
            return Err(IndexError::Corrupt("bad magic".to_owned()));
        }
        if get_u32(bytes, 8) != INDEX_VERSION {
            return Err(IndexError::Corrupt("unsupported version".to_owned()));
        }
        if get_u32(bytes, 12) as usize != HEADER_LEN {
            return Err(IndexError::Corrupt("bad header length".to_owned()));
        }
        let expected = get_u32(bytes, HEADER_LEN - 4);
        let actual = crc32fast::hash(&bytes[..HEADER_LEN - 4]);
        if expected != actual {
            return Err(IndexError::Corrupt("header checksum mismatch".to_owned()));
        }

        let mut path_hash = [0_u8; 32];
        path_hash.copy_from_slice(&bytes[64..96]);
        let mut sample_hash = [0_u8; 32];
        sample_hash.copy_from_slice(&bytes[96..128]);

        Ok(Self {
            state: get_u32(bytes, 16),
            source_size: get_u64(bytes, 24),
            source_modified_ns: get_u64(bytes, 32),
            checkpoint_bytes: get_u64(bytes, 40),
            line_count: get_u64(bytes, 48),
            entry_count: get_u64(bytes, 56),
            path_hash,
            sample_hash,
        })
    }
}

pub fn inspect_source(path: &Path) -> Result<SourceDescriptor> {
    let canonical_path = std::fs::canonicalize(path)
        .map_err(|error| IndexError::InvalidSource(format!("{}: {error}", path.display())))?;
    let before = std::fs::metadata(&canonical_path)?;
    if !before.is_file() {
        return Err(IndexError::InvalidSource(format!(
            "{} is not a regular file",
            canonical_path.display()
        )));
    }

    let size = before.len();
    let source_modified_ns = modified_ns(&before);
    let sample_hash = sample_hash(&canonical_path, size)?;
    let after = std::fs::metadata(&canonical_path)?;
    if after.len() != size || modified_ns(&after) != source_modified_ns {
        return Err(IndexError::SourceChanged);
    }

    Ok(SourceDescriptor {
        canonical_path: canonical_path.clone(),
        identity: FileIdentity {
            size,
            modified_ns: source_modified_ns,
            path_hash: hash_path(&canonical_path),
            sample_hash,
        },
    })
}

pub fn ensure_source_unchanged(source: &SourceDescriptor) -> Result<()> {
    let metadata = std::fs::metadata(&source.canonical_path)?;
    if !metadata.is_file()
        || metadata.len() != source.identity.size
        || modified_ns(&metadata) != source.identity.modified_ns
    {
        return Err(IndexError::SourceChanged);
    }
    Ok(())
}

pub fn ensure_source_unchanged_full(source: &SourceDescriptor) -> Result<()> {
    let current = inspect_source(&source.canonical_path)?;
    if current.identity != source.identity {
        return Err(IndexError::SourceChanged);
    }
    Ok(())
}

pub fn index_paths(source: &SourceDescriptor, config: &IndexConfig) -> IndexPaths {
    let mut hasher = Sha256::new();
    hasher.update(source.identity.path_hash);
    hasher.update(source.identity.size.to_le_bytes());
    hasher.update(source.identity.modified_ns.to_le_bytes());
    hasher.update(source.identity.sample_hash);
    hasher.update(config.checkpoint_bytes.to_le_bytes());
    let key = hex_lower(&hasher.finalize());

    IndexPaths {
        final_path: config.directory.join(format!("{key}.tsidx")),
        partial_path: config.directory.join(format!("{key}.partial")),
        lock_path: config.directory.join(format!("{key}.lock")),
    }
}

pub fn load_existing(source: &SourceDescriptor, config: &IndexConfig) -> Result<Option<LineIndex>> {
    let paths = index_paths(source, config);
    if !paths.final_path.exists() {
        return Ok(None);
    }
    load_index_file(&paths.final_path, source, config).map(Some)
}

pub fn build_or_load(
    source: &SourceDescriptor,
    config: &IndexConfig,
    progress: &BuildProgress,
    force_rebuild: bool,
) -> Result<LineIndex> {
    std::fs::create_dir_all(&config.directory)?;
    progress.check_cancelled()?;

    let paths = index_paths(source, config);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&paths.lock_path)?;
    FileExt::try_lock_exclusive(&lock_file).map_err(|error| {
        if error.kind() == io::ErrorKind::WouldBlock {
            IndexError::Busy
        } else {
            IndexError::Io(error)
        }
    })?;

    if !force_rebuild {
        match load_existing(source, config) {
            Ok(Some(index)) => {
                progress.update(index.source_size, index.line_count, index.entries.len());
                return Ok(index);
            }
            Ok(None) => {}
            Err(IndexError::Corrupt(error)) => {
                tracing::warn!(%error, path = %paths.final_path.display(), "discarding corrupt index");
                remove_if_exists(&paths.final_path)?;
            }
            Err(error) => return Err(error),
        }
    } else {
        remove_if_exists(&paths.final_path)?;
        remove_if_exists(&paths.partial_path)?;
    }

    if paths.partial_path.exists() {
        match load_index_file(&paths.partial_path, source, config) {
            Ok(mut index) => {
                replace_file(&paths.partial_path, &paths.final_path)?;
                index.index_path = paths.final_path;
                progress.update(index.source_size, index.line_count, index.entries.len());
                return Ok(index);
            }
            Err(IndexError::Corrupt(_)) => {}
            Err(error) => return Err(error),
        }
    }

    let (mut partial_file, mut entries) = prepare_partial(&paths, source, config)?;
    let mut source_file = open_sequential(&source.canonical_path)?;
    let mut absolute = entries.last().map_or(0, |entry| entry.offset);
    let mut newlines_seen = entries
        .last()
        .map_or(0, |entry| entry.line.saturating_sub(1));
    let mut checkpoint_offset = entries.last().map_or(0, |entry| entry.offset);
    let mut last_byte = None;
    let mut pending_records = Vec::with_capacity(RECORD_FLUSH_BYTES);
    let mut last_durable_offset = absolute;

    source_file.seek(SeekFrom::Start(absolute))?;
    partial_file.seek(SeekFrom::End(0))?;
    progress.update(
        absolute,
        line_estimate(source.identity.size, absolute, newlines_seen, None),
        entries.len(),
    );

    let mut buffer = vec![0_u8; INDEX_READ_BUFFER_BYTES];
    while absolute < source.identity.size {
        progress.check_cancelled()?;
        let remaining = source.identity.size - absolute;
        let read_size = remaining.min(buffer.len() as u64) as usize;
        let bytes_read = source_file.read(&mut buffer[..read_size])?;
        if bytes_read == 0 {
            return Err(IndexError::SourceChanged);
        }

        let chunk = &buffer[..bytes_read];
        let chunk_start = absolute;
        let chunk_end = chunk_start + bytes_read as u64;
        let mut cursor = 0_usize;

        while cursor < bytes_read {
            progress.check_cancelled()?;
            let threshold = checkpoint_offset.saturating_add(config.checkpoint_bytes);
            let minimum_newline_offset = threshold.saturating_sub(1);
            if minimum_newline_offset >= chunk_end {
                newlines_seen += count(&chunk[cursor..], b'\n') as u64;
                break;
            }

            let search_start = cursor.max(
                minimum_newline_offset
                    .saturating_sub(chunk_start)
                    .min(bytes_read as u64) as usize,
            );
            newlines_seen += count(&chunk[cursor..search_start], b'\n') as u64;

            let Some(relative) = memchr(b'\n', &chunk[search_start..]) else {
                newlines_seen += count(&chunk[search_start..], b'\n') as u64;
                break;
            };
            let newline_index = search_start + relative;
            newlines_seen += 1;
            let next_line_offset = chunk_start + newline_index as u64 + 1;
            cursor = newline_index + 1;

            if next_line_offset < source.identity.size {
                let entry = IndexEntry {
                    line: newlines_seen + 1,
                    offset: next_line_offset,
                };
                entries.push(entry);
                pending_records.extend_from_slice(&encode_record(entry));
                checkpoint_offset = next_line_offset;
                if pending_records.len() >= RECORD_FLUSH_BYTES {
                    partial_file.write_all(&pending_records)?;
                    pending_records.clear();
                }
            }
        }

        absolute = chunk_end;
        last_byte = chunk.last().copied();
        progress.update(
            absolute,
            line_estimate(source.identity.size, absolute, newlines_seen, last_byte),
            entries.len(),
        );

        if absolute.saturating_sub(last_durable_offset) >= DURABLE_PROGRESS_BYTES {
            if !pending_records.is_empty() {
                partial_file.write_all(&pending_records)?;
                pending_records.clear();
            }
            partial_file.sync_data()?;
            last_durable_offset = absolute;
        }
    }

    if !pending_records.is_empty() {
        partial_file.write_all(&pending_records)?;
    }

    let line_count = if source.identity.size == 0 {
        0
    } else if last_byte == Some(b'\n') {
        newlines_seen
    } else {
        newlines_seen + 1
    };

    ensure_source_unchanged_full(source)?;
    let mut header = IndexHeader::building(source, config);
    header.state = STATE_COMPLETE;
    header.line_count = line_count;
    header.entry_count = entries.len() as u64;
    partial_file.seek(SeekFrom::Start(0))?;
    partial_file.write_all(&header.encode())?;
    partial_file.set_len(HEADER_LEN as u64 + entries.len() as u64 * RECORD_LEN as u64)?;
    partial_file.sync_all()?;
    drop(partial_file);

    replace_file(&paths.partial_path, &paths.final_path)?;
    progress.update(source.identity.size, line_count, entries.len());

    Ok(LineIndex {
        index_path: paths.final_path,
        source_size: source.identity.size,
        line_count,
        checkpoint_bytes: config.checkpoint_bytes,
        entries,
    })
}

pub fn open_random(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_RANDOM_ACCESS: u32 = 0x1000_0000;
        options.custom_flags(FILE_FLAG_RANDOM_ACCESS);
    }
    options.open(path)
}

pub fn locate_line_offset(file: &mut File, index: &LineIndex, target_line: u64) -> Result<u64> {
    if target_line == index.line_count.saturating_add(1) {
        return Ok(index.source_size);
    }
    if target_line == 0 || target_line > index.line_count || index.entries.is_empty() {
        return Err(IndexError::InvalidSource(format!(
            "line {target_line} is outside 1..={}",
            index.line_count
        )));
    }

    let checkpoint = index.checkpoint_for_line(target_line);
    if checkpoint.line == target_line {
        return Ok(checkpoint.offset);
    }

    file.seek(SeekFrom::Start(checkpoint.offset))?;
    let mut remaining_newlines = target_line - checkpoint.line;
    let mut absolute = checkpoint.offset;
    let mut buffer = vec![0_u8; LOCATE_BUFFER_BYTES];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            return Err(IndexError::Corrupt(format!(
                "could not locate line {target_line}"
            )));
        }
        let chunk = &buffer[..bytes_read];
        let newline_count = count(chunk, b'\n') as u64;
        if newline_count < remaining_newlines {
            remaining_newlines -= newline_count;
            absolute += bytes_read as u64;
            continue;
        }

        let position = memchr_iter(b'\n', chunk)
            .nth((remaining_newlines - 1) as usize)
            .ok_or_else(|| IndexError::Corrupt("newline count mismatch".to_owned()))?;
        return Ok(absolute + position as u64 + 1);
    }
}

fn prepare_partial(
    paths: &IndexPaths,
    source: &SourceDescriptor,
    config: &IndexConfig,
) -> Result<(File, Vec<IndexEntry>)> {
    if paths.partial_path.exists() {
        match resume_partial(&paths.partial_path, source, config) {
            Ok(value) => return Ok(value),
            Err(IndexError::Corrupt(error)) => {
                tracing::warn!(%error, path = %paths.partial_path.display(), "discarding corrupt partial index");
                remove_if_exists(&paths.partial_path)?;
            }
            Err(error) => return Err(error),
        }
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&paths.partial_path)?;
    file.write_all(&IndexHeader::building(source, config).encode())?;

    let mut entries = Vec::new();
    if source.identity.size > 0 {
        let first = IndexEntry { line: 1, offset: 0 };
        file.write_all(&encode_record(first))?;
        entries.push(first);
    }
    file.sync_data()?;
    Ok((file, entries))
}

fn resume_partial(
    path: &Path,
    source: &SourceDescriptor,
    config: &IndexConfig,
) -> Result<(File, Vec<IndexEntry>)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let header = read_header(&mut file)?;
    if header.state != STATE_BUILDING {
        return Err(IndexError::Corrupt(
            "partial index is not in building state".to_owned(),
        ));
    }
    if !header.matches(source, config) {
        return Err(IndexError::Corrupt(
            "partial index belongs to another source snapshot".to_owned(),
        ));
    }

    let file_len = file.metadata()?.len();
    if file_len < HEADER_LEN as u64 {
        return Err(IndexError::Corrupt("partial index is truncated".to_owned()));
    }
    let available_records = (file_len - HEADER_LEN as u64) / RECORD_LEN as u64;
    file.seek(SeekFrom::Start(HEADER_LEN as u64))?;
    let mut entries = Vec::with_capacity(available_records as usize);
    let mut record_bytes = [0_u8; RECORD_LEN];

    for _ in 0..available_records {
        file.read_exact(&mut record_bytes)?;
        let Ok(entry) = decode_record(&record_bytes) else {
            break;
        };
        if !valid_next_entry(entries.last().copied(), entry, source.identity.size) {
            break;
        }
        entries.push(entry);
    }

    if source.identity.size == 0 || entries.first() != Some(&IndexEntry { line: 1, offset: 0 }) {
        entries.clear();
    }

    let valid_len = HEADER_LEN as u64 + entries.len() as u64 * RECORD_LEN as u64;
    file.set_len(valid_len)?;
    file.seek(SeekFrom::End(0))?;

    if source.identity.size > 0 && entries.is_empty() {
        let first = IndexEntry { line: 1, offset: 0 };
        file.write_all(&encode_record(first))?;
        entries.push(first);
        file.sync_data()?;
    }

    Ok((file, entries))
}

fn load_index_file(
    path: &Path,
    source: &SourceDescriptor,
    config: &IndexConfig,
) -> Result<LineIndex> {
    let mut file = File::open(path)?;
    let header = read_header(&mut file)?;
    if header.state != STATE_COMPLETE {
        return Err(IndexError::Corrupt("index is not complete".to_owned()));
    }
    if !header.matches(source, config) {
        return Err(IndexError::Corrupt(
            "index belongs to another source snapshot".to_owned(),
        ));
    }

    let expected_len = HEADER_LEN as u64
        + header
            .entry_count
            .checked_mul(RECORD_LEN as u64)
            .ok_or_else(|| IndexError::Corrupt("entry count overflow".to_owned()))?;
    if file.metadata()?.len() != expected_len {
        return Err(IndexError::Corrupt("index length mismatch".to_owned()));
    }
    let capacity = usize::try_from(header.entry_count)
        .map_err(|_| IndexError::Corrupt("too many index entries".to_owned()))?;
    let mut entries = Vec::with_capacity(capacity);
    let mut record_bytes = [0_u8; RECORD_LEN];
    for _ in 0..header.entry_count {
        file.read_exact(&mut record_bytes)?;
        let entry = decode_record(&record_bytes)?;
        if !valid_next_entry(entries.last().copied(), entry, source.identity.size) {
            return Err(IndexError::Corrupt(
                "index entries are not strictly increasing".to_owned(),
            ));
        }
        entries.push(entry);
    }

    if source.identity.size == 0 {
        if header.line_count != 0 || !entries.is_empty() {
            return Err(IndexError::Corrupt("invalid empty-file index".to_owned()));
        }
    } else {
        if header.line_count == 0 || header.line_count > source.identity.size {
            return Err(IndexError::Corrupt("invalid line count".to_owned()));
        }
        if entries.first() != Some(&IndexEntry { line: 1, offset: 0 }) {
            return Err(IndexError::Corrupt("missing first-line entry".to_owned()));
        }
        if entries
            .last()
            .is_some_and(|entry| entry.line > header.line_count)
        {
            return Err(IndexError::Corrupt(
                "entry line exceeds total line count".to_owned(),
            ));
        }
    }

    Ok(LineIndex {
        index_path: path.to_path_buf(),
        source_size: header.source_size,
        line_count: header.line_count,
        checkpoint_bytes: header.checkpoint_bytes,
        entries,
    })
}

fn read_header(file: &mut File) -> Result<IndexHeader> {
    let mut bytes = [0_u8; HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut bytes)?;
    IndexHeader::decode(&bytes)
}

fn valid_next_entry(previous: Option<IndexEntry>, entry: IndexEntry, source_size: u64) -> bool {
    if entry.line == 0 || entry.offset >= source_size {
        return false;
    }
    previous.is_none_or(|previous| entry.line > previous.line && entry.offset > previous.offset)
}

fn encode_record(entry: IndexEntry) -> [u8; RECORD_LEN] {
    let mut bytes = [0_u8; RECORD_LEN];
    put_u64(&mut bytes, 0, entry.line);
    put_u64(&mut bytes, 8, entry.offset);
    let checksum = crc32fast::hash(&bytes[..16]);
    put_u32(&mut bytes, 16, checksum);
    bytes
}

fn decode_record(bytes: &[u8; RECORD_LEN]) -> Result<IndexEntry> {
    if get_u32(bytes, 16) != crc32fast::hash(&bytes[..16]) {
        return Err(IndexError::Corrupt("record checksum mismatch".to_owned()));
    }
    Ok(IndexEntry {
        line: get_u64(bytes, 0),
        offset: get_u64(bytes, 8),
    })
}

fn open_sequential(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
        options.custom_flags(FILE_FLAG_SEQUENTIAL_SCAN);
    }
    options.open(path)
}

fn sample_hash(path: &Path, size: u64) -> Result<[u8; 32]> {
    let mut file = open_random(path)?;
    let mut hasher = Sha256::new();
    hasher.update(b"trace-search-source-sample-v1");
    hasher.update(size.to_le_bytes());

    let first_len = size.min(SAMPLE_BYTES as u64) as usize;
    let mut buffer = vec![0_u8; first_len];
    if first_len > 0 {
        file.read_exact(&mut buffer)?;
        hasher.update(b"first");
        hasher.update((first_len as u64).to_le_bytes());
        hasher.update(&buffer);
    }

    if size > SAMPLE_BYTES as u64 {
        let last_len = size.min(SAMPLE_BYTES as u64) as usize;
        file.seek(SeekFrom::Start(size - last_len as u64))?;
        buffer.resize(last_len, 0);
        file.read_exact(&mut buffer)?;
        hasher.update(b"last");
        hasher.update((last_len as u64).to_le_bytes());
        hasher.update(&buffer);
    }

    Ok(hasher.finalize().into())
}

fn modified_ns(metadata: &Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or(0)
}

fn hash_path(path: &Path) -> [u8; 32] {
    #[cfg(windows)]
    let normalized = path.to_string_lossy().to_lowercase();
    #[cfg(not(windows))]
    let normalized = path.to_string_lossy();
    Sha256::digest(normalized.as_bytes()).into()
}

fn line_estimate(size: u64, absolute: u64, newlines: u64, last_byte: Option<u8>) -> u64 {
    if size == 0 || absolute == 0 {
        0
    } else if absolute == size && last_byte == Some(b'\n') {
        newlines
    } else {
        newlines.saturating_add(1)
    }
}

fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    remove_if_exists(to)?;
    std::fs::rename(from, to)
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed slice"))
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed slice"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_loads_and_locates_lines() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("sample.log");
        std::fs::write(&source_path, b"one\ntwo\nthree\nfour\nfive").unwrap();
        let source = inspect_source(&source_path).unwrap();
        let config = IndexConfig {
            directory: temp.path().join("indexes"),
            checkpoint_bytes: 8,
        };
        let progress = BuildProgress::new(CancellationToken::new());

        let index = build_or_load(&source, &config, &progress, false).unwrap();
        assert_eq!(index.line_count, 5);
        assert!(index.entries.len() >= 2);

        let loaded = load_existing(&source, &config).unwrap().unwrap();
        assert_eq!(loaded.line_count, 5);
        let mut file = open_random(&source.canonical_path).unwrap();
        assert_eq!(locate_line_offset(&mut file, &loaded, 1).unwrap(), 0);
        assert_eq!(locate_line_offset(&mut file, &loaded, 3).unwrap(), 8);
        assert_eq!(
            locate_line_offset(&mut file, &loaded, 6).unwrap(),
            source.identity.size
        );
    }

    #[test]
    fn counts_empty_lines_and_terminal_newline() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("empty-lines.log");
        std::fs::write(&source_path, b"\nA\n\n").unwrap();
        let source = inspect_source(&source_path).unwrap();
        let config = IndexConfig {
            directory: temp.path().join("indexes"),
            checkpoint_bytes: 2,
        };
        let progress = BuildProgress::new(CancellationToken::new());
        let index = build_or_load(&source, &config, &progress, false).unwrap();
        assert_eq!(index.line_count, 3);
    }

    #[test]
    fn handles_empty_file() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("empty.log");
        std::fs::write(&source_path, b"").unwrap();
        let source = inspect_source(&source_path).unwrap();
        let config = IndexConfig {
            directory: temp.path().join("indexes"),
            checkpoint_bytes: 8,
        };
        let progress = BuildProgress::new(CancellationToken::new());
        let index = build_or_load(&source, &config, &progress, false).unwrap();
        assert_eq!(index.line_count, 0);
        assert!(index.entries.is_empty());
    }

    #[test]
    fn resumes_from_partial_checkpoint_records() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("resume.log");
        let content = (1..=100)
            .map(|line| format!("line-{line:03}\n"))
            .collect::<String>();
        std::fs::write(&source_path, content).unwrap();
        let source = inspect_source(&source_path).unwrap();
        let config = IndexConfig {
            directory: temp.path().join("indexes"),
            checkpoint_bytes: 32,
        };
        let first_progress = BuildProgress::new(CancellationToken::new());
        let complete = build_or_load(&source, &config, &first_progress, false).unwrap();
        assert!(complete.entries.len() > 2);

        let paths = index_paths(&source, &config);
        std::fs::remove_file(&paths.final_path).unwrap();
        let mut partial = File::create(&paths.partial_path).unwrap();
        partial
            .write_all(&IndexHeader::building(&source, &config).encode())
            .unwrap();
        for entry in complete.entries.iter().take(2) {
            partial.write_all(&encode_record(*entry)).unwrap();
        }
        partial.sync_all().unwrap();
        drop(partial);

        let resumed_progress = BuildProgress::new(CancellationToken::new());
        let resumed = build_or_load(&source, &config, &resumed_progress, false).unwrap();
        assert_eq!(resumed.line_count, 100);
        assert_eq!(resumed.entries, complete.entries);
        assert_eq!(resumed_progress.bytes_indexed(), source.identity.size);
    }
}
