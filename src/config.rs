use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Parser;

use crate::engine::{EngineConfig, MAX_SEARCH_THREADS};

const MIN_CHECKPOINT_BYTES: u64 = 64 * 1024;
const MAX_CHECKPOINT_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Args {
    /// HTTP listen address.
    #[arg(long, env = "TRACE_SEARCH_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,

    /// Directory for persistent binary indexes.
    #[arg(
        long,
        env = "TRACE_SEARCH_INDEX_DIR",
        default_value = ".trace-search-index"
    )]
    pub index_dir: PathBuf,

    /// Maximum byte distance between index checkpoints at line boundaries.
    #[arg(
        long,
        env = "TRACE_SEARCH_CHECKPOINT_BYTES",
        default_value_t = 8 * 1024 * 1024
    )]
    pub checkpoint_bytes: u64,

    /// Root directory under which export_lines may write files.
    #[arg(long, env = "TRACE_SEARCH_EXPORT_ROOT", default_value = ".")]
    pub export_root: PathBuf,

    /// Maximum lines accepted by one read_lines call.
    #[arg(long, env = "TRACE_SEARCH_MAX_READ_LINES", default_value_t = 100_000)]
    pub max_read_lines: u64,

    /// Worker threads used by one search_lines call; zero selects automatically, maximum 32.
    #[arg(long, env = "TRACE_SEARCH_THREADS", default_value_t = 0)]
    pub search_threads: usize,

    /// Approximate aggregate memory budget for concurrent search workers.
    #[arg(
        long,
        env = "TRACE_SEARCH_MEMORY_BUDGET_BYTES",
        default_value_t = 1024 * 1024 * 1024
    )]
    pub search_memory_budget_bytes: usize,

    /// Maximum matches returned by one search_lines call.
    #[arg(long, env = "TRACE_SEARCH_MAX_MATCHES", default_value_t = 10_000)]
    pub max_matches: u64,

    /// Maximum source bytes placed in one MCP response.
    #[arg(
        long,
        env = "TRACE_SEARCH_MAX_CONTENT_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    pub max_content_bytes: usize,

    /// Maximum single line size accepted by search_lines.
    #[arg(
        long,
        env = "TRACE_SEARCH_MAX_LINE_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    pub max_line_bytes: usize,

    /// Maximum regex or literal pattern size.
    #[arg(
        long,
        env = "TRACE_SEARCH_MAX_PATTERN_BYTES",
        default_value_t = 16 * 1024
    )]
    pub max_pattern_bytes: usize,

    /// Maximum lines copied by export_lines; zero means unlimited.
    #[arg(
        long,
        env = "TRACE_SEARCH_MAX_EXPORT_LINES",
        default_value_t = 100_000_000
    )]
    pub max_export_lines: u64,

    /// Maximum concurrent read/search/export operations.
    #[arg(long, env = "TRACE_SEARCH_QUERY_CONCURRENCY", default_value_t = 4)]
    pub query_concurrency: usize,

    /// Bearer token required by every HTTP endpoint. Prefer the environment variable.
    #[arg(long, env = "TRACE_SEARCH_BEARER_TOKEN", hide_env_values = true)]
    pub bearer_token: Option<String>,

    /// Explicitly permit a non-loopback listener without authentication.
    #[arg(long, env = "TRACE_SEARCH_ALLOW_UNAUTHENTICATED_REMOTE")]
    pub allow_unauthenticated_remote: bool,

    /// Additional Host header values accepted by the MCP transport.
    #[arg(
        long = "allowed-host",
        env = "TRACE_SEARCH_ALLOWED_HOSTS",
        value_delimiter = ','
    )]
    pub allowed_hosts: Vec<String>,

    /// Browser Origin values accepted by the MCP transport.
    #[arg(
        long = "allowed-origin",
        env = "TRACE_SEARCH_ALLOWED_ORIGINS",
        value_delimiter = ','
    )]
    pub allowed_origins: Vec<String>,
}

impl Args {
    pub fn validate(&self) -> Result<()> {
        if !(MIN_CHECKPOINT_BYTES..=MAX_CHECKPOINT_BYTES).contains(&self.checkpoint_bytes) {
            bail!(
                "checkpoint-bytes must be between {MIN_CHECKPOINT_BYTES} and {MAX_CHECKPOINT_BYTES}"
            );
        }
        if self.max_read_lines == 0 {
            bail!("max-read-lines must be greater than zero");
        }
        if self.search_threads > MAX_SEARCH_THREADS {
            bail!("search-threads must not exceed {MAX_SEARCH_THREADS}");
        }
        if self.search_memory_budget_bytes == 0 {
            bail!("search-memory-budget-bytes must be greater than zero");
        }
        if self.max_matches == 0 {
            bail!("max-matches must be greater than zero");
        }
        if self.max_content_bytes == 0 {
            bail!("max-content-bytes must be greater than zero");
        }
        if self.max_line_bytes == 0 {
            bail!("max-line-bytes must be greater than zero");
        }
        if self.max_pattern_bytes == 0 {
            bail!("max-pattern-bytes must be greater than zero");
        }
        if self.query_concurrency == 0 {
            bail!("query-concurrency must be greater than zero");
        }
        if !self.bind.ip().is_loopback()
            && self.bearer_token.is_none()
            && !self.allow_unauthenticated_remote
        {
            bail!(
                "a non-loopback listener requires TRACE_SEARCH_BEARER_TOKEN or --allow-unauthenticated-remote"
            );
        }
        if let Some(token) = &self.bearer_token
            && (token.is_empty() || token.bytes().any(|byte| byte.is_ascii_whitespace()))
        {
            bail!("bearer token must be non-empty and contain no ASCII whitespace");
        }
        Ok(())
    }

    pub fn engine_config(&self) -> Result<EngineConfig> {
        let index_dir = prepare_directory(&self.index_dir, "index directory")?;
        let export_root = prepare_directory(&self.export_root, "export root")?;

        Ok(EngineConfig {
            index_dir,
            export_root,
            checkpoint_bytes: self.checkpoint_bytes,
            max_read_lines: self.max_read_lines,
            search_threads: effective_search_threads(self.search_threads),
            search_memory_budget_bytes: self.search_memory_budget_bytes,
            max_matches: self.max_matches,
            max_content_bytes: self.max_content_bytes,
            max_line_bytes: self.max_line_bytes,
            max_pattern_bytes: self.max_pattern_bytes,
            max_export_lines: self.max_export_lines,
            query_concurrency: self.query_concurrency,
        })
    }

    pub fn effective_allowed_hosts(&self) -> Vec<String> {
        let mut hosts = BTreeSet::from([
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
            "::1".to_owned(),
        ]);
        if !self.bind.ip().is_unspecified() {
            hosts.insert(self.bind.ip().to_string());
        }
        hosts.extend(self.allowed_hosts.iter().cloned());
        hosts.into_iter().collect()
    }
}

fn effective_search_threads(configured: usize) -> usize {
    if configured != 0 {
        return configured;
    }
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(8)
}

fn prepare_directory(path: &Path, label: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create {label}: {}", path.display()))?;
    std::fs::canonicalize(path)
        .with_context(|| format!("failed to canonicalize {label}: {}", path.display()))
}
