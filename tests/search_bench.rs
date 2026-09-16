use std::{path::PathBuf, sync::Arc, time::Instant};

use tokio::time::{Duration, sleep};
use trace_search_mcp::{
    engine::{EngineConfig, FileEngine, OpenFileRequest, SearchLinesRequest},
    index,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual release-mode benchmark"]
async fn benchmarks_full_file_literal_search() {
    let source_path = PathBuf::from(
        std::env::var_os("TRACE_SEARCH_BENCH_FILE")
            .expect("set TRACE_SEARCH_BENCH_FILE to a large log file"),
    );
    let source = index::inspect_source(&source_path).unwrap();
    let index_dir = std::env::current_dir()
        .unwrap()
        .join("target")
        .join("search-bench-index");
    std::fs::create_dir_all(&index_dir).unwrap();
    let index_dir = std::fs::canonicalize(index_dir).unwrap();
    let export_root = source
        .canonical_path
        .parent()
        .and_then(|path| std::fs::canonicalize(path).ok())
        .unwrap();
    let threads = std::env::var("TRACE_SEARCH_BENCH_THREADS")
        .unwrap_or_else(|_| "1,2,4,8".to_owned())
        .split(',')
        .map(|value| value.trim().parse::<usize>().unwrap())
        .collect::<Vec<_>>();
    let pattern = std::env::var("TRACE_SEARCH_BENCH_PATTERN")
        .unwrap_or_else(|_| "__TRACE_SEARCH_BENCH_PATTERN_THAT_DOES_NOT_EXIST__".to_owned());
    let repeats = std::env::var("TRACE_SEARCH_BENCH_REPEATS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(1);

    println!(
        "benchmark file={} bytes={} threads={threads:?} pattern={pattern:?}",
        source.canonical_path.display(),
        source.identity.size,
    );

    for thread_count in threads {
        let engine = Arc::new(FileEngine::new(EngineConfig {
            index_dir: index_dir.clone(),
            export_root: export_root.clone(),
            checkpoint_bytes: 8 * 1024 * 1024,
            max_read_lines: 100_000,
            search_threads: thread_count,
            search_memory_budget_bytes: 1024 * 1024 * 1024,
            max_matches: 10_000,
            max_content_bytes: 16 * 1024 * 1024,
            max_line_bytes: 64 * 1024 * 1024,
            max_pattern_bytes: 16 * 1024,
            max_export_lines: 100_000_000,
            query_concurrency: 1,
        }));

        let index_started = Instant::now();
        engine
            .open_file(OpenFileRequest {
                path: source.canonical_path.to_string_lossy().into_owned(),
                force_rebuild: false,
            })
            .await
            .unwrap();
        loop {
            let status = engine.status().await;
            match status.state.as_str() {
                "ready" => break,
                "failed" => panic!("indexing failed: {:?}", status.error),
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
        println!(
            "threads={thread_count} index_ready_seconds={:.3}",
            index_started.elapsed().as_secs_f64()
        );

        for repeat in 1..=repeats {
            let search_started = Instant::now();
            let response = engine
                .search_lines(SearchLinesRequest {
                    pattern: pattern.clone(),
                    start_line: 1,
                    max_scan_lines: u64::MAX,
                    max_matches: Some(10_000),
                    regex: Some(false),
                    case_sensitive: Some(true),
                })
                .await
                .unwrap();
            let elapsed = search_started.elapsed().as_secs_f64();
            println!(
                "threads={thread_count} repeat={repeat} search_seconds={elapsed:.6} full_scan_gib_per_second={:.3} scanned_lines={} matches={} eof={} match_limit_reached={}",
                source.identity.size as f64 / elapsed / (1024.0 * 1024.0 * 1024.0),
                response.scanned_lines,
                response.matches.len(),
                response.eof,
                response.match_limit_reached,
            );
        }
        engine.shutdown().await;
    }
}
