# trace-search-mcp

一个使用 Rust 编写、仅通过 MCP Streamable HTTP 暴露能力的超大文本文件查询服务。它面向 800 GiB 以上的只读、按行组织的日志或 trace 文件，不会把整个文件映射或加载进内存。

## 功能

- 同一时刻只允许一个活动文件。
- 第一次打开文件时异步建立持久化索引，后续打开直接复用。
- 从指定行开始快速读取指定行数。
- 从指定行开始，在限定行数内执行正则表达式搜索。
- 从指定行开始，在限定行数内执行高效字面量搜索。
- 将指定行范围原样、流式、原子地保存到指定路径。
- 索引中断后可从最后一个有效检查点继续。
- HTTP Bearer Token、Host 校验和可选 Origin 校验。

## 快速启动

```powershell
cargo build --release
$env:TRACE_SEARCH_BEARER_TOKEN = "replace-with-a-long-random-token"
target\release\trace-search-mcp.exe `
  --bind 127.0.0.1:8080 `
  --index-dir D:\trace-index `
  --export-root D:\trace-export
```

MCP 地址：`http://127.0.0.1:8080/mcp`

通用 MCP 客户端配置示例：

```json
{
  "mcpServers": {
    "trace-search": {
      "type": "http",
      "url": "http://127.0.0.1:8080/mcp",
      "headers": {
        "Authorization": "Bearer replace-with-a-long-random-token"
      }
    }
  }
}
```

不同客户端的配置字段可能略有差异，但传输类型必须是 Streamable HTTP，不能配置为 stdio。

## MCP 工具

### `open_file`

```json
{
  "path": "D:\\logs\\trace.log",
  "force_rebuild": false
}
```

存在有效索引时立即返回 `ready`。首次打开时返回 `indexing`，索引在后台构建，避免单个 HTTP 请求持续数小时。

### `get_file_status`

无参数。索引期间返回字节进度、发现的行数、吞吐和预计剩余时间。只有 `state` 为 `ready` 时才能查询内容。

### `read_lines`

```json
{
  "start_line": 1000000000,
  "line_count": 200
}
```

返回内容保留源文件中的 `LF` 或 `CRLF`。若内容超过 `--max-content-bytes`，应减少行数或改用 `export_lines`。

### `search_lines`

正则搜索：

```json
{
  "pattern": "ERROR\\s+request_id=[0-9a-f]+",
  "start_line": 1000000000,
  "max_scan_lines": 500000,
  "max_matches": 100,
  "regex": true,
  "case_sensitive": true
}
```

字面量搜索：

```json
{
  "pattern": "connection reset",
  "start_line": 1000000000,
  "max_scan_lines": 500000,
  "max_matches": 100,
  "regex": false,
  "case_sensitive": false
}
```

`next_line` 可用于分页继续搜索。搜索范围完全由调用方传入的 `max_scan_lines` 决定，没有额外的服务端行数上限。`scanned_lines` 表示按行序确认完成的逻辑前缀；并行线程可能已预读后续分块。字面量且区分大小写时使用预编译 SIMD `memmem`；其他模式使用 Rust regex 引擎，不存在灾难性回溯。

### `export_lines`

```json
{
  "start_line": 1000000000,
  "line_count": 1000000,
  "output_path": "incident-42.log",
  "overwrite": false
}
```

相对路径基于 `--export-root`。绝对路径也必须位于该根目录下，且父目录必须已经存在。写入临时文件并同步成功后才发布目标文件。

### `close_file`

无参数。关闭活动文件；若正在建立索引，则请求取消。已经完成或部分完成的索引不会被删除。

## 性能模型

默认每隔 8 MiB 在下一个行边界记录一条 `(行号, 字节偏移)`：

- 800 GiB 文件约产生 102,400 条记录。
- 每条记录 20 字节，索引主体约 2 MiB。
- 行号定位先在内存中二分检查点，再扫描最多约 8 MiB；超长单行可能突破这个距离。
- 首次索引只顺序读取文件一次，使用 32 MiB 缓冲、SIMD 换行计数和 Windows 顺序读取提示。
- 任意正则无法预先建立通用内容索引。搜索会定位范围首尾，并利用已有行边界检查点把范围切成近似等字节块，由多个独立文件句柄并行顺序扫描。
- 每个搜索工作线程复用已编译匹配器；普通行直接在 4 MiB 读取缓冲的切片上匹配，仅跨缓冲行和命中行发生复制。结果通过有序同步通道归并，返回顺序、`next_line` 和各项响应上限与单线程语义一致。
- 导出只定位首尾字节偏移，然后用 8 MiB 缓冲流式复制，不占用与导出大小成比例的内存。

可通过 `--checkpoint-bytes` 调整空间和随机定位 I/O 的权衡。NVMe 场景可使用 4-16 MiB；机械盘可使用 16-64 MiB 来减小索引记录数量。

## 重要配置

| 参数 | 环境变量 | 默认值 |
| --- | --- | --- |
| `--bind` | `TRACE_SEARCH_BIND` | `127.0.0.1:8080` |
| `--index-dir` | `TRACE_SEARCH_INDEX_DIR` | `.trace-search-index` |
| `--checkpoint-bytes` | `TRACE_SEARCH_CHECKPOINT_BYTES` | `8388608` |
| `--export-root` | `TRACE_SEARCH_EXPORT_ROOT` | 当前目录 |
| `--max-read-lines` | `TRACE_SEARCH_MAX_READ_LINES` | `100000` |
| `--search-threads` | `TRACE_SEARCH_THREADS` | `0`（自动，最多 8） |
| `--search-memory-budget-bytes` | `TRACE_SEARCH_MEMORY_BUDGET_BYTES` | `1073741824` |
| `--max-matches` | `TRACE_SEARCH_MAX_MATCHES` | `10000` |
| `--max-content-bytes` | `TRACE_SEARCH_MAX_CONTENT_BYTES` | `16777216` |
| `--max-line-bytes` | `TRACE_SEARCH_MAX_LINE_BYTES` | `67108864` |
| `--max-export-lines` | `TRACE_SEARCH_MAX_EXPORT_LINES` | `100000000` |
| `--query-concurrency` | `TRACE_SEARCH_QUERY_CONCURRENCY` | `4` |
| `--bearer-token` | `TRACE_SEARCH_BEARER_TOKEN` | 无 |
| `--allowed-host` | `TRACE_SEARCH_ALLOWED_HOSTS` | 本地地址 |
| `--allowed-origin` | `TRACE_SEARCH_ALLOWED_ORIGINS` | 不校验 Origin |

`--search-threads=0` 会按可用 CPU 自动选择且最多使用 8 个线程；显式值必须在 `1..=32`。实际线程数还会按搜索内存预算、`--query-concurrency`、读取缓冲、`--max-line-bytes` 和 `--max-content-bytes` 自动下调，极端配置下仍至少保留一个线程。HTTP 请求被取消后，取消信号会传递到搜索线程，且查询并发许可会保留到后台 I/O 实际结束。

运行 `trace-search-mcp --help` 查看完整参数。

## 一致性与限制

- 行号从 1 开始，以字节 `\n` 划分；文件末尾的换行不会额外产生一个虚拟空行。
- 文件被视为不可变快照。大小或修改时间变化后，查询会失败，必须重新 `open_file` 建立新索引。
- 内容按 UTF-8 返回；非法字节使用替换字符并设置 `lossy_utf8=true`。索引和字面量匹配仍基于原始字节。
- `search_lines` 对单行大小有限制，避免异常单行耗尽内存；`read_lines` 和 `export_lines` 仍可定位或导出超长行。
- 多线程搜索或多个并发查询可能使机械盘频繁寻道，可将 `--search-threads` 和 `--query-concurrency` 都调低到 `1`。
- 非本地监听默认必须配置 Bearer Token。跨主机部署应在反向代理上启用 TLS，因为明文 HTTP 会暴露 Token 和文件内容。

## 验证

```powershell
cargo fmt --all --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo build --release
```

更详细的索引格式、状态机和复杂度说明见 [DESIGN.md](DESIGN.md)。
