use std::sync::Arc;

use rmcp::{
    Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};

use crate::engine::{
    ExportLinesRequest, ExportLinesResponse, FileEngine, FileStatusResponse, OpenFileRequest,
    OpenFileResponse, ReadLinesRequest, ReadLinesResponse, SearchLinesRequest, SearchLinesResponse,
};

#[derive(Clone)]
pub struct TraceSearchServer {
    engine: Arc<FileEngine>,
    tool_router: ToolRouter<Self>,
}

#[tool_router(router = tool_router)]
impl TraceSearchServer {
    pub fn new(engine: Arc<FileEngine>) -> Self {
        Self {
            engine,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "open_file",
        description = "Open the single active file. A valid persistent index is reused; otherwise indexing starts asynchronously. Poll get_file_status until state is ready."
    )]
    async fn open_file(
        &self,
        Parameters(request): Parameters<OpenFileRequest>,
    ) -> Result<Json<OpenFileResponse>, String> {
        self.engine
            .open_file(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "get_file_status",
        description = "Return the active file and index state, including first-pass indexing progress."
    )]
    async fn get_file_status(&self) -> Json<FileStatusResponse> {
        Json(self.engine.status().await)
    }

    #[tool(
        name = "close_file",
        description = "Close the active file or cancel an in-progress index build. Existing persistent indexes are retained."
    )]
    async fn close_file(&self) -> Json<FileStatusResponse> {
        Json(self.engine.close_file().await)
    }

    #[tool(
        name = "read_lines",
        description = "Read at most line_count lines beginning at one-based start_line. Line endings are preserved."
    )]
    async fn read_lines(
        &self,
        Parameters(request): Parameters<ReadLinesRequest>,
    ) -> Result<Json<ReadLinesResponse>, String> {
        self.engine
            .read_lines(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "search_lines",
        description = "Search the caller-provided max_scan_lines range beginning at one-based start_line using parallel indexed chunks. regex=false uses a precompiled literal search; regex=true uses Rust regex syntax."
    )]
    async fn search_lines(
        &self,
        Parameters(request): Parameters<SearchLinesRequest>,
    ) -> Result<Json<SearchLinesResponse>, String> {
        self.engine
            .search_lines(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "export_lines",
        description = "Copy at most line_count lines beginning at one-based start_line to output_path under the configured export root. Data is streamed and the destination is published atomically."
    )]
    async fn export_lines(
        &self,
        Parameters(request): Parameters<ExportLinesRequest>,
    ) -> Result<Json<ExportLinesResponse>, String> {
        self.engine
            .export_lines(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for TraceSearchServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "trace-search-mcp",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Call open_file first. If indexing starts, poll get_file_status until ready. Lines are one-based and the active source is treated as an immutable snapshot.",
            )
    }
}
