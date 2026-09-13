use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;
use trace_search_mcp::{config::Args, engine::FileEngine, mcp::TraceSearchServer};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AuthState {
    expected_authorization: Option<Arc<Vec<u8>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("trace_search_mcp=info,rmcp=info")),
        )
        .init();

    let args = Args::parse();
    args.validate()?;
    let engine = Arc::new(FileEngine::new(args.engine_config()?));
    let cancellation = CancellationToken::new();

    let engine_for_factory = engine.clone();
    let transport_config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_cancellation_token(cancellation.child_token())
        .with_allowed_hosts(args.effective_allowed_hosts())
        .with_allowed_origins(args.allowed_origins.clone());
    let mcp_service: StreamableHttpService<TraceSearchServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(TraceSearchServer::new(engine_for_factory.clone())),
            Default::default(),
            transport_config,
        );

    let auth = AuthState {
        expected_authorization: args
            .bearer_token
            .as_ref()
            .map(|token| Arc::new(format!("Bearer {token}").into_bytes())),
    };
    let app = Router::new()
        .nest_service("/mcp", mcp_service)
        .route("/healthz", get(health))
        .layer(middleware::from_fn_with_state(auth, require_auth));

    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("failed to bind {}", args.bind))?;
    tracing::info!(address = %args.bind, endpoint = "/mcp", "HTTP MCP server listening");

    let shutdown_token = cancellation.clone();
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "failed to install Ctrl+C handler");
            }
            shutdown_token.cancel();
        })
        .await;

    cancellation.cancel();
    engine.shutdown().await;
    server_result.context("HTTP server failed")
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn require_auth(State(auth): State<AuthState>, request: Request, next: Next) -> Response {
    let Some(expected) = auth.expected_authorization else {
        return next.run(request).await;
    };
    let authorized = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .is_some_and(|actual| {
            let actual = actual.as_bytes();
            actual.len() == expected.len() && bool::from(actual.ct_eq(expected.as_slice()))
        });
    if authorized {
        return next.run(request).await;
    }

    let mut response = (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}
