use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};

use mls_playground::message_relay::MessageRelay;

type SharedRelay = Arc<MessageRelay>;

fn parse_args() -> Result<SocketAddr> {
    let mut args = std::env::args().skip(1);
    let mut listen_addr: Option<SocketAddr> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen-addr" => {
                let raw = args
                    .next()
                    .ok_or_else(|| anyhow!("Missing value after --listen-addr"))?;
                let parsed: SocketAddr = raw
                    .parse()
                    .map_err(|e| anyhow!("Invalid --listen-addr '{}': {}", raw, e))?;
                listen_addr = Some(parsed);
            }
            _ => {}
        }
    }

    Ok(listen_addr.unwrap_or_else(|| "127.0.0.1:4000".parse().unwrap()))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let addr = parse_args()?;

    let state: SharedRelay = Arc::new(MessageRelay::new());

    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/group/{group_id}/application-message/{sender}",
            post(publish_group_application_message),
        )
        .route(
            "/application-message/{recipient}",
            get(fetch_application_message),
        )
        .with_state(state);

    println!("[RELAY] Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow!("Could not bind relay listener on {}: {}", addr, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("Message relay server crashed: {}", e))?;

    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn publish_group_application_message(
    State(state): State<SharedRelay>,
    Path((group_id, sender)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let recipients_header = match headers.get("x-recipients") {
        Some(value) => value,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "Missing required x-recipients header",
            )
                .into_response()
        }
    };

    let recipients = match parse_recipients_header(recipients_header) {
        Ok(recipients) => recipients,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };

    match state.publish_group_application_message(&group_id, &sender, &recipients, body.to_vec()) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

async fn fetch_application_message(
    State(state): State<SharedRelay>,
    Path(recipient): Path<String>,
) -> Response {
    match state.fetch_application_message(&recipient) {
        Some(bytes) => bytes_response(bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn parse_recipients_header(value: &HeaderValue) -> Result<Vec<String>, String> {
    let raw = value
        .to_str()
        .map_err(|_| "x-recipients header is not valid UTF-8".to_string())?;

    let recipients: Vec<String> = raw
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect();

    if recipients.is_empty() {
        return Err("x-recipients header did not contain any recipients".to_string());
    }

    Ok(recipients)
}

fn bytes_response(bytes: Vec<u8>) -> Response {
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
}
