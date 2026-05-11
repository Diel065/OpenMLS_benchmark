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

use signal_benchmark::message_relay::MessageRelay;

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
        .route("/metrics", get(metrics))
        .route(
            "/conversation/{conversation_id}/message/{sender}",
            post(publish_pairwise_message),
        )
        .route("/message/{recipient}", get(fetch_message))
        .route("/message/{recipient}/pending", get(fetch_pending_message))
        .route("/message/{recipient}/ack/{message_id}", post(ack_message))
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

async fn metrics(
    State(state): State<SharedRelay>,
) -> axum::Json<signal_benchmark::service_metrics::ServiceMetricsSnapshot> {
    axum::Json(state.metrics().snapshot())
}

async fn publish_pairwise_message(
    State(state): State<SharedRelay>,
    Path((conversation_id, sender)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let metrics = state.metrics().start("publish_pairwise_message");
    let recipients_header = match headers.get("x-recipients") {
        Some(value) => value,
        None => {
            let response = (
                StatusCode::BAD_REQUEST,
                "Missing required x-recipients header",
            )
                .into_response();
            metrics.finish_http_status(response.status().as_u16());
            return response;
        }
    };

    let recipients = match parse_recipients_header(recipients_header) {
        Ok(recipients) => recipients,
        Err(message) => {
            let response = (StatusCode::BAD_REQUEST, message).into_response();
            metrics.finish_http_status(response.status().as_u16());
            return response;
        }
    };

    let response =
        match state.publish_pairwise_message(&conversation_id, &sender, &recipients, body.to_vec())
        {
            Ok(()) => StatusCode::OK.into_response(),
            Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
        };
    metrics.finish_http_status(response.status().as_u16());
    response
}

async fn fetch_message(
    State(state): State<SharedRelay>,
    Path(recipient): Path<String>,
) -> Response {
    let metrics = state.metrics().start("fetch_message");
    let response = match state.fetch_message(&recipient) {
        Some(bytes) => bytes_response(bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    metrics.finish_http_status(response.status().as_u16());
    response
}

async fn fetch_pending_message(
    State(state): State<SharedRelay>,
    Path(recipient): Path<String>,
) -> Response {
    let metrics = state.metrics().start("fetch_pending_message");
    let response = match state.fetch_pending_message(&recipient) {
        Some(message) => axum::Json(message).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    metrics.finish_http_status(response.status().as_u16());
    response
}

async fn ack_message(
    State(state): State<SharedRelay>,
    Path((recipient, message_id)): Path<(String, String)>,
) -> Response {
    let metrics = state.metrics().start("ack_message");
    let acked = state.ack_message(&recipient, &message_id);
    let response = if acked {
        StatusCode::OK.into_response()
    } else {
        (StatusCode::OK, "message was already acked or absent").into_response()
    };
    metrics.finish_http_status(response.status().as_u16());
    response
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
