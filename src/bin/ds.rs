use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use mls_playground::debug::debug_logs_enabled;
use mls_playground::delivery_service::{DeliveryService, GroupInfo};

type SharedDs = Arc<DeliveryService>;

#[derive(Debug, Deserialize)]
struct GroupStatePutRequest {
    members: Vec<String>,
}

#[derive(Debug, Serialize)]
struct GroupStateResponse {
    group_id: String,
    current_epoch: u64,
    members: Vec<String>,
}

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

    Ok(listen_addr.unwrap_or_else(|| "127.0.0.1:3000".parse().unwrap()))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let addr = parse_args()?;

    let state: SharedDs = Arc::new(DeliveryService::new());

    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/keypackage/{owner}",
            post(publish_key_package).get(fetch_key_package),
        )
        .route("/commit/{recipient}", get(fetch_commit))
        .route(
            "/welcome/{recipient}",
            post(publish_welcome).get(fetch_welcome),
        )
        .route(
            "/ratchet-tree/{recipient}",
            post(publish_ratchet_tree).get(fetch_ratchet_tree),
        )
        .route("/group/{group_id}/state/{epoch}", put(put_group_state))
        .route("/group/{group_id}/state", get(get_group_state))
        .route(
            "/group/{group_id}/commit/{sender}/{epoch}",
            post(publish_group_commit),
        )
        .with_state(state);

    println!("[DS] Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow!("Could not bind DS listener on {}: {}", addr, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("DS server crashed: {}", e))?;

    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn publish_key_package(
    State(state): State<SharedDs>,
    Path(owner): Path<String>,
    body: Bytes,
) -> StatusCode {
    state.publish_key_package(&owner, body.to_vec());
    if debug_logs_enabled() {
        println!("[DS] Stored KeyPackage for {}", owner);
    }
    StatusCode::OK
}

async fn fetch_key_package(State(state): State<SharedDs>, Path(owner): Path<String>) -> Response {
    match state.fetch_key_package(&owner) {
        Some(bytes) => bytes_response(bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn put_group_state(
    State(state): State<SharedDs>,
    Path((group_id, epoch)): Path<(String, u64)>,
    Json(body): Json<GroupStatePutRequest>,
) -> Response {
    match state.put_group_state(&group_id, epoch, body.members) {
        Ok(()) => {
            if debug_logs_enabled() {
                println!(
                    "[DS] Updated group state for group={} epoch={}",
                    group_id, epoch
                );
            }
            StatusCode::OK.into_response()
        }
        Err(message) => (StatusCode::CONFLICT, message).into_response(),
    }
}

async fn get_group_state(State(state): State<SharedDs>, Path(group_id): Path<String>) -> Response {
    match state.get_group_state(&group_id) {
        Some(GroupInfo {
            current_epoch,
            members,
        }) => Json(GroupStateResponse {
            group_id,
            current_epoch,
            members,
        })
        .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn publish_group_commit(
    State(state): State<SharedDs>,
    Path((group_id, sender, epoch)): Path<(String, String, u64)>,
    body: Bytes,
) -> Response {
    match state.publish_group_commit(&group_id, &sender, epoch, body.to_vec()) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(message) => (StatusCode::CONFLICT, message).into_response(),
    }
}

async fn fetch_commit(State(state): State<SharedDs>, Path(recipient): Path<String>) -> Response {
    match state.fetch_commit(&recipient) {
        Some(bytes) => bytes_response(bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn publish_welcome(
    State(state): State<SharedDs>,
    Path(recipient): Path<String>,
    body: Bytes,
) -> StatusCode {
    state.publish_welcome(&recipient, body.to_vec());
    if debug_logs_enabled() {
        println!("[DS] Stored Welcome for {}", recipient);
    }
    StatusCode::OK
}

async fn fetch_welcome(State(state): State<SharedDs>, Path(recipient): Path<String>) -> Response {
    match state.fetch_welcome(&recipient) {
        Some(bytes) => bytes_response(bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn publish_ratchet_tree(
    State(state): State<SharedDs>,
    Path(recipient): Path<String>,
    body: Bytes,
) -> StatusCode {
    state.publish_ratchet_tree(&recipient, body.to_vec());
    if debug_logs_enabled() {
        println!("[DS] Stored ratchet tree for {}", recipient);
    }
    StatusCode::OK
}

async fn fetch_ratchet_tree(
    State(state): State<SharedDs>,
    Path(recipient): Path<String>,
) -> Response {
    match state.fetch_ratchet_tree(&recipient) {
        Some(bytes) => bytes_response(bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn bytes_response(bytes: Vec<u8>) -> Response {
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
}
