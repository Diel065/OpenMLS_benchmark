use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};

use signal_benchmark::debug::debug_logs_enabled;
use signal_benchmark::key_repository::{
    KeyRepository, PrekeyBundleBatchStorable, PrekeyBundleStorable,
};

type SharedKr = Arc<KeyRepository>;

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

    let state: SharedKr = Arc::new(KeyRepository::new());

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route(
            "/prekey-bundle/{participant}",
            put(publish_prekey_bundle).get(fetch_prekey_bundle),
        )
        .route("/prekey-bundles/{participant}", put(publish_prekey_bundles))
        .route(
            "/prekey-bundle/{participant}/consume",
            post(consume_one_time_prekey),
        )
        .route(
            "/prekey-bundle/{participant}/consumed",
            get(get_one_time_prekeys_consumed),
        )
        .route("/prekey-bundle/{participant}", delete(remove_participant))
        .with_state(state);

    println!("[KR] Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow!("Could not bind KR listener on {}: {}", addr, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("KR server crashed: {}", e))?;

    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics(
    State(state): State<SharedKr>,
) -> Json<signal_benchmark::service_metrics::ServiceMetricsSnapshot> {
    Json(state.metrics().snapshot())
}

async fn publish_prekey_bundle(
    State(state): State<SharedKr>,
    Path(participant): Path<String>,
    Json(bundle): Json<PrekeyBundleStorable>,
) -> StatusCode {
    let metrics = state.metrics().start("publish_prekey_bundle");
    state.publish_prekey_bundle(&participant, bundle);
    if debug_logs_enabled() {
        println!("[KR] Stored prekey bundle for {}", participant);
    }
    metrics.finish(false);
    StatusCode::OK
}

async fn publish_prekey_bundles(
    State(state): State<SharedKr>,
    Path(participant): Path<String>,
    Json(batch): Json<PrekeyBundleBatchStorable>,
) -> StatusCode {
    let metrics = state.metrics().start("publish_prekey_bundles");
    let count = batch.one_time_prekeys.len() + usize::from(batch.signed_prekey_fallback);
    state.publish_prekey_bundle_batch(&participant, batch);
    if debug_logs_enabled() {
        println!("[KR] Stored {} prekey bundles for {}", count, participant);
    }
    metrics.finish(false);
    StatusCode::OK
}

async fn fetch_prekey_bundle(
    State(state): State<SharedKr>,
    Path(participant): Path<String>,
) -> Response {
    let metrics = state.metrics().start("fetch_prekey_bundle");
    let response = match state.fetch_prekey_bundle(&participant) {
        Some(bundle) => Json(bundle).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    metrics.finish_http_status(response.status().as_u16());
    response
}

async fn consume_one_time_prekey(
    State(state): State<SharedKr>,
    Path(participant): Path<String>,
) -> StatusCode {
    let metrics = state.metrics().start("consume_one_time_prekey");
    let consumed = state.consume_one_time_prekey(&participant);
    if debug_logs_enabled() {
        println!(
            "[KR] Consumed one-time prekey for {} consumed={}",
            participant, consumed
        );
    }
    metrics.finish(false);
    StatusCode::OK
}

async fn get_one_time_prekeys_consumed(
    State(state): State<SharedKr>,
    Path(participant): Path<String>,
) -> Json<serde_json::Value> {
    let metrics = state.metrics().start("get_one_time_prekeys_consumed");
    let count = state.one_time_prekeys_consumed(&participant);
    metrics.finish(false);
    Json(serde_json::json!({ "participant": participant, "consumed": count }))
}

async fn remove_participant(
    State(state): State<SharedKr>,
    Path(participant): Path<String>,
) -> StatusCode {
    let metrics = state.metrics().start("remove_participant");
    state.remove_participant(&participant);
    if debug_logs_enabled() {
        println!("[KR] Removed participant {}", participant);
    }
    metrics.finish(false);
    StatusCode::OK
}
