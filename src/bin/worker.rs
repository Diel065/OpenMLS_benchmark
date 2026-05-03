use std::{
    net::SocketAddr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};

use mls_playground::client::Client;
use mls_playground::debug::{debug_logs_enabled, worker_debug_logs_enabled};
use mls_playground::worker_api::{
    handle_command, Command, CommandResponse, CompletedCommandCache, IncomingCommandRequest,
    PendingIntent,
};
use tokio::sync::{mpsc, oneshot};

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;

struct WorkerState {
    client: Client,
    queued_intent: Option<PendingIntent>,
    ds_url: String,
    relay_url: String,
    response_cache: CompletedCommandCache,
    debug_enabled: bool,
}

struct WorkerCommandEnvelope {
    request_id: Option<String>,
    command: Command,
    expected_epoch: Option<u64>,
    phase: Option<String>,
    enqueued_at: Instant,
    enqueued_unix_ms: u128,
    queue_depth_estimate: usize,
    response_tx: oneshot::Sender<CommandResponse>,
}

type CommandTx = mpsc::Sender<WorkerCommandEnvelope>;

fn parse_args() -> Result<(String, String, String, SocketAddr)> {
    let mut args = std::env::args().skip(1);

    let mut name: Option<String> = None;
    let mut ds_url: Option<String> = None;
    let mut relay_url: Option<String> = None;
    let mut listen_addr: Option<SocketAddr> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                name = args.next();
            }
            "--ds-url" => {
                ds_url = args.next();
            }
            "--relay-url" => {
                relay_url = args.next();
            }
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

    let name = name.ok_or_else(|| anyhow!("Missing --name"))?;
    let ds_url = ds_url.ok_or_else(|| anyhow!("Missing --ds-url"))?;
    let relay_url = relay_url.ok_or_else(|| anyhow!("Missing --relay-url"))?;
    let listen_addr = listen_addr.unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());

    Ok((name, ds_url, relay_url, listen_addr))
}

async fn health() -> &'static str {
    "ok"
}

async fn run_command(
    State(command_tx): State<CommandTx>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let (request_id, command, expected_epoch, phase) = request.into_parts();
    let (response_tx, response_rx) = oneshot::channel();
    let queue_depth_estimate = command_tx
        .max_capacity()
        .saturating_sub(command_tx.capacity());

    if debug_logs_enabled() {
        eprintln!(
            "[WORKER] enqueue command; queue_depth_estimate={} queue_capacity={}",
            queue_depth_estimate,
            command_tx.max_capacity()
        );
    }

    if command_tx
        .send(WorkerCommandEnvelope {
            request_id,
            command,
            expected_epoch,
            phase,
            enqueued_at: Instant::now(),
            enqueued_unix_ms: unix_ms_now(),
            queue_depth_estimate,
            response_tx,
        })
        .await
        .is_err()
    {
        return Json(CommandResponse::error(
            "worker command actor is not running",
        ));
    }

    match response_rx.await {
        Ok(response) => Json(response),
        Err(err) => Json(CommandResponse::error(format!(
            "worker command actor dropped response: {}",
            err
        ))),
    }
}

async fn command_actor(mut rx: mpsc::Receiver<WorkerCommandEnvelope>, mut state: WorkerState) {
    while let Some(envelope) = rx.recv().await {
        let request_id = envelope.request_id.as_deref().unwrap_or("-");
        let command_name = envelope.command.kind();
        let is_mutating = envelope.command.is_mls_mutating();
        let phase = envelope.phase.as_deref().unwrap_or("-");

        if let Some(request_id) = envelope.request_id.as_deref() {
            if let Some(response) = state.response_cache.get(request_id) {
                if state.debug_enabled {
                    eprintln!(
                        "[WORKER {}] command request_id={} command={} phase={} cache_hit=true queue_depth={} enqueued_unix_ms={} finish_unix_ms={} enqueued_ms_ago={} result_status={}",
                        state.client.name,
                        request_id,
                        command_name,
                        phase,
                        envelope.queue_depth_estimate,
                        envelope.enqueued_unix_ms,
                        unix_ms_now(),
                        envelope.enqueued_at.elapsed().as_millis(),
                        response.status
                    );
                }

                let _ = envelope.response_tx.send(response);
                continue;
            }
        }

        let start = Instant::now();
        let start_unix_ms = unix_ms_now();
        let before_epoch = if is_mutating {
            state.client.current_epoch_u64().ok()
        } else {
            None
        };

        if state.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} expected_epoch={:?} cache_hit=false queue_depth={} enqueued_unix_ms={} start_unix_ms={} enqueue_wait_ms={} epoch_before={:?}",
                state.client.name,
                request_id,
                command_name,
                phase,
                envelope.expected_epoch,
                envelope.queue_depth_estimate,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                envelope.enqueued_at.elapsed().as_millis(),
                before_epoch
            );
        }

        let result = handle_command(
            &mut state.client,
            &state.ds_url,
            &state.relay_url,
            &mut state.queued_intent,
            envelope.command,
        )
        .await;

        let response = match result {
            Ok(message) => CommandResponse::ok(message),
            Err(err) => CommandResponse::error(err.to_string()),
        };

        let after_epoch = if is_mutating {
            state.client.current_epoch_u64().ok()
        } else {
            None
        };

        if state.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} enqueued_unix_ms={} start_unix_ms={} finish_unix_ms={} finish_ms={} result_status={} epoch_before={:?} epoch_after={:?}",
                state.client.name,
                request_id,
                command_name,
                phase,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                unix_ms_now(),
                start.elapsed().as_millis(),
                response.status,
                before_epoch,
                after_epoch
            );
        }

        if let Some(request_id) = envelope.request_id {
            state.response_cache.insert(request_id, response.clone());
        }

        let _ = envelope.response_tx.send(response);
    }
}

fn unix_ms_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn command_queue_capacity() -> usize {
    std::env::var("OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|capacity| *capacity > 0)
        .unwrap_or(DEFAULT_COMMAND_QUEUE_CAPACITY)
}

fn idempotency_cache_size() -> usize {
    std::env::var("OPENMLS_WORKER_IDEMPOTENCY_CACHE_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2048)
}

fn idempotency_cache_ttl() -> Duration {
    let seconds = std::env::var("OPENMLS_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3600);

    Duration::from_secs(seconds)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (name, ds_url, relay_url, listen_addr) = parse_args()?;

    let queue_capacity = command_queue_capacity();
    let cache_size = idempotency_cache_size();
    let cache_ttl = idempotency_cache_ttl();
    let debug_enabled = worker_debug_logs_enabled(&name);
    let (command_tx, command_rx) = mpsc::channel(queue_capacity);

    tokio::spawn(command_actor(
        command_rx,
        WorkerState {
            client: Client::new(&name)?,
            queued_intent: None,
            ds_url: ds_url.clone(),
            relay_url: relay_url.clone(),
            response_cache: CompletedCommandCache::new(cache_size, cache_ttl),
            debug_enabled,
        },
    ));

    let app = Router::new()
        .route("/health", get(health))
        .route("/command", post(run_command))
        .with_state(command_tx);

    if debug_enabled || debug_logs_enabled() {
        eprintln!(
            "[WORKER {}] starting on http://{} with DS={} RELAY={} queue_capacity={} idempotency_cache_size={} idempotency_cache_ttl_seconds={}",
            name,
            listen_addr,
            ds_url,
            relay_url,
            queue_capacity,
            cache_size,
            cache_ttl.as_secs()
        );
    }

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .map_err(|e| anyhow!("Could not bind worker listener on {}: {}", listen_addr, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("Worker server crashed: {}", e))?;

    Ok(())
}
