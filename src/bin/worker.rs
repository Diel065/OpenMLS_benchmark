use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};

use mls_playground::client::Client;
use mls_playground::debug::debug_logs_enabled;
use mls_playground::worker_api::{handle_command, Command, CommandResponse, PendingIntent};
use tokio::sync::{mpsc, oneshot};

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;

struct WorkerState {
    client: Client,
    queued_intent: Option<PendingIntent>,
    ds_url: String,
    relay_url: String,
}

struct WorkerCommandEnvelope {
    command: Command,
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
    Json(command): Json<Command>,
) -> Json<CommandResponse> {
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
            command,
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

        let _ = envelope.response_tx.send(response);
    }
}

fn command_queue_capacity() -> usize {
    std::env::var("OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|capacity| *capacity > 0)
        .unwrap_or(DEFAULT_COMMAND_QUEUE_CAPACITY)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (name, ds_url, relay_url, listen_addr) = parse_args()?;

    let queue_capacity = command_queue_capacity();
    let (command_tx, command_rx) = mpsc::channel(queue_capacity);

    tokio::spawn(command_actor(
        command_rx,
        WorkerState {
            client: Client::new(&name)?,
            queued_intent: None,
            ds_url: ds_url.clone(),
            relay_url: relay_url.clone(),
        },
    ));

    let app = Router::new()
        .route("/health", get(health))
        .route("/command", post(run_command))
        .with_state(command_tx);

    if debug_logs_enabled() {
        eprintln!(
            "[WORKER {}] starting on http://{} with DS={} RELAY={} queue_capacity={}",
            name, listen_addr, ds_url, relay_url, queue_capacity
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
