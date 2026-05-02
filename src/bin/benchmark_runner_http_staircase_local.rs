use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;

use mls_playground::local_launcher::{launch_local_stack, LocalLaunchConfig};
use mls_playground::staircase_runner::{run_staircase_benchmark, StaircaseConfig};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 4)]
    spawn_local_workers: usize,

    #[arg(long, default_value_t = false)]
    preflight_only: bool,

    #[arg(long, default_value = "127.0.0.1:3000")]
    ds_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1")]
    worker_host: String,

    #[arg(long, default_value_t = 8081)]
    base_worker_port: u16,

    #[arg(long, default_value_t = 2)]
    min_size: usize,

    #[arg(long)]
    max_size: Option<usize>,

    #[arg(long, default_value_t = 1)]
    step_size: usize,

    #[arg(long, default_value_t = 1)]
    roundtrips: usize,

    /// Base scaling factor before capping: requested updates = update_rounds * N
    #[arg(long, default_value_t = 2)]
    update_rounds: usize,

    /// Hard cap on successful self-update cycles at each plateau
    #[arg(long, default_value_t = 16)]
    max_update_samples_per_plateau: usize,

    /// Base scaling factor before capping: requested sends = app_rounds * N per payload
    #[arg(long, default_value_t = 2)]
    app_rounds: usize,

    /// Hard cap on successful application sends per payload at each plateau
    #[arg(long, default_value_t = 16)]
    max_app_samples_per_payload: usize,

    #[arg(long, value_delimiter = ',', default_value = "32,256,1024,4096")]
    payload_sizes: Vec<usize>,

    #[arg(long, default_value = "run-001")]
    run_id: String,

    #[arg(long, default_value = "http-staircase-local")]
    scenario: String,

    #[arg(long, default_value = "benchmark_output")]
    output_dir: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let deployment = launch_local_stack(&LocalLaunchConfig {
        worker_count: args.spawn_local_workers,
        ds_listen_addr: args.ds_listen_addr,
        worker_host: args.worker_host.clone(),
        base_worker_port: args.base_worker_port,
        run_id: args.run_id.clone(),
        scenario: args.scenario.clone(),
        output_dir: args.output_dir.clone(),
    })?;

    run_staircase_benchmark(StaircaseConfig {
        preflight_only: args.preflight_only,
        ds_url: deployment.ds_url.clone(),
        workers: deployment.workers.clone(),
        min_size: args.min_size,
        max_size: args.max_size,
        step_size: args.step_size,
        roundtrips: args.roundtrips,
        update_rounds: args.update_rounds,
        app_rounds: args.app_rounds,
        max_update_samples_per_plateau: args.max_update_samples_per_plateau,
        max_app_samples_per_payload: args.max_app_samples_per_payload,
        payload_sizes: args.payload_sizes,
        worker_health_timeout_seconds: 300,
        worker_health_poll_ms: 250,
        run_id: args.run_id,
        scenario: args.scenario,
        output_dir: args.output_dir,
    })?;

    drop(deployment);
    Ok(())
}
