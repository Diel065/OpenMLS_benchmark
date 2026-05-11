use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use clap::ArgAction;

use signal_benchmark::staircase_runner::{
    parse_worker_layout, parse_worker_specs, run_staircase_benchmark, workers_from_layout,
    StaircaseConfig,
};

#[derive(clap::Parser, Debug)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    kr_url: String,

    #[arg(long, default_value = "http://127.0.0.1:4000")]
    relay_url: String,

    /// Worker specs in the form ID=URL.
    /// Can be repeated:
    ///   --worker 00001=http://127.0.0.1:8081
    #[arg(long)]
    worker: Vec<String>,

    /// Path to a file containing one worker spec per line in the form ID=URL.
    /// Blank lines and lines starting with '#' are ignored.
    #[arg(long)]
    workers_file: Option<String>,

    /// Path to worker_layout.json for hybrid layout mode.
    #[arg(long)]
    worker_layout: Option<String>,

    #[arg(long, default_value_t = false)]
    preflight_only: bool,

    #[arg(long, default_value_t = 2)]
    min_size: usize,

    #[arg(long)]
    max_size: Option<usize>,

    #[arg(long, default_value_t = 1)]
    step_size: usize,

    #[arg(long, default_value_t = 1)]
    roundtrips: usize,

    /// App-level send rounds per participant per plateau
    #[arg(long, default_value_t = 1)]
    app_rounds: usize,

    /// Hard cap on successful application sends per payload at each plateau
    #[arg(long, default_value_t = 16)]
    max_app_samples_per_payload: usize,

    #[arg(long, value_delimiter = ',', default_value = "32,256,1024,4096")]
    payload_sizes: Vec<usize>,

    #[arg(long, default_value = "run-001")]
    run_id: String,

    #[arg(long, default_value = "http-staircase")]
    scenario: String,

    #[arg(long, default_value = "benchmark_output")]
    output_dir: String,

    #[arg(long, default_value_t = 300)]
    worker_health_timeout_seconds: u64,

    #[arg(long, default_value_t = 250)]
    worker_health_poll_ms: u64,

    #[arg(long, default_value_t = 0)]
    max_fanout_parallelism: usize,

    #[arg(long, default_value_t = 0)]
    min_fanout_parallelism: usize,

    #[arg(long, action = ArgAction::SetTrue)]
    fanout_adaptive: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    no_fanout_adaptive: bool,

    #[arg(long, default_value_t = 0.0)]
    fanout_error_rate_threshold: f64,

    #[arg(long, default_value_t = 0)]
    fanout_p95_threshold_ms: u128,

    #[arg(long, default_value_t = 0)]
    http_pool_max_idle_per_host: usize,

    #[arg(long, action = ArgAction::SetTrue)]
    profile_only_singletons: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    no_aggregate: bool,
}

fn load_worker_specs(args: &Args) -> Result<Vec<String>> {
    let mut specs = Vec::new();

    if let Some(path) = &args.workers_file {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read workers file '{}'", path))?;

        for (idx, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if !line.contains('=') {
                return Err(anyhow!(
                    "Invalid worker spec on line {} of '{}': expected ID=URL, got '{}'",
                    idx + 1,
                    path,
                    line
                ));
            }

            specs.push(line.to_string());
        }
    }

    specs.extend(args.worker.iter().cloned());

    if specs.is_empty() {
        return Err(anyhow!(
            "No workers provided. Use --worker ID=URL and/or --workers-file PATH"
        ));
    }

    Ok(specs)
}

fn main() -> Result<()> {
    let args = <Args as clap::Parser>::parse();
    if args.fanout_adaptive && args.no_fanout_adaptive {
        return Err(anyhow!(
            "--fanout-adaptive and --no-fanout-adaptive cannot both be set"
        ));
    }

    let fanout_adaptive = if args.no_fanout_adaptive {
        Some(false)
    } else if args.fanout_adaptive {
        Some(true)
    } else {
        None
    };

    let worker_layout = if let Some(layout_path) = &args.worker_layout {
        let layout = parse_worker_layout(Path::new(layout_path))?;
        Some(layout)
    } else {
        None
    };

    let workers = if let Some(ref layout) = worker_layout {
        let w = workers_from_layout(layout);
        eprintln!(
            "[layout] loaded {} logical workers from layout ({} physical, mode={})",
            layout.logical_worker_count, layout.physical_worker_count, layout.layout_mode
        );
        w
    } else {
        let worker_specs = load_worker_specs(&args)?;
        parse_worker_specs(&worker_specs)?
    };

    run_staircase_benchmark(StaircaseConfig {
        preflight_only: args.preflight_only,
        kr_url: args.kr_url,
        relay_url: args.relay_url,
        workers,
        min_size: args.min_size,
        max_size: args.max_size,
        step_size: args.step_size,
        roundtrips: args.roundtrips,
        app_rounds: args.app_rounds,
        max_app_samples_per_payload: args.max_app_samples_per_payload,
        payload_sizes: args.payload_sizes,
        run_id: args.run_id,
        scenario: args.scenario,
        output_dir: args.output_dir,
        worker_health_timeout_seconds: args.worker_health_timeout_seconds,
        worker_health_poll_ms: args.worker_health_poll_ms,
        max_fanout_parallelism: args.max_fanout_parallelism,
        min_fanout_parallelism: args.min_fanout_parallelism,
        fanout_adaptive,
        fanout_error_rate_threshold: args.fanout_error_rate_threshold,
        fanout_p95_threshold_ms: args.fanout_p95_threshold_ms,
        http_pool_max_idle_per_host: args.http_pool_max_idle_per_host,
        profile_only_singletons: args.profile_only_singletons,
        no_aggregate: args.no_aggregate,
        worker_layout,
    })
}
