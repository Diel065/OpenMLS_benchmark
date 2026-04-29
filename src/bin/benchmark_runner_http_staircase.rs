use std::fs;

use anyhow::{anyhow, Context, Result};

use mls_playground::staircase_runner::{
    parse_worker_specs, run_staircase_benchmark, StaircaseConfig,
};

#[derive(clap::Parser, Debug)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    ds_url: String,

    /// Worker specs in the form ID=URL.
    /// Can be repeated:
    ///   --worker 00001=http://127.0.0.1:8081
    #[arg(long)]
    worker: Vec<String>,

    /// Path to a file containing one worker spec per line in the form ID=URL.
    /// Blank lines and lines starting with '#' are ignored.
    #[arg(long)]
    workers_file: Option<String>,

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

    #[arg(long, default_value = "http-staircase")]
    scenario: String,

    #[arg(long, default_value = "benchmark_output")]
    output_dir: String,
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
    let worker_specs = load_worker_specs(&args)?;
    let workers = parse_worker_specs(&worker_specs)?;

    run_staircase_benchmark(StaircaseConfig {
        ds_url: args.ds_url,
        workers,
        min_size: args.min_size,
        max_size: args.max_size,
        step_size: args.step_size,
        roundtrips: args.roundtrips,
        update_rounds: args.update_rounds,
        app_rounds: args.app_rounds,
        max_update_samples_per_plateau: args.max_update_samples_per_plateau,
        max_app_samples_per_payload: args.max_app_samples_per_payload,
        payload_sizes: args.payload_sizes,
        run_id: args.run_id,
        scenario: args.scenario,
        output_dir: args.output_dir,
    })
}
