use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use signal_benchmark::staircase_runner::{
    aggregate_csv, parse_worker_layout, run_dir_for, validate_run_id,
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    run_dir: Option<String>,

    #[arg(long)]
    run_id: Option<String>,

    #[arg(long, default_value = "benchmark_output")]
    output_dir: String,

    #[arg(long)]
    layout_file: Option<String>,

    #[arg(long)]
    workers_file: Option<String>,
}

fn main() -> Result<()> {
    let args = <Args as Parser>::parse();

    let run_dir: PathBuf = if let Some(dir) = &args.run_dir {
        PathBuf::from(dir)
    } else {
        let run_id = args
            .run_id
            .ok_or_else(|| anyhow::anyhow!("Either --run-dir or --run-id must be provided"))?;
        validate_run_id(&run_id)?;
        run_dir_for(&args.output_dir, &run_id)
    };

    if !run_dir.exists() {
        return Err(anyhow::anyhow!(
            "Run directory does not exist: {}",
            run_dir.display()
        ));
    }

    let layout = if let Some(layout_path) = &args.layout_file {
        Some(parse_worker_layout(&PathBuf::from(layout_path))?)
    } else {
        let default_path = run_dir.join("worker_layout.json");
        if default_path.exists() {
            Some(parse_worker_layout(&default_path)?)
        } else {
            None
        }
    };

    let worker_ids: Vec<String> = if let Some(workers_file) = &args.workers_file {
        let content = std::fs::read_to_string(workers_file)
            .with_context(|| format!("Failed to read workers file '{}'", workers_file))?;
        let mut ids = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((id, _)) = line.split_once('=') {
                ids.push(id.to_string());
            }
        }
        ids
    } else {
        let mut ids: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&run_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(participant_id) = name_str
                .strip_prefix("participant-")
                .and_then(|s| s.strip_suffix(".jsonl"))
            {
                ids.push(participant_id.to_string());
            }
        }
        ids.sort();
        ids
    };

    if worker_ids.is_empty() {
        return Err(anyhow::anyhow!(
            "No workers found in {}",
            if args.workers_file.is_some() {
                format!("workers file")
            } else {
                format!("run directory {}", run_dir.display())
            }
        ));
    }

    aggregate_csv(&run_dir, &worker_ids, &layout)?;

    println!(
        "Aggregation complete: {}",
        run_dir.join("events.csv").display()
    );

    Ok(())
}
