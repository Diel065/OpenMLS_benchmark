use std::io::{self, BufRead};

use anyhow::{anyhow, Result};

use mls_playground::client::Client;
use mls_playground::worker_api::{handle_command, Command, CommandResponse, PendingIntent};

fn parse_args() -> Result<(String, String, String)> {
    let mut args = std::env::args().skip(1);

    let mut name: Option<String> = None;
    let mut ds_url: Option<String> = None;
    let mut relay_url: Option<String> = None;

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
            _ => {}
        }
    }

    let name = name.ok_or_else(|| anyhow!("Missing --name"))?;
    let ds_url = ds_url.ok_or_else(|| anyhow!("Missing --ds-url"))?;
    let relay_url = relay_url.ok_or_else(|| anyhow!("Missing --relay-url"))?;

    Ok((name, ds_url, relay_url))
}

fn print_response(response: &CommandResponse) {
    println!("{}", serde_json::to_string(response).unwrap());
}

fn print_response_ok(message: &str) {
    let response = CommandResponse::ok(message);
    print_response(&response);
}

fn print_response_error(message: &str) {
    let response = CommandResponse::error(message);
    print_response(&response);
}

fn main() -> Result<()> {
    let (name, ds_url, relay_url) = parse_args()?;

    let mut client = Client::new(&name)?;
    let mut queued_intent: Option<PendingIntent> = None;

    eprintln!(
        "[CLIENT {}] started, DS={}, RELAY={}",
        name, ds_url, relay_url
    );

    let stdin = io::stdin();
    for line_result in stdin.lock().lines() {
        let line = match line_result {
            Ok(line) => line,
            Err(err) => {
                print_response_error(&format!("stdin read error: {}", err));
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let command: Command = match serde_json::from_str(&line) {
            Ok(cmd) => cmd,
            Err(err) => {
                print_response_error(&format!("invalid command json: {}", err));
                continue;
            }
        };

        match handle_command(&mut client, &ds_url, &relay_url, &mut queued_intent, command) {
            Ok(message) => print_response_ok(&message),
            Err(err) => print_response_error(&err.to_string()),
        }
    }

    Ok(())
}