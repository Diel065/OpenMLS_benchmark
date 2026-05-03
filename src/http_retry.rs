use std::future::Future;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Error, Result};
use rand::{thread_rng, Rng};
use reqwest::StatusCode;

const MAX_ATTEMPTS: usize = 10;
const INITIAL_DELAY: Duration = Duration::from_millis(100);
const MAX_DELAY: Duration = Duration::from_secs(3);

pub enum RetryDecision<T> {
    Success(T),
    Transient(String),
    Fatal(Error),
}

pub fn retry_transient_http<T, F>(
    op: &str,
    client_id: Option<&str>,
    url: &str,
    mut attempt_fn: F,
) -> Result<T>
where
    F: FnMut() -> RetryDecision<T>,
{
    let started = Instant::now();
    let mut delay = INITIAL_DELAY;

    for attempt in 1..=MAX_ATTEMPTS {
        match attempt_fn() {
            RetryDecision::Success(value) => return Ok(value),
            RetryDecision::Fatal(err) => {
                return Err(retry_error(op, client_id, url, attempt, started, err));
            }
            RetryDecision::Transient(cause) if attempt < MAX_ATTEMPTS => {
                let sleep_for = with_jitter(delay);
                eprintln!(
                    "[retry] op={} client={} attempt={}/{} delay_ms={} url={} error={}",
                    op,
                    client_id.unwrap_or("-"),
                    attempt,
                    MAX_ATTEMPTS,
                    sleep_for.as_millis(),
                    url,
                    cause
                );
                thread::sleep(sleep_for);
                delay = next_delay(delay);
            }
            RetryDecision::Transient(cause) => {
                return Err(anyhow!(
                    "{} failed: client={} url={} attempts={} elapsed_ms={} cause={}",
                    op,
                    client_id.unwrap_or("-"),
                    url,
                    attempt,
                    started.elapsed().as_millis(),
                    cause
                ));
            }
        }
    }

    unreachable!("retry loop always returns")
}

pub async fn retry_transient_http_async<T, F, Fut>(
    op: &str,
    client_id: Option<&str>,
    url: &str,
    mut attempt_fn: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = RetryDecision<T>>,
{
    let started = Instant::now();
    let mut delay = INITIAL_DELAY;

    for attempt in 1..=MAX_ATTEMPTS {
        match attempt_fn().await {
            RetryDecision::Success(value) => return Ok(value),
            RetryDecision::Fatal(err) => {
                return Err(retry_error(op, client_id, url, attempt, started, err));
            }
            RetryDecision::Transient(cause) if attempt < MAX_ATTEMPTS => {
                let sleep_for = with_jitter(delay);
                eprintln!(
                    "[retry] op={} client={} attempt={}/{} delay_ms={} url={} error={}",
                    op,
                    client_id.unwrap_or("-"),
                    attempt,
                    MAX_ATTEMPTS,
                    sleep_for.as_millis(),
                    url,
                    cause
                );
                tokio::time::sleep(sleep_for).await;
                delay = next_delay(delay);
            }
            RetryDecision::Transient(cause) => {
                return Err(anyhow!(
                    "{} failed: client={} url={} attempts={} elapsed_ms={} cause={}",
                    op,
                    client_id.unwrap_or("-"),
                    url,
                    attempt,
                    started.elapsed().as_millis(),
                    cause
                ));
            }
        }
    }

    unreachable!("retry loop always returns")
}

pub fn is_transient_reqwest_error(err: &reqwest::Error) -> bool {
    if err.is_timeout() || err.is_connect() || err.is_body() {
        return true;
    }

    let message = err.to_string().to_ascii_lowercase();
    message.contains("connection refused")
        || message.contains("connection reset")
        || message.contains("connection reset by peer")
        || message.contains("connection closed")
        || message.contains("broken pipe")
        || message.contains("error sending request")
        || message.contains("error trying to connect")
        || message.contains("operation timed out")
        || message.contains("request timed out")
        || message.contains("unexpected eof")
        || message.contains("incomplete message")
        || message.contains("channel closed")
        || message.contains("dns error")
        || message.contains("failed to lookup address information")
        || message.contains("failed to resolve")
        || message.contains("temporary failure in name resolution")
        || message.contains("name or service not known")
        || message.contains("nodename nor servname provided")
}

pub fn is_connect_stage_reqwest_error(err: &reqwest::Error) -> bool {
    if err.is_connect() {
        return true;
    }

    let message = err.to_string().to_ascii_lowercase();
    message.contains("host is unreachable")
        || message.contains("no route to host")
        || message.contains("network is unreachable")
        || message.contains("connection refused")
        || message.contains("tcp connect error")
        || message.contains("connect timed out")
        || message.contains("connection timed out")
        || message.contains("dns error")
        || message.contains("failed to lookup address information")
        || message.contains("failed to resolve")
        || message.contains("temporary failure in name resolution")
        || message.contains("name or service not known")
        || message.contains("nodename nor servname provided")
}

pub fn is_transient_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

fn retry_error(
    op: &str,
    client_id: Option<&str>,
    url: &str,
    attempts: usize,
    started: Instant,
    err: Error,
) -> Error {
    anyhow!(
        "{} failed: client={} url={} attempts={} elapsed_ms={} cause={:#}",
        op,
        client_id.unwrap_or("-"),
        url,
        attempts,
        started.elapsed().as_millis(),
        err
    )
}

fn next_delay(delay: Duration) -> Duration {
    let doubled_ms = delay.as_millis().saturating_mul(2);
    let max_ms = MAX_DELAY.as_millis();
    Duration::from_millis(doubled_ms.min(max_ms) as u64)
}

fn with_jitter(delay: Duration) -> Duration {
    let base_ms = delay.as_millis() as u64;
    let jitter_cap_ms = (base_ms / 10).clamp(1, 100);
    let jitter_ms = thread_rng().gen_range(0..=jitter_cap_ms);
    Duration::from_millis(base_ms + jitter_ms)
}
