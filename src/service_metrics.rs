use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::Instant,
};

use serde::Serialize;

const DEFAULT_WARN_IN_FLIGHT: u64 = 128;
const DEFAULT_LATENCY_SAMPLE_LIMIT: usize = 4096;

#[derive(Debug)]
pub struct ServiceMetrics {
    endpoints: Mutex<HashMap<String, EndpointMetrics>>,
    warn_in_flight: u64,
    latency_sample_limit: usize,
}

#[derive(Debug, Default)]
struct EndpointMetrics {
    request_count: u64,
    error_count: u64,
    in_flight: u64,
    max_in_flight: u64,
    latencies_ms: VecDeque<u128>,
}

#[derive(Debug, Serialize)]
pub struct ServiceMetricsSnapshot {
    pub endpoints: HashMap<String, EndpointMetricsSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct EndpointMetricsSnapshot {
    pub request_count: u64,
    pub error_count: u64,
    pub in_flight: u64,
    pub max_in_flight: u64,
    pub latency_sample_count: usize,
    pub latency_p50_ms: Option<u128>,
    pub latency_p95_ms: Option<u128>,
    pub latency_p99_ms: Option<u128>,
    pub latency_max_ms: Option<u128>,
}

pub struct EndpointMetricsGuard<'a> {
    metrics: &'a ServiceMetrics,
    endpoint: &'static str,
    started: Instant,
}

impl ServiceMetrics {
    pub fn new() -> Self {
        let warn_in_flight = std::env::var("OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_WARN_IN_FLIGHT);

        Self {
            endpoints: Mutex::new(HashMap::new()),
            warn_in_flight,
            latency_sample_limit: DEFAULT_LATENCY_SAMPLE_LIMIT,
        }
    }

    pub fn start(&self, endpoint: &'static str) -> EndpointMetricsGuard<'_> {
        let in_flight = {
            let mut endpoints = self.endpoints.lock().unwrap();
            let metrics = endpoints.entry(endpoint.to_string()).or_default();
            metrics.request_count += 1;
            metrics.in_flight += 1;
            metrics.max_in_flight = metrics.max_in_flight.max(metrics.in_flight);
            metrics.in_flight
        };

        if in_flight > self.warn_in_flight {
            eprintln!(
                "[service-metrics] endpoint={} in_flight={} warn_threshold={}",
                endpoint, in_flight, self.warn_in_flight
            );
        }

        EndpointMetricsGuard {
            metrics: self,
            endpoint,
            started: Instant::now(),
        }
    }

    fn finish(&self, endpoint: &'static str, latency_ms: u128, error: bool) {
        let mut endpoints = self.endpoints.lock().unwrap();
        let metrics = endpoints.entry(endpoint.to_string()).or_default();

        if metrics.in_flight > 0 {
            metrics.in_flight -= 1;
        }
        if error {
            metrics.error_count += 1;
        }

        metrics.latencies_ms.push_back(latency_ms);
        while metrics.latencies_ms.len() > self.latency_sample_limit {
            metrics.latencies_ms.pop_front();
        }
    }

    pub fn snapshot(&self) -> ServiceMetricsSnapshot {
        let endpoints = self.endpoints.lock().unwrap();
        let endpoints = endpoints
            .iter()
            .map(|(endpoint, metrics)| {
                let (p50, p95, p99, max) =
                    latency_percentiles(metrics.latencies_ms.iter().copied().collect());
                (
                    endpoint.clone(),
                    EndpointMetricsSnapshot {
                        request_count: metrics.request_count,
                        error_count: metrics.error_count,
                        in_flight: metrics.in_flight,
                        max_in_flight: metrics.max_in_flight,
                        latency_sample_count: metrics.latencies_ms.len(),
                        latency_p50_ms: p50,
                        latency_p95_ms: p95,
                        latency_p99_ms: p99,
                        latency_max_ms: max,
                    },
                )
            })
            .collect();

        ServiceMetricsSnapshot { endpoints }
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl EndpointMetricsGuard<'_> {
    pub fn finish(self, error: bool) {
        self.metrics
            .finish(self.endpoint, self.started.elapsed().as_millis(), error);
    }
}

fn latency_percentiles(
    mut latencies: Vec<u128>,
) -> (Option<u128>, Option<u128>, Option<u128>, Option<u128>) {
    if latencies.is_empty() {
        return (None, None, None, None);
    }

    latencies.sort_unstable();
    let max = latencies.last().copied();

    let percentile = |pct: usize| -> Option<u128> {
        let len = latencies.len();
        let idx = ((len.saturating_sub(1)) * pct).div_ceil(100);
        latencies.get(idx).copied()
    };

    (percentile(50), percentile(95), percentile(99), max)
}
