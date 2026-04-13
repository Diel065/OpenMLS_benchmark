# ---------- builder ----------
FROM rust:bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release --bin ds --bin message_relay --bin worker --bin benchmark_runner_http_staircase

# ---------- common runtime base ----------
FROM debian:bookworm-slim AS runtime-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------- ds runtime ----------
FROM runtime-base AS ds-runtime
COPY --from=builder /app/target/release/ds /usr/local/bin/ds
CMD ["ds"]

# ---------- relay runtime ----------
FROM runtime-base AS relay-runtime
COPY --from=builder /app/target/release/message_relay /usr/local/bin/message_relay
CMD ["message_relay"]

# ---------- shared app runtime ----------
FROM runtime-base AS app-runtime
COPY --from=builder /app/target/release/worker /usr/local/bin/worker
COPY --from=builder /app/target/release/benchmark_runner_http_staircase /usr/local/bin/benchmark_runner_http_staircase
COPY docker/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
CMD ["worker"]