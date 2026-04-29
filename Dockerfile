# syntax=docker/dockerfile:1.7

# ---------- builder ----------
FROM rust:bookworm AS builder

WORKDIR /app

RUN rustup target add x86_64-unknown-linux-musl \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        musl-tools \
        clang \
        lld \
        cmake \
        make \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build \
    --release \
    --target x86_64-unknown-linux-musl \
    --bin ds \
    --bin message_relay \
    --bin worker \
    --bin benchmark_runner_http_staircase

RUN mkdir -p /out \
    && cp /app/target/x86_64-unknown-linux-musl/release/ds /out/ds \
    && cp /app/target/x86_64-unknown-linux-musl/release/message_relay /out/message_relay \
    && cp /app/target/x86_64-unknown-linux-musl/release/worker /out/worker \
    && cp /app/target/x86_64-unknown-linux-musl/release/benchmark_runner_http_staircase /out/benchmark_runner_http_staircase

# ---------- ds runtime ----------
FROM scratch AS ds-runtime
COPY --from=builder /out/ds /ds
ENTRYPOINT ["/ds"]

# ---------- relay runtime ----------
FROM scratch AS relay-runtime
COPY --from=builder /out/message_relay /message_relay
ENTRYPOINT ["/message_relay"]

# ---------- worker runtime ----------
FROM scratch AS worker-runtime
COPY --from=builder /out/worker /worker
ENTRYPOINT ["/worker"]

# ---------- runner runtime ----------
FROM scratch AS runner-runtime
COPY --from=builder /out/benchmark_runner_http_staircase /benchmark_runner_http_staircase
ENTRYPOINT ["/benchmark_runner_http_staircase"]