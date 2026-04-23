# syntax=docker/dockerfile:1.7

FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p ggap-node

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/ginnungagap --shell /usr/sbin/nologin ggap \
 && mkdir -p /var/lib/ginnungagap /etc/ginnungagap \
 && chown -R ggap:ggap /var/lib/ginnungagap /etc/ginnungagap

COPY --from=builder /app/target/release/ggap-node /usr/local/bin/ggap-node
COPY config/ /etc/ginnungagap/

USER ggap
WORKDIR /var/lib/ginnungagap
EXPOSE 17000 17001 9090
ENTRYPOINT ["/usr/local/bin/ggap-node"]
