# The Orbit sync engine (`orbit-server`): VStream subscriber, distributor and fill worker in one
# process. Built for Railway (see railway.json and docs/deployment-railway.md), but it is a plain
# image: any host that gives it a persistent volume and one replica works.
#
# The engine fetches the compiled sync schema from the Worker at start (`GET /internal/schema`),
# so the image carries no artifact. Mount one and set SYNC_SCHEMA_PATH to run against a local copy.

FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates ./crates
RUN cargo build --release -p orbit-server --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
  && useradd --system --uid 10001 --create-home orbit
COPY --from=build /src/target/release/orbit-server /usr/local/bin/orbit-server
ENV STATE_PATH=/data/orbit-state.sqlite \
    RUST_LOG=info
# The checkpoint state must survive restarts: mount a volume at /data. There is deliberately no
# VOLUME instruction: Railway's builder fails the build on it (without a log line), and every
# host attaches its volume at run time anyway.
RUN mkdir -p /data && chown orbit:orbit /data
USER orbit
# Prometheus metrics; Railway's health check hits /metrics on $PORT.
EXPOSE 9464
CMD ["sh", "-c", "exec orbit-server run --metrics-addr 0.0.0.0:${PORT:-9464}"]
