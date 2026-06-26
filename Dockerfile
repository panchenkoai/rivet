# Stage 0: chef — the cargo-chef toolchain, shared by the planner and builder.
FROM rust:1.94-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /src

# Stage 1: planner — distil the dependency graph into recipe.json. Cheap (no
# compilation); recipe.json changes only when the dependency set changes, so the
# expensive "cook" layer below stays cached across source-only edits.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: builder — cook the dependencies FIRST as their own Docker layer keyed
# on recipe.json. A source-only change then recompiles just rivet, not the ~500
# dependency crates (arrow / parquet / tokio …) — that layer is reused.
FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked

# Stage 3: runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 rivet \
    && useradd --uid 10001 --gid rivet --no-create-home rivet

COPY --from=builder /src/target/release/rivet /usr/local/bin/rivet
COPY --from=builder /src/target/release/rivet-mcp /usr/local/bin/rivet-mcp

USER rivet
ENTRYPOINT ["rivet"]
