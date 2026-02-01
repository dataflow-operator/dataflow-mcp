# Build stage
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy manifests and source
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/dataflow-mcp /usr/local/bin/dataflow-mcp

ENTRYPOINT ["/usr/local/bin/dataflow-mcp"]
