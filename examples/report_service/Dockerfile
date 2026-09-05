# ==========================================
# 1. Build Stage
# ==========================================
FROM ghcr.io/gleam-lang/gleam:v1.18.1-erlang-alpine AS builder

WORKDIR /build

# Install build dependencies required for compiling Erlang/C dependencies
RUN apk add --no-cache git build-base rebar3

# Download dependencies first for optimal Docker layer caching
COPY gleam.toml manifest.toml ./
RUN gleam deps download

# Copy application source code
COPY src src

# Export production Erlang shipment bundle
RUN gleam export erlang-shipment

# ==========================================
# 2. Production Runner Stage
# ==========================================
FROM ghcr.io/gleam-lang/gleam:v1.18.1-erlang-alpine AS runner

WORKDIR /app

# Install runtime utilities & create non-root user
RUN apk add --no-cache dumb-init ca-certificates ncurses-libs libstdc++ openssl \
    && addgroup -g 10001 -S gleam \
    && adduser -u 10001 -S -G gleam -h /app gleam

# Copy shipment artifacts from builder
COPY --from=builder --chown=gleam:gleam /build/build/erlang-shipment /app

# Switch to non-root user
USER gleam

# Fine-tuning default environment variables
ENV DATABASE_URL="" \
    HOST="0.0.0.0" \
    PORT="8080" \
    INSTANCE_ID="reports-app" \
    DB_POOL_SIZE="20" \
    WORKER_CONCURRENCY="8" \
    WORKER_PREFETCH="2" \
    HTTP_POOL_WORKERS="8" \
    HTTP_POOL_PREFETCH="1" \
    HTTP_BUFFER_CAPACITY="32" \
    SIMULATED_WORK_MS="0"

EXPOSE 8080

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/bin/sh", "/app/entrypoint.sh", "run"]
