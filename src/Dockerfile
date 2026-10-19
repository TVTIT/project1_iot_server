# Build stage
FROM golang:1.27.2-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum for layer caching
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/mosquitto-auth-init ./cmd/mosquitto-auth-init && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/mosquitto-reloader ./cmd/mosquitto-reloader && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/mosquitto-credential-controller ./cmd/mosquitto-credential-controller && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/gateway ./cmd/gateway

# Final stage
FROM eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9 AS runtime
USER 1883:1883
WORKDIR /app

FROM runtime AS auth-init
COPY --from=builder /app/mosquitto-auth-init /app/mosquitto-auth-init
ENTRYPOINT ["/app/mosquitto-auth-init"]

FROM runtime AS reloader
COPY --from=builder /app/mosquitto-reloader /app/mosquitto-reloader
ENTRYPOINT ["/app/mosquitto-reloader"]

FROM runtime AS credential-controller
COPY --from=builder /app/mosquitto-credential-controller /app/mosquitto-credential-controller
ENTRYPOINT ["/app/mosquitto-credential-controller"]

# Gateway simulator: synthetic telemetry producer with a durable SQLite outbox.
# The binary is built with CGO disabled (the pure-Go SQLite driver needs no libc)
# so it runs unchanged on the same pinned runtime base as the other Go binaries.
# Only a writable outbox directory and the public CA are mounted at runtime;
# no credentials, private keys or populated databases are baked into the image.
# This target is opt-in; it is intentionally not a default root Compose service.
FROM runtime AS gateway
USER root
RUN mkdir -p /data/outbox /data/ca && chown -R 1883:1883 /data
USER 1883:1883
WORKDIR /app
COPY --from=builder /app/gateway /app/gateway
VOLUME ["/data/outbox"]
ENTRYPOINT ["/app/gateway"]

# Keep backend last: existing docker build/smoke harnesses use the default target.
FROM runtime AS backend

WORKDIR /app
COPY --from=builder /app/server /app/server

EXPOSE 8080

ENTRYPOINT ["/app/server"]
