# Build stage
FROM golang:1.27.1-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum for layer caching
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/mosquitto-auth-init ./cmd/mosquitto-auth-init && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/mosquitto-reloader ./cmd/mosquitto-reloader

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

# Keep backend last: existing docker build/smoke harnesses use the default target.
FROM runtime AS backend

WORKDIR /app
COPY --from=builder /app/server /app/server

EXPOSE 8080

ENTRYPOINT ["/app/server"]
