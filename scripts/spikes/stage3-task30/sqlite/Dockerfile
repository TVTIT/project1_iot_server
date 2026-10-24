# Reruns use the patched toolchain; historical spike results retain their original version.
FROM golang:1.27.3-alpine AS builder
WORKDIR /spike
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /sqlite-spike .

FROM eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9
USER 1883:1883
COPY --from=builder /sqlite-spike /sqlite-spike
ENTRYPOINT ["/sqlite-spike"]
