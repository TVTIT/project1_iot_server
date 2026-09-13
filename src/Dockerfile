# Build stage
FROM golang:1.27.1-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum for layer caching
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server ./cmd/server

# Final stage
FROM alpine:3.19

WORKDIR /app
COPY --from=builder /app/server /app/server

EXPOSE 8080

CMD ["/app/server"]
