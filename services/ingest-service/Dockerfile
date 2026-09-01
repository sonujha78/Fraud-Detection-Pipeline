FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /ingest-service ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /ingest-service /ingest-service

ENV GRPC_PORT=50051
ENV KAFKA_BOOTSTRAP_SERVERS=kafka.kafka.svc.cluster.local:9092

EXPOSE 50051
EXPOSE 9100
ENTRYPOINT ["/ingest-service"]
