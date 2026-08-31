FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /query-service ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /query-service /query-service

ENV GRPC_PORT=50052
ENV CASSANDRA_HOST=cassandra.cassandra.svc.cluster.local
ENV CASSANDRA_KEYSPACE=fraud_detection

EXPOSE 50052
ENTRYPOINT ["/query-service"]
