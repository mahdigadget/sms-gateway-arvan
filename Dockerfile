FROM golang:1.26-bookworm AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=1
RUN go build -o /out/api ./cmd/api \
    && go build -o /out/report ./cmd/report

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/api /out/report ./
COPY .env ./.env

EXPOSE 9095 9096
CMD ["/app/api"]
