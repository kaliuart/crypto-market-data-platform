# Crypto Market Data Platform

A real-time market data pipeline that collects aggregated BTC/USDT trades
from Binance, publishes them to Kafka, and stores them in ClickHouse.

## Architecture

```text
Binance WebSocket
        |
        v
Python collector
        |
        v
      Kafka
        |
        v
Storage writer
        |
        v
   ClickHouse
```

The collector:

- receives Binance `aggTrade` messages;
- validates and transforms them;
- detects duplicate, out-of-order, and missing trade IDs;
- publishes trades to Kafka;
- exposes Prometheus metrics.

The storage writer:

- consumes trades from Kafka;
- validates Kafka messages;
- maps them to ClickHouse rows;
- inserts messages in batches;
- commits Kafka offsets after a successful insert.

## Technology stack

- Python 3.11+
- Binance WebSocket API
- Apache Kafka
- ClickHouse
- Prometheus
- Grafana
- Docker Compose
- uv
- pytest

## Requirements

- Docker and Docker Compose
- Python 3.11 or newer
- uv

## Installation

Install the Python dependencies:

```powershell
uv sync
```

Run the tests:

```powershell
uv run pytest -q
```

## Running the project

Set the ClickHouse password:

```powershell
$env:CLICKHOUSE_PASSWORD = "your-password"
```

Start the infrastructure:

```powershell
docker compose up -d
```

Start the market data collector:

```powershell
uv run python main.py
```

In another PowerShell terminal, set the password again:

```powershell
$env:CLICKHOUSE_PASSWORD = "your-password"
```

Then start the storage writer:

```powershell
uv run python storage_writer.py
```

## Local services

| Service | Address |
|---|---|
| Kafka UI | `http://localhost:8080` |
| Collector metrics | `http://localhost:8000` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| ClickHouse HTTP | `http://localhost:8123` |

## Kafka

Topic:

```text
binance.aggregated-trades.v1
```

The Kafka producer uses idempotent publishing.

The storage writer disables automatic offset commits and commits offsets
only after a successful ClickHouse insert.

## Current limitations

- one Binance symbol;
- one Kafka broker;
- applications run outside Docker;
- no dead-letter queue;
- no Binance backfill;
- no formal ClickHouse deduplication strategy;
- no storage-writer metrics;


## Tests

Run the complete test suite:

```powershell
uv run pytest -q
```