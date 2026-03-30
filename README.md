# AnvoIO Core Monitor

Block Producer monitoring for Anvo Core and Legacy Antelope/EOSIO chains.

## Overview

Core Monitor tracks block producer performance in real-time using the State History Plugin (SHiP) WebSocket protocol. It uses slot-based round detection derived from Antelope timestamps for deterministic, accurate round tracking — even when producers are offline. It detects missed blocks, missed rounds, schedule changes, fork events, and producer registration actions, persisting all data to PostgreSQL and alerting operators via Telegram and Slack.

### Features

- **Real-time SHiP streaming** — connects directly to nodeos state_history_plugin, no Hyperion dependency for live data
- **Slot-based round detection** — deterministic round boundaries from Antelope timestamps, works correctly when any number of producers are offline
- **WTMSIG support** — parses `producer_schedule_change_extension` from block header extensions for schedule change detection on chains with WTMSIG enabled
- **Schedule lifecycle** — correctly handles proposed → pending → active schedule transitions
- **Multi-chain support** — monitor multiple chains concurrently from a single instance
- **Schedule-relative round numbering** — rounds numbered from schedule activation, reset on schedule changes
- **Missed block detection** — per-producer, per-round tracking with configurable alerting
- **Fork detection** — real-time microfork/fork identification from block ID mismatches
- **Schedule change tracking** — detects new producer schedules, tracks additions/removals
- **Producer registration events** — monitors regproducer/unregprod actions
- **Producer status** — Up/Degraded/Down based on actual block production in the latest round
- **Historical data** — bulk loader for backfilling from full history SHiP nodes via CSV pipeline
- **Dashboard** — web-based UI with producer reliability rankings, round history, event log, timeframe selector, UTC/local toggle, pagination
- **REST API** — JSON endpoints for all monitored data with input validation
- **Alerting** — Telegram and Slack with enable/disable flags and throttling
- **Data retention** — configurable rolling window (default 18 months) with weekly/monthly summaries
- **PostgreSQL** — production-grade persistence with connection pooling
- **Docker deployment** — core-monitor + PostgreSQL via docker-compose
- **CI** — GitHub Actions with PostgreSQL service container

## Requirements

- Node.js 22+
- Docker and Docker Compose (for deployment)
- PostgreSQL 17+ (runs in Docker)
- Access to a nodeos instance with `state_history_plugin` enabled
- Optional: Hyperion v2 endpoint for schedule activation timestamps

## Quick Start

```bash
# Clone the repository
git clone https://github.com/AnvoIO/core-monitor.git
cd core-monitor

# Configure
cp .env.example .env
# Edit .env with your SHiP endpoints, PostgreSQL credentials, and alerting config

# Production (Docker)
docker compose up -d --build
```

The dashboard will be available at `http://localhost:3100` (or your configured `MONITOR_PORT`).

### Development

```bash
npm install
npm run dev
```

Requires a PostgreSQL instance (see `docker-compose.test.yml` for a test instance).

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Description | Default |
|---|---|---|
| `CHAINS` | Comma-separated chain IDs to monitor | required |
| `{CHAIN}_SHIP_URL` | Primary SHiP WebSocket URL | required |
| `{CHAIN}_SHIP_FAILOVER_URL` | Failover SHiP endpoint | optional |
| `{CHAIN}_API_URL` | Chain RPC endpoint | required |
| `{CHAIN}_HYPERION_URL` | Hyperion v2 endpoint | optional |
| `{CHAIN}_CHAIN_ID` | Expected chain ID | required |
| `POSTGRES_URL` | PostgreSQL connection string | required |
| `POSTGRES_DB` | Database name | required |
| `POSTGRES_USER` | Database user | required |
| `POSTGRES_PASSWORD` | Database password | required |
| `TELEGRAM_ENABLED` | Enable Telegram alerts | `false` |
| `TELEGRAM_API_KEY` | Telegram Bot API token | optional |
| `SLACK_ENABLED` | Enable Slack alerts | `false` |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | optional |
| `API_PORT` | Dashboard/API listen port | `3000` |
| `API_CORS_ORIGIN` | CORS allowed origin | `*` |
| `MONITOR_PORT` | Host port mapping for Docker | `3100` |
| `RETENTION_DAYS` | Data retention period | `548` (18 months) |

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard UI |
| `GET /api/v1/health` | Health check |
| `GET /api/v1/chains` | List monitored chains |
| `GET /api/v1/:chain/:network/status` | Current monitor status, schedule activation, earliest data |
| `GET /api/v1/:chain/:network/producers` | Producer reliability stats, signing keys, production status |
| `GET /api/v1/:chain/:network/rounds` | Round history with pagination (`?limit=`, `?offset=`, `?since=`) |
| `GET /api/v1/:chain/:network/events` | Event log with filtering (`?type=`, `?limit=`, `?offset=`, `?since=`, `?until=`) |
| `GET /api/v1/:chain/:network/summaries/weekly` | Weekly summaries |
| `GET /api/v1/:chain/:network/summaries/monthly` | Monthly summaries |

## Dashboard

The web dashboard provides:

- **Timeframe selector** — 1 hour, 1 day, 1 week, 1 month, 1 year, all time
- **Chain/network tabs** — switch between monitored chains
- **Sort options** — performance, schedule order, alphabetical
- **UTC/Local time toggle** — persisted in browser
- **Producer table** — three sections: Scheduled, Standby, Unregistered
- **Status badges** — Up (green), Degraded (yellow), Down (red) based on latest round
- **Round and event pagination** — configurable page size (25/50/100)

## Bulk Loading Historical Data

The bulk loader streams blocks from a SHiP endpoint, writes CSV files, then imports into PostgreSQL. Run on the same machine as the SHiP node for best performance (~10K blocks/sec).

```bash
# Phase 1: Generate CSVs on the SHiP host
npx tsx src/bulk-loader.ts <ship_url> <api_url> <chain> <network> <start_block> [<end_block>]

# Phase 2: Copy CSVs to the monitor host, then import
POSTGRES_URL=postgresql://... npx tsx src/import-csv.ts <chain> <network> <csv_dir>
```

## Testing

Requires a PostgreSQL test instance (use `docker-compose.test.yml`):

```bash
# Start test database
docker compose -f docker-compose.test.yml up -d

# Run all tests
TEST_POSTGRES_URL=postgresql://test:test@localhost:15432/core_monitor_test npm test

# Individual suites
npm run test:unit
npm run test:integration
npm run test:e2e          # Requires network access to SHiP endpoints
```

## CI

GitHub Actions runs on all pull requests targeting `main` or `dev`:
- PostgreSQL 17 service container
- Node.js 22
- Build verification
- 88 tests across 10 test files
- Docker image build

## Architecture

```
SHiP WebSocket ──> ShipClient ──> ChainMonitor ──> RoundEvaluator ──> PostgreSQL
                                       │                                   │
                                       ├──> AlertManager ──> Telegram      └──> API (Fastify)
                                       │                 └──> Slack              └──> Dashboard
                                       └──> Fork Detection
```

**Key design decisions:**
- **Slot-based rounds** — `round = floor(slot / (scheduleSize * blocksPerBp))` where `slot = (timestamp - epoch) / 500ms`
- **Schedule-relative numbering** — display round = global round - activation round
- **Partial round discard** — first round after startup is always discarded (incomplete observation)
- **Pending schedule** — `new_producers` from block headers is stored as pending, only applied when `schedule_version` increments

## License

[MIT License](LICENSE) — Copyright (c) 2026 Stratovera LLC and its contributors.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting pull requests.
