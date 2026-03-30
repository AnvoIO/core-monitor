# AnvoIO Core Monitor

Block Producer monitoring for Anvo Core and Legacy Antelope/EOSIO chains.

## Overview

Core Monitor tracks block producer performance in real-time using the State History Plugin (SHiP) WebSocket protocol. It detects missed blocks, missed rounds, schedule changes, and fork events, persisting all data to SQLite and alerting operators via Telegram and Slack.

### Features

- **Real-time SHiP streaming** — connects directly to nodeos state_history_plugin, no Hyperion dependency
- **Multi-chain support** — monitor multiple chains concurrently from a single instance
- **Round tracking** — accurate round numbering relative to schedule version, bootstrapped from Hyperion
- **Missed block detection** — per-producer, per-round tracking with configurable alerting
- **Fork detection** — real-time microfork/fork identification from block ID mismatches
- **Schedule change tracking** — detects new producer schedules, tracks additions/removals
- **Producer registration events** — monitors regproducer/unregprod actions
- **Historical data** — bulk loader for backfilling from full history SHiP nodes
- **Dashboard** — web-based UI with producer reliability rankings, round history, and event log
- **REST API** — JSON endpoints for all monitored data
- **Alerting** — Telegram and Slack with enable/disable flags and throttling
- **Data retention** — configurable rolling window (default 18 months) with weekly/monthly summaries
- **Docker deployment** — single container with SQLite persistence

## Requirements

- Node.js 22+
- Docker (for deployment)
- Access to a nodeos instance with `state_history_plugin` enabled
- Optional: Hyperion v2 endpoint for accurate round number calculation

## Quick Start

```bash
# Clone the repository
git clone https://github.com/AnvoIO/core-monitor.git
cd core-monitor

# Install dependencies
npm install

# Configure
cp .env.example .env
# Edit .env with your SHiP endpoints and alerting credentials

# Development
npm run dev

# Production (Docker)
docker compose up -d --build
```

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
| `TELEGRAM_ENABLED` | Enable Telegram alerts | `false` |
| `TELEGRAM_API_KEY` | Telegram Bot API token | optional |
| `SLACK_ENABLED` | Enable Slack alerts | `false` |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | optional |
| `API_PORT` | Dashboard/API listen port | `3000` |
| `RETENTION_DAYS` | Data retention period | `548` (18 months) |

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard UI |
| `GET /api/v1/health` | Health check |
| `GET /api/v1/chains` | List monitored chains |
| `GET /api/v1/:chain/:network/status` | Current monitor status |
| `GET /api/v1/:chain/:network/producers` | Producer reliability stats |
| `GET /api/v1/:chain/:network/rounds` | Recent round history |
| `GET /api/v1/:chain/:network/events` | Event log (missed blocks, forks, schedule changes) |
| `GET /api/v1/:chain/:network/summaries/weekly` | Weekly summaries |
| `GET /api/v1/:chain/:network/summaries/monthly` | Monthly summaries |

## Bulk Loading Historical Data

To backfill from a full history SHiP node:

```bash
npx tsx src/bulk-loader.ts <ship_url> <api_url> <chain> <network> <start_block> [<end_block>]
```

## Testing

```bash
npm test              # All tests
npm run test:unit     # Unit tests only
npm run test:integration  # Integration tests
npm run test:e2e      # E2E tests (requires network)
```

## Architecture

```
SHiP WebSocket ──> ShipClient ──> ChainMonitor ──> RoundEvaluator ──> Database (SQLite)
                                       │                                    │
                                       └──> AlertManager ──> Telegram       └──> API (Fastify)
                                                         └──> Slack              └──> Dashboard
```

## License

[MIT License](LICENSE) — Copyright (c) 2026 Stratovera LLC and its contributors.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting pull requests.
