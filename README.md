# Anvo Core Monitor

Block Producer monitoring for Anvo Core and Legacy Antelope/EOSIO chains.

## Overview

Core Monitor tracks block producer performance in real-time using the State History Plugin (SHiP) WebSocket protocol. It uses slot-based round detection derived from Antelope timestamps for deterministic, accurate round tracking — even when producers are offline. It detects missed blocks, missed rounds, schedule changes, fork events, and producer registration actions, persisting all data to PostgreSQL and alerting operators via Telegram and Slack.

### Features

- **Real-time SHiP streaming** — connects directly to nodeos state_history_plugin, no Hyperion dependency for live data
- **Slot-based round detection** — deterministic round boundaries from Antelope timestamps, works correctly when any number of producers are offline
- **WTMSIG support** — parses `producer_schedule_change_extension` from block header extensions for schedule change detection on chains with WTMSIG enabled
- **Schedule lifecycle** — correctly handles proposed → pending → active schedule transitions, discards partial rounds at boundaries
- **Multi-chain support** — monitor multiple chains concurrently from a single deployment
- **Schedule-relative round numbering** — rounds numbered from schedule activation, reset on schedule changes
- **Missed block detection** — per-producer, per-round tracking with configurable alerting
- **Fork detection** — real-time microfork/fork identification from block ID mismatches
- **Schedule change tracking** — detects new producer schedules, tracks additions/removals/key updates, shows pending schedules
- **Producer events** — monitors regproducer, unregprod, and kickbp actions
- **Producer status** — Up/Degraded/Down based on actual block production in the latest round
- **Historical backfill** — catchup writer streams from full-history SHiP nodes directly to PostgreSQL, automatically detects live writer boundary, resumes from last processed block on restart
- **Summary tables** — daily aggregated stats (producer_stats_daily, outage_events, round_counts_daily) eliminate full-table scans on "All Time" queries, maintained incrementally by the live writer with hourly reconciliation
- **Outage tracking** — per-producer outage history with streak detection, duration, and timeline
- **Dashboard** — web-based UI with producer reliability rankings, round history, event log, timeframe selector, UTC/local toggle, pagination
- **REST API** — JSON endpoints for all monitored data with input validation
- **Alerting** — Telegram and Slack with enable/disable flags and throttling
- **Data retention** — configurable rolling window (default 18 months) with weekly/monthly summaries
- **PostgreSQL** — production-grade persistence with connection pooling and optimized indexes
- **Docker deployment** — reader/writer/postgres via docker-compose
- **CI** — GitHub Actions with PostgreSQL service container

## Architecture

The monitor runs as three separate containers:

```
┌──────────────────────────────────────────────────────────────┐
│  Writer                                                       │
│  SHiP WebSocket ──> ShipClient ──> ChainMonitor              │
│                          │              │                      │
│                          │              ├──> RoundEvaluator ──┤
│                          │              ├──> Fork Detection    │──> PostgreSQL
│                          │              └──> Producer Events   │
│                          │                                     │
│                          └──> AlertManager ──> Telegram/Slack  │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  Reader                                                       │
│  API (Fastify) ──> PostgreSQL (read-only)                     │
│       └──> Dashboard (static HTML/JS)                         │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  Catchup Writer (optional)                                    │
│  Full-history SHiP ──> RoundEvaluator ──> PostgreSQL          │
│  Streams from genesis, stops at live writer's start point     │
└──────────────────────────────────────────────────────────────┘
```

- **Writer** — streams live blocks from SHiP, evaluates rounds, writes to PostgreSQL, sends alerts
- **Reader** — serves the dashboard and REST API, reads from PostgreSQL
- **Catchup Writer** — optional service for backfilling historical data from a full-history SHiP node

This separation ensures API responsiveness is never affected by block processing load.

## Requirements

- Docker and Docker Compose
- PostgreSQL 17+ (runs in Docker)
- Access to a nodeos instance with `state_history_plugin` enabled
- Optional: Hyperion v2 endpoint for schedule activation timestamps
- Optional: Full-history SHiP node for catchup backfill

## Quick Start

```bash
# Clone the repository
git clone https://github.com/AnvoIO/core-monitor.git
cd core-monitor

# Configure
cp .env.example .env
# Edit .env with your SHiP endpoints, PostgreSQL credentials, and alerting config

# Start (postgres + writer + reader)
docker compose up -d --build
```

The dashboard will be available at `http://localhost:3100` (or your configured `MONITOR_PORT`).

### Historical Backfill

To backfill from a full-history SHiP node while the live monitor continues running:

```bash
# Add to .env:
CATCHUP_SHIP_URL=ws://your-history-node:8088

# Start the catchup writer alongside the running services
docker compose --profile catchup up -d catchup
```

The catchup writer automatically detects where the live writer started and stops there — no overlap, no manual coordination. On restart, it resumes from the last processed block rather than starting over.

### Development

```bash
npm install
npm run dev
```

Requires a PostgreSQL instance (see `docker-compose.test.yml` for a test instance).

## Configuration

All configuration is via environment variables (see `.env.example`):

### Chain Configuration

| Variable | Description | Default |
|---|---|---|
| `CHAINS` | Comma-separated chain IDs to monitor | required |
| `{CHAIN}_SHIP_URL` | Primary SHiP WebSocket URL | required |
| `{CHAIN}_SHIP_FAILOVER_URL` | Failover SHiP endpoint | optional |
| `{CHAIN}_API_URL` | Chain RPC endpoint | required |
| `{CHAIN}_HYPERION_URL` | Hyperion v2 endpoint | optional |
| `{CHAIN}_CHAIN_ID` | Expected chain ID | required |

### Database

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_URL` | PostgreSQL connection string | required |
| `POSTGRES_DB` | Database name | required |
| `POSTGRES_USER` | Database user | required |
| `POSTGRES_PASSWORD` | Database password | required |

### Alerting (Writer)

| Variable | Description | Default |
|---|---|---|
| `TELEGRAM_ENABLED` | Enable Telegram alerts | `false` |
| `TELEGRAM_API_KEY` | Telegram Bot API token | optional |
| `SLACK_ENABLED` | Enable Slack alerts | `false` |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | optional |

### API (Reader)

| Variable | Description | Default |
|---|---|---|
| `API_PORT` | Dashboard/API listen port | `3000` |
| `API_CORS_ORIGIN` | CORS allowed origin | `*` |
| `MONITOR_PORT` | Host port mapping for Docker | `3100` |

### Catchup Writer

| Variable | Description | Default |
|---|---|---|
| `CATCHUP_SHIP_URL` | SHiP endpoint for full-history node | optional |
| `{CHAIN}_CATCHUP_SHIP_URL` | Per-chain override | optional |
| `CATCHUP_START_BLOCK` | Start block for backfill | `1` |

## Alerting

The writer sends alerts to configured Telegram and/or Slack channels. Each chain can have separate status and alert chat IDs for Telegram (`TELEGRAM_{CHAIN}_STATUS_CHAT_ID`, `TELEGRAM_{CHAIN}_ALERT_CHAT_ID`).

### Alert Types and Routing

```
┌───────────────────────┬────────┬────────┬───────┐
│         Alert         │ Header │ Status │ Alert │
├───────────────────────┼────────┼────────┼───────┤
│ Missed Round          │ 🚨     │        │ ✓     │
├───────────────────────┼────────┼────────┼───────┤
│ Missed Blocks         │ 🚨     │        │ ✓     │
├───────────────────────┼────────┼────────┼───────┤
│ Degraded Round        │ 🚨     │ ✓      │       │
├───────────────────────┼────────┼────────┼───────┤
│ Schedule Active       │ ✅     │ ✓      │       │
├───────────────────────┼────────┼────────┼───────┤
│ Producer Registered   │ ✅     │ ✓      │       │
├───────────────────────┼────────┼────────┼───────┤
│ Producer Kicked       │ ⚠️     │ ✓      │       │
├───────────────────────┼────────┼────────┼───────┤
│ Producer Unregistered │ ✅     │ ✓      │       │
├───────────────────────┼────────┼────────┼───────┤
│ Status Update (6hr)   │ ✅     │ ✓      │ ✓     │
└───────────────────────┴────────┴────────┴───────┘
```

- **Missed Round** — Producer produced 0 blocks in a round
- **Missed Blocks** — Producer produced some but not all blocks
- **Degraded Round** — Round summary with all issues (missed rounds, missed blocks, forks)
- **Schedule Active** — New schedule version activated (shows ✚ added, − removed, 🔑 key updates)
- **Producer Registered/Kicked/Unregistered** — `regproducer`, `kickbp`, `unregprod` actions
- **Status Update** — Periodic 6-hour summary: rounds evaluated, reliability %, round span

**Status channel** — operational updates, round recaps, schedule changes. Low-noise overview of chain health.

**Alert channel** — actionable alerts requiring attention. Individual missed round/block events for each affected producer.

### Message Format

All messages follow a consistent format with indented body lines:

```
🚨 Degraded Round [ Schedule 486 / Round 14 ]
 ❌ Missed Round: badprod produced 0 of 12 blocks
 ⚠️ Missed Blocks: flaky produced 8 of 12 blocks
 ⚠️ Forked Block: bp1 block 235702100 replaced by bp2
```

```
✅ Schedule v487 Now Active
 Schedule v487 (21 producers) at block 235702237
 ✚ heliosblocks
 − batcavebp
```

```
✅ Status Update — Rounds 486:100 → 486:243
 143 rounds evaluated, 143 perfect
 21 active producers, 100.00% reliability
 See https://monitor.cryptobloks.io/ for details.
```

## Database

### PostgreSQL Tuning

For large datasets (millions of rounds), configure shared memory in `docker-compose.yml`:

```yaml
postgres:
  shm_size: '1gb'
```

And tune PostgreSQL settings:

```sql
ALTER SYSTEM SET shared_buffers = '512MB';
ALTER SYSTEM SET work_mem = '128MB';
ALTER SYSTEM SET effective_cache_size = '2GB';
```

### Indexes

The schema includes optimized indexes for the common query patterns. For large datasets, ensure these additional indexes exist:

```sql
CREATE INDEX IF NOT EXISTS idx_rounds_chain_ts ON rounds(chain, network, timestamp_start DESC);
CREATE INDEX IF NOT EXISTS idx_rounds_chain_created ON rounds(chain, network, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rounds_chain_id ON rounds(chain, network, id DESC);
CREATE INDEX IF NOT EXISTS idx_rp_roundid_stats ON round_producers(round_id) INCLUDE (producer, blocks_produced, blocks_expected, blocks_missed);
CREATE INDEX IF NOT EXISTS idx_rp_round_producer ON round_producers(round_id, producer, blocks_produced);
CREATE INDEX IF NOT EXISTS idx_missed_chain_ts ON missed_block_events(chain, network, timestamp DESC);
```

### Storage

For best performance, place the PostgreSQL data directory on fast storage (SSD/SAS). The docker-compose volume can be changed from a named volume to a bind mount:

```yaml
volumes:
  - /path/to/fast/storage:/var/lib/postgresql/data
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard UI |
| `GET /api/v1/health` | Health check |
| `GET /api/v1/chains` | List monitored chains |
| `GET /api/v1/:chain/:network/status` | Monitor status, schedule, pending schedule |
| `GET /api/v1/:chain/:network/producers` | Producer reliability, signing keys, status |
| `GET /api/v1/:chain/:network/producers/:name` | Individual producer stats |
| `GET /api/v1/:chain/:network/producers/:name/outages` | Outage history with streak details (`?days=`, `?since=`) |
| `GET /api/v1/:chain/:network/rounds` | Round history (`?limit=`, `?offset=`, `?since=`) |
| `GET /api/v1/:chain/:network/events` | Event log (`?type=`, `?limit=`, `?offset=`, `?since=`, `?until=`) |
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
- **Pending schedule indicator** — shows proposed schedule changes with added/removed producers
- **Round and event pagination** — configurable page size (25/50/100)

## Security Recommendations

When deploying to production:

- **Change the default database password** — the `.env.example` uses `CHANGEME` as a placeholder. Generate a strong password before deploying.
- **Set CORS origin** — change `API_CORS_ORIGIN` from `*` to your dashboard domain (e.g., `https://monitor.yourdomain.com`). Wildcard CORS should only be used in development.
- **Use Cloudflare Access or reverse proxy auth** — the API endpoints are unauthenticated by default. Use Cloudflare Access, nginx basic auth, or a reverse proxy with authentication to restrict access. See issue #1.
- **Enable rate limiting** — consider adding rate limiting at the reverse proxy level. See issue #2.
- **Place PostgreSQL on fast storage** — use SSD or SAS for the Postgres data directory for best query performance with large datasets.
- **Configure `shm_size`** — set `shm_size: '1gb'` (or higher) in docker-compose for PostgreSQL parallel query workers.

## Testing

Requires a PostgreSQL test instance (use `docker-compose.test.yml`):

```bash
# Start test database
docker compose -f docker-compose.test.yml up -d

# Run all tests
TEST_POSTGRES_URL=postgresql://test:test@localhost:15432/core_monitor_test npm test
```

## CI

GitHub Actions runs on all pull requests targeting `main` or `dev`:
- PostgreSQL 17 service container
- Node.js 22
- Build verification
- Docker image build
- Cancels in-progress runs on new push

## Key Design Decisions

- **Slot-based rounds** — `round = floor(slot / (scheduleSize * blocksPerBp))` where `slot = (timestamp - epoch) / 500ms`
- **Schedule-relative numbering** — display round = global round - activation round
- **Partial round discard** — first round after startup or schedule change is always discarded
- **Pending schedule** — `new_producers` from block headers is stored as pending, only applied when `schedule_version` increments
- **Serialized block processing** — blocks are processed sequentially via promise chain to prevent duplicate round evaluations
- **Reader/writer separation** — API and SHiP processing run in separate containers for isolation
- **Time filtering on `timestamp_start`** — queries filter on the round's actual timestamp, not the DB insertion time
- **Summary tables** — daily aggregations avoid full-table scans; API queries read historical days from summaries and supplement with a live scan of today's raw data
- **Incremental + reconciliation** — live writer upserts summaries per round (low overhead); hourly cron re-derives today's stats from raw data as a safety net

## License

[MIT License](LICENSE) — Copyright (c) 2026 Stratovera LLC and its contributors.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting pull requests.
