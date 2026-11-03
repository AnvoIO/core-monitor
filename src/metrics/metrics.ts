/**
 * Prometheus metrics for the writer.
 *
 * Counters and gauges are updated by ChainMonitor and AlertManager. Chain state gauges
 * (up, SHiP connection) are read from registered sources at scrape time, so they can't go stale.
 */

import http from 'node:http';
import { Counter, Gauge, Registry, collectDefaultMetrics } from 'prom-client';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'metrics' });

export const METRIC_PREFIX = 'core_monitor_';

export const registry = new Registry();

const chainLabels = ['chain', 'network'] as const;
const producerLabels = ['chain', 'network', 'producer'] as const;

export const blocksProduced = new Counter({
  name: `${METRIC_PREFIX}blocks_produced_total`,
  help: 'Blocks produced per producer, counted per completed round',
  labelNames: producerLabels,
  registers: [registry],
});

export const blocksMissed = new Counter({
  name: `${METRIC_PREFIX}blocks_missed_total`,
  help: 'Blocks missed per producer, counted per completed round',
  labelNames: producerLabels,
  registers: [registry],
});

export const roundsMissed = new Counter({
  name: `${METRIC_PREFIX}rounds_missed_total`,
  help: 'Rounds in which the producer produced no blocks',
  labelNames: producerLabels,
  registers: [registry],
});

export const roundsEvaluated = new Counter({
  name: `${METRIC_PREFIX}rounds_total`,
  help: 'Rounds evaluated',
  labelNames: chainLabels,
  registers: [registry],
});

export const forks = new Counter({
  name: `${METRIC_PREFIX}forks_total`,
  help: 'Forks detected (a delivered block replaced by another at the same height)',
  labelNames: chainLabels,
  registers: [registry],
});

export const headBlock = new Gauge({
  name: `${METRIC_PREFIX}head_block`,
  help: 'Chain head block number reported by SHiP',
  labelNames: chainLabels,
  registers: [registry],
});

export const libBlock = new Gauge({
  name: `${METRIC_PREFIX}lib_block`,
  help: 'Last irreversible block number reported by SHiP',
  labelNames: chainLabels,
  registers: [registry],
});

export const processedBlock = new Gauge({
  name: `${METRIC_PREFIX}processed_block`,
  help: 'Last block number processed by the writer',
  labelNames: chainLabels,
  registers: [registry],
});

export const blocksBehind = new Gauge({
  name: `${METRIC_PREFIX}blocks_behind`,
  help: 'Head block minus last processed block',
  labelNames: chainLabels,
  registers: [registry],
});

export const lastBlockTimestamp = new Gauge({
  name: `${METRIC_PREFIX}last_block_timestamp_seconds`,
  help: 'Block time of the last processed block, as Unix seconds',
  labelNames: chainLabels,
  registers: [registry],
});

export const shipDisconnects = new Counter({
  name: `${METRIC_PREFIX}ship_disconnects_total`,
  help: 'SHiP connections lost (socket closed or stalled)',
  labelNames: chainLabels,
  registers: [registry],
});

export const shipReconnectAttempts = new Counter({
  name: `${METRIC_PREFIX}ship_reconnect_attempts_total`,
  help: 'SHiP reconnect attempts scheduled',
  labelNames: chainLabels,
  registers: [registry],
});

export const shipFailovers = new Counter({
  name: `${METRIC_PREFIX}ship_failovers_total`,
  help: 'Switches from the primary SHiP endpoint to a failover endpoint',
  labelNames: chainLabels,
  registers: [registry],
});

export const alertsSent = new Counter({
  name: `${METRIC_PREFIX}alerts_sent_total`,
  help: 'Alerts delivered, per channel and severity',
  labelNames: ['channel', 'severity'] as const,
  registers: [registry],
});

export const alertSendFailures = new Counter({
  name: `${METRIC_PREFIX}alert_send_failures_total`,
  help: 'Alerts that failed to deliver, per channel (alerts are not retried)',
  labelNames: ['channel'] as const,
  registers: [registry],
});

export interface ChainSnapshot {
  /** The chain monitor is running (started and not stopped). */
  running: boolean;
  /** The SHiP WebSocket is open. */
  connected: boolean;
  /** 0 for the primary SHiP endpoint, >0 for a failover endpoint. */
  endpointIndex: number;
  url: string;
}

const chainSources = new Map<string, { chain: string; network: string; read: () => ChainSnapshot }>();

/** Register a chain monitor; its state gauges are read from `read` at scrape time. */
export function registerChainSource(chain: string, network: string, read: () => ChainSnapshot): void {
  chainSources.set(`${chain}:${network}`, { chain, network, read });
}

/** Test helper: forget all registered chain sources. */
export function clearChainSources(): void {
  chainSources.clear();
}

/**
 * host[:port] of a URL, with no scheme, credentials, path or query, so nothing secret
 * in a URL can become a label value.
 */
export function hostLabel(url: string): string {
  try {
    return new URL(url).host || 'unknown';
  } catch {
    return 'invalid';
  }
}

function snapshotGauge(name: string, help: string, value: (snap: ChainSnapshot) => number): void {
  new Gauge({
    name: `${METRIC_PREFIX}${name}`,
    help,
    labelNames: chainLabels,
    registers: [registry],
    collect() {
      this.reset();
      for (const src of chainSources.values()) {
        this.set({ chain: src.chain, network: src.network }, value(src.read()));
      }
    },
  });
}

snapshotGauge('up', '1 while the writer is running the monitor for this chain', (s) => (s.running ? 1 : 0));
snapshotGauge('ship_connected', '1 when the SHiP WebSocket is open', (s) => (s.connected ? 1 : 0));
snapshotGauge(
  'ship_failover_active',
  '1 when on (or reconnecting to) a failover SHiP endpoint instead of the primary',
  (s) => (s.endpointIndex > 0 ? 1 : 0)
);

new Gauge({
  name: `${METRIC_PREFIX}ship_endpoint_info`,
  help: 'Current SHiP endpoint (value is always 1)',
  labelNames: ['chain', 'network', 'role', 'host'] as const,
  registers: [registry],
  collect() {
    this.reset();
    for (const src of chainSources.values()) {
      const snap = src.read();
      this.set(
        {
          chain: src.chain,
          network: src.network,
          role: snap.endpointIndex === 0 ? 'primary' : 'failover',
          host: hostLabel(snap.url),
        },
        1
      );
    }
  },
});

/*
 * Counters are created at 0 before their first increment: increase() and rate() treat a
 * series' first sample as a baseline, so a first increment without a 0 before it is lost.
 */

/** Create the per-chain counter series at 0. */
export function initChainSeries(chain: string, network: string): void {
  const labels = { chain, network };
  for (const c of [roundsEvaluated, forks, shipDisconnects, shipReconnectAttempts, shipFailovers]) {
    c.inc(labels, 0);
  }
}

/** Create the per-producer counter series at 0 (idempotent). */
export function initProducerSeries(chain: string, network: string, producers: readonly string[]): void {
  for (const producer of producers) {
    const labels = { chain, network, producer };
    blocksProduced.inc(labels, 0);
    blocksMissed.inc(labels, 0);
    roundsMissed.inc(labels, 0);
  }
}

export const ALERT_SEVERITIES = ['info', 'warn', 'alert'] as const;

/** Create the per-channel alert counter series at 0. */
export function initAlertChannelSeries(channel: string): void {
  alertSendFailures.inc({ channel }, 0);
  for (const severity of ALERT_SEVERITIES) alertsSent.inc({ channel, severity }, 0);
}

collectDefaultMetrics({ register: registry, prefix: METRIC_PREFIX });

/** Parse an Antelope block timestamp (UTC, usually without a zone suffix) to Unix seconds. */
export function blockTimeSeconds(timestamp: string): number | null {
  const hasZone = /(Z|[+-]\d{2}:?\d{2})$/.test(timestamp);
  const ms = Date.parse(hasZone ? timestamp : `${timestamp}Z`);
  return Number.isFinite(ms) ? ms / 1000 : null;
}

/** Serve GET /metrics. Every other path is 404. */
export function startMetricsServer(port: number, host: string): Promise<http.Server> {
  const server = http.createServer(async (req, res) => {
    const path = (req.url || '').split('?')[0];
    if (req.method !== 'GET' || path !== '/metrics') {
      res.writeHead(404, { 'Content-Type': 'text/plain' }).end('Not found\n');
      return;
    }
    try {
      const body = await registry.metrics();
      res.writeHead(200, { 'Content-Type': registry.contentType }).end(body);
    } catch (err) {
      log.error({ err }, 'Failed to render metrics');
      res.writeHead(500, { 'Content-Type': 'text/plain' }).end('Error\n');
    }
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      server.off('error', reject);
      log.info({ host, port, path: '/metrics' }, 'Metrics endpoint listening');
      resolve(server);
    });
  });
}
