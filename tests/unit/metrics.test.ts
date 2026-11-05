import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  registry,
  hostLabel,
  blockTimeSeconds,
  initChainSeries,
  initProducerSeries,
  registerChainSource,
  clearChainSources,
  startMetricsServer,
} from '../../src/metrics/metrics.js';
import { ChainMonitor } from '../../src/monitor/ChainMonitor.js';
import { AlertManager } from '../../src/alerting/AlertManager.js';
import type { AlertChannel, AlertMessage } from '../../src/alerting/types.js';
import { TelegramAlert } from '../../src/alerting/TelegramAlert.js';
import { errorCause } from '../../src/utils/errors.js';
import type { ChainConfig } from '../../src/config.js';
import type { RoundResult } from '../../src/monitor/RoundEvaluator.js';
import type { Database } from '../../src/store/Database.js';

/** Value of one series, matched on every label given. */
async function value(name: string, labels: Record<string, string> = {}): Promise<number | undefined> {
  const metric = registry.getSingleMetric(`core_monitor_${name}`);
  if (!metric) throw new Error(`no metric ${name}`);
  const { values } = await metric.get();
  const match = values.find((v) =>
    Object.entries(labels).every(([k, want]) => (v.labels as Record<string, unknown>)[k] === want)
  );
  return match?.value;
}

const chainConfig: ChainConfig = {
  id: 'libre_testnet',
  chain: 'libre',
  network: 'testnet',
  shipUrl: 'ws://127.0.0.1:1',
  shipFailoverUrls: [],
  apiUrl: 'http://127.0.0.1:1',
  chainId: 'c'.repeat(64),
  scheduleSize: 21,
  blocksPerBp: 12,
  blockTimeMs: 500,
};

const message: AlertMessage = {
  severity: 'alert',
  chain: 'libre',
  network: 'testnet',
  title: 'Missed round',
  body: 'body',
  timestamp: '2026-11-03T00:00:00.000',
};

beforeEach(() => {
  registry.resetMetrics();
  clearChainSources();
});

describe('metric helpers', () => {
  it('keeps only host:port in endpoint labels', () => {
    expect(hostLabel('wss://user:secret@ship.libre.pdx.cryptobloks.io/path?token=x#f')).toBe(
      'ship.libre.pdx.cryptobloks.io'
    );
    expect(hostLabel('ws://10.10.10.182:8080')).toBe('10.10.10.182:8080');
    expect(hostLabel('not a url')).toBe('invalid');
  });

  it('reads block timestamps as UTC whatever the zone suffix', () => {
    expect(blockTimeSeconds('2026-11-02T17:12:17.500')).toBe(Date.UTC(2026, 10, 2, 17, 12, 17, 500) / 1000);
    expect(blockTimeSeconds('2026-11-02T17:12:17.500Z')).toBe(Date.UTC(2026, 10, 2, 17, 12, 17, 500) / 1000);
    expect(blockTimeSeconds('garbage')).toBeNull();
  });

  it('creates counter series at 0 so the first increment is visible', async () => {
    initChainSeries('libre', 'testnet');
    initProducerSeries('libre', 'testnet', ['cryptobloks', 'potato1']);

    expect(await value('forks_total', { network: 'testnet' })).toBe(0);
    expect(await value('ship_failovers_total', { network: 'testnet' })).toBe(0);
    expect(await value('blocks_missed_total', { producer: 'potato1' })).toBe(0);
    expect(await value('rounds_missed_total', { producer: 'cryptobloks' })).toBe(0);
  });

  it('reads chain state gauges at scrape time', async () => {
    let snap = { running: true, connected: true, endpointIndex: 0, url: 'ws://10.10.10.181:8080' };
    registerChainSource('libre', 'testnet', () => snap);

    expect(await value('up', { network: 'testnet' })).toBe(1);
    expect(await value('ship_failover_active', { network: 'testnet' })).toBe(0);
    expect(await value('ship_endpoint_info', { role: 'primary', host: '10.10.10.181:8080' })).toBe(1);

    snap = { running: true, connected: false, endpointIndex: 1, url: 'wss://ship.testnet.libre.pdx.cryptobloks.io' };
    expect(await value('ship_connected', { network: 'testnet' })).toBe(0);
    expect(await value('ship_failover_active', { network: 'testnet' })).toBe(1);
    expect(await value('ship_endpoint_info', { role: 'primary' })).toBeUndefined();
    expect(await value('ship_endpoint_info', { role: 'failover' })).toBe(1);
  });

  it('describes fetch failure causes by code only', () => {
    const err = new TypeError('fetch failed', { cause: Object.assign(new Error('connect to secret-host'), { code: 'ETIMEDOUT' }) });
    expect(errorCause(err)).toBe('ETIMEDOUT');
    const agg = new TypeError('fetch failed', {
      cause: { name: 'AggregateError', errors: [{ code: 'ECONNREFUSED' }, { code: 'ENETUNREACH' }] },
    });
    expect(errorCause(agg)).toBe('ECONNREFUSED,ENETUNREACH');
    expect(errorCause(new Error('plain'))).toBeUndefined();
  });
});

describe('round metrics from ChainMonitor', () => {
  it('counts produced and missed blocks and missed rounds per producer', async () => {
    const monitor = new ChainMonitor(chainConfig, {} as Database);
    const round: RoundResult = {
      roundNumber: 7,
      scheduleVersion: 276,
      timestampStart: '2026-11-02T17:10:12.000',
      timestampEnd: '2026-11-02T17:12:17.500',
      producersProduced: 1,
      producersMissed: 2,
      forks: [],
      producerResults: [
        { producer: 'amsterdam', position: 0, blocksExpected: 12, blocksProduced: 12, blocksMissed: 0, firstBlock: 1, lastBlock: 12 },
        { producer: 'blocktime', position: 1, blocksExpected: 12, blocksProduced: 5, blocksMissed: 7, firstBlock: 13, lastBlock: 17 },
        { producer: 'cryptobloks', position: 2, blocksExpected: 12, blocksProduced: 0, blocksMissed: 12, firstBlock: null, lastBlock: null },
        { producer: 'partial', position: 3, blocksExpected: 0, blocksProduced: 0, blocksMissed: 0, firstBlock: null, lastBlock: null },
      ],
    };

    (monitor as any).emitRoundResult(round);
    (monitor as any).emitRoundResult(round);

    expect(await value('rounds_total', { network: 'testnet' })).toBe(2);
    expect(await value('blocks_produced_total', { producer: 'amsterdam' })).toBe(24);
    expect(await value('blocks_missed_total', { producer: 'blocktime' })).toBe(14);
    expect(await value('rounds_missed_total', { producer: 'blocktime' })).toBeUndefined();
    expect(await value('blocks_missed_total', { producer: 'cryptobloks' })).toBe(24);
    expect(await value('rounds_missed_total', { producer: 'cryptobloks' })).toBe(2);
    expect(await value('rounds_missed_total', { producer: 'partial' })).toBeUndefined();
  });
});

describe('alert delivery metrics', () => {
  const channel = (name: string, send: AlertChannel['send']): AlertChannel => ({ name, send });

  it('counts sent and failed deliveries per channel, starting from 0', async () => {
    const manager = new AlertManager(0);
    manager.addChannel(channel('good', vi.fn(async () => 'sent' as const)));
    manager.addChannel(channel('bad', vi.fn(async () => 'failed' as const)));
    manager.addChannel(channel('throws', vi.fn(async () => { throw new Error('boom'); })));
    manager.addChannel(channel('idle', vi.fn(async () => 'skipped' as const)));

    expect(await value('alert_send_failures_total', { channel: 'bad' })).toBe(0);

    await manager.sendAlert(message);

    expect(await value('alerts_sent_total', { channel: 'good', severity: 'alert' })).toBe(1);
    expect(await value('alert_send_failures_total', { channel: 'good' })).toBe(0);
    expect(await value('alert_send_failures_total', { channel: 'bad' })).toBe(1);
    expect(await value('alert_send_failures_total', { channel: 'throws' })).toBe(1);
    expect(await value('alert_send_failures_total', { channel: 'idle' })).toBe(0);
    expect(await value('alerts_sent_total', { channel: 'idle', severity: 'alert' })).toBe(0);
  });

  describe('Telegram results', () => {
    const originalFetch = globalThis.fetch;
    afterEach(() => {
      globalThis.fetch = originalFetch;
    });

    const telegram = () =>
      new TelegramAlert('123:abc', new Map([['libre_testnet', { statusChatId: 's', alertChatId: 'a' }]]), { retryDelaysMs: [0, 0] });

    it('reports failed when any chat send fails', async () => {
      let call = 0;
      globalThis.fetch = vi.fn(async () => {
        call++;
        if (call >= 2) throw new TypeError('fetch failed', { cause: { code: 'ECONNRESET' } });
        return new Response('{}', { status: 200 });
      }) as typeof fetch;

      expect(await telegram().send({ ...message, routing: 'both' })).toBe('failed');
    });

    it('reports failed on an API error and sent on success', async () => {
      globalThis.fetch = vi.fn(async () => new Response('bad', { status: 400 })) as typeof fetch;
      expect(await telegram().send(message)).toBe('failed');

      globalThis.fetch = vi.fn(async () => new Response('{}', { status: 200 })) as typeof fetch;
      expect(await telegram().send(message)).toBe('sent');
    });

    it('reports skipped when the chain has no chat ids', async () => {
      globalThis.fetch = vi.fn() as unknown as typeof fetch;
      expect(await telegram().send({ ...message, network: 'mainnet' })).toBe('skipped');
      expect(globalThis.fetch).not.toHaveBeenCalled();
    });
  });
});

describe('metrics endpoint', () => {
  let server: Server;
  afterEach(() => server?.close());

  it('serves Prometheus text on /metrics and 404 elsewhere', async () => {
    server = await startMetricsServer(0, '127.0.0.1');
    const { port } = server.address() as AddressInfo;
    initChainSeries('libre', 'mainnet');

    const res = await fetch(`http://127.0.0.1:${port}/metrics`);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/plain');
    const body = await res.text();
    expect(body).toContain('core_monitor_forks_total{chain="libre",network="mainnet"} 0');
    expect(body).toContain('core_monitor_process_cpu_seconds_total');

    expect((await fetch(`http://127.0.0.1:${port}/`)).status).toBe(404);
    expect((await fetch(`http://127.0.0.1:${port}/metrics`, { method: 'POST' })).status).toBe(404);
  });
});
