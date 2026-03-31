import { logger } from '../utils/logger.js';
import type { AlertChannel, AlertMessage, AlertSeverity } from './types.js';

const log = logger.child({ module: 'AlertManager' });

interface ThrottleEntry {
  lastSent: number;
  count: number;
}

export class AlertManager {
  private channels: AlertChannel[] = [];
  private throttleMap: Map<string, ThrottleEntry> = new Map();
  private throttleWindowMs: number;

  constructor(throttleWindowMs: number = 60000) {
    this.throttleWindowMs = throttleWindowMs;
  }

  addChannel(channel: AlertChannel): void {
    this.channels.push(channel);
    log.info({ channel: channel.name }, 'Alert channel registered');
  }

  async sendAlert(message: AlertMessage): Promise<void> {
    // Throttle duplicate alerts
    const key = `${message.chain}:${message.network}:${message.title}`;
    const now = Date.now();
    const throttle = this.throttleMap.get(key);

    if (throttle && now - throttle.lastSent < this.throttleWindowMs) {
      throttle.count++;
      return;
    }

    // If we suppressed messages, note that in the body
    let body = message.body;
    if (throttle && throttle.count > 0) {
      body += `\n(${throttle.count} similar alert(s) suppressed)`;
    }

    this.throttleMap.set(key, { lastSent: now, count: 0 });

    const enriched: AlertMessage = { ...message, body };

    log.info(
      { severity: message.severity, chain: message.chain, network: message.network, title: message.title },
      'Sending alert'
    );

    const sends = this.channels.map((ch) =>
      ch.send(enriched).catch((err) => {
        log.error({ err, channel: ch.name }, 'Alert channel send failed');
      })
    );

    await Promise.all(sends);
  }

  // -- Per-producer alerts (individual notifications) --

  async missedRound(params: {
    chain: string;
    network: string;
    producer: string;
    round: number;
    scheduleVersion: number;
    blocksMissed: number;
    timestamp: string;
  }): Promise<void> {
    const roundNum = params.round.toLocaleString();
    await this.sendAlert({
      severity: 'alert',
      chain: params.chain,
      network: params.network,
      title: `Missed Round [ Schedule ${params.scheduleVersion} / Round ${roundNum} ]`,
      body: `\u274C Missed Round: ${params.producer} produced 0 of ${params.blocksMissed} blocks`,
      timestamp: params.timestamp,
    });
  }

  async missedBlocks(params: {
    chain: string;
    network: string;
    producer: string;
    round: number;
    scheduleVersion: number;
    blocksProduced: number;
    blocksMissed: number;
    blocksExpected: number;
    timestamp: string;
  }): Promise<void> {
    const roundNum = params.round.toLocaleString();
    await this.sendAlert({
      severity: 'warn',
      chain: params.chain,
      network: params.network,
      title: `Missed Blocks [ Schedule ${params.scheduleVersion} / Round ${roundNum} ]`,
      body: `\u26A0\uFE0F Missed Blocks: ${params.producer} produced ${params.blocksProduced} of ${params.blocksExpected} blocks`,
      timestamp: params.timestamp,
    });
  }

  // -- Round summary (degraded round alert) --

  async roundComplete(params: {
    chain: string;
    network: string;
    round: number;
    producersProduced: number;
    producersMissed: number;
    partialProducers?: Array<{ producer: string; produced: number; missed: number; expected: number }>;
    missedProducers?: Array<{ producer: string; expected: number }>;
    forks?: Array<{ blockNumber: number; originalProducer: string; replacementProducer: string }>;
    scheduleVersion: number;
    timestamp: string;
  }): Promise<void> {
    const missed = params.missedProducers || [];
    const partial = params.partialProducers || [];
    const forks = params.forks || [];

    // Only send when round is degraded
    if (missed.length === 0 && partial.length === 0 && forks.length === 0) return;

    const roundNum = params.round.toLocaleString();
    const lines: string[] = [];

    for (const m of missed) {
      lines.push(`\u274C Missed Round: ${m.producer} produced 0 of ${m.expected} blocks`);
    }
    for (const p of partial) {
      lines.push(`\u26A0\uFE0F Missed Blocks: ${p.producer} produced ${p.produced} of ${p.expected} blocks`);
    }
    for (const f of forks) {
      lines.push(`\u26A0\uFE0F Forked Block: ${f.originalProducer} block ${f.blockNumber} replaced by ${f.replacementProducer}`);
    }

    await this.sendAlert({
      severity: 'alert',
      chain: params.chain,
      network: params.network,
      title: `Degraded Round [ Schedule ${params.scheduleVersion} / Round ${roundNum} ]`,
      body: lines.join('\n'),
      timestamp: params.timestamp,
    });
  }

  // -- Schedule & producer events --

  async scheduleChange(params: {
    chain: string;
    network: string;
    version: number;
    producers: string[];
    added?: string[];
    removed?: string[];
    keyUpdates?: string[];
    blockNum: number;
    timestamp: string;
  }): Promise<void> {
    const added = params.added || [];
    const removed = params.removed || [];
    const keyUpdates = params.keyUpdates || [];

    let title: string;
    if (added.length === 0 && removed.length === 0 && keyUpdates.length > 0) {
      title = `Schedule v${params.version} — Key Update`;
    } else {
      title = `Schedule Change to v${params.version}`;
    }

    const lines: string[] = [];
    lines.push(`Schedule v${params.version} (${params.producers.length} producers) at block ${params.blockNum}`);
    if (added.length > 0) lines.push(`Added: ${added.join(', ')}`);
    if (removed.length > 0) lines.push(`Removed: ${removed.join(', ')}`);
    if (keyUpdates.length > 0) lines.push(`Key update: ${keyUpdates.join(', ')}`);
    if (added.length === 0 && removed.length === 0 && keyUpdates.length === 0) {
      lines.push(`Producers: ${params.producers.join(', ')}`);
    }

    await this.sendAlert({
      severity: 'info',
      chain: params.chain,
      network: params.network,
      title,
      body: lines.join('\n'),
      timestamp: params.timestamp,
    });
  }

  async producerAction(params: {
    chain: string;
    network: string;
    action: string;
    producer: string;
    data: any;
    blockNum: number;
    timestamp: string;
  }): Promise<void> {
    let actionLabel: string;
    let severity: 'info' | 'warn' | 'alert';
    if (params.action === 'regproducer') {
      actionLabel = 'Registered';
      severity = 'info';
    } else if (params.action === 'kickbp') {
      actionLabel = 'Kicked';
      severity = 'warn';
    } else {
      actionLabel = 'Unregistered';
      severity = 'info';
    }
    await this.sendAlert({
      severity,
      chain: params.chain,
      network: params.network,
      title: `Producer ${actionLabel}: ${params.producer}`,
      body: `${params.producer} ${actionLabel.toLowerCase()} at block ${params.blockNum}`,
      timestamp: params.timestamp,
    });
  }

  async fork(params: {
    chain: string;
    network: string;
    blockNumber: number;
    originalProducer: string;
    replacementProducer: string;
    timestamp: string;
  }): Promise<void> {
    await this.sendAlert({
      severity: 'warn',
      chain: params.chain,
      network: params.network,
      title: `Forked Block [ block ${params.blockNumber} ]`,
      body: `\u26A0\uFE0F Forked Block: ${params.originalProducer} block ${params.blockNumber} replaced by ${params.replacementProducer}`,
      timestamp: params.timestamp,
    });
  }
}
