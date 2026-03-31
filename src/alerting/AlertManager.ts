import { logger } from '../utils/logger.js';
import type { AlertChannel, AlertMessage, AlertSeverity } from './types.js';

const log = logger.child({ module: 'AlertManager' });

interface ThrottleEntry {
  lastSent: number;
  count: number;
}

interface StatusAccumulator {
  firstSchedule: number;
  firstRound: number;
  lastSchedule: number;
  lastRound: number;
  totalRounds: number;
  degradedRounds: number;
  producerCount: number;
  lastUpdate: number;
}

const STATUS_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 hours

export class AlertManager {
  private channels: AlertChannel[] = [];
  private throttleMap: Map<string, ThrottleEntry> = new Map();
  private throttleWindowMs: number;
  private statusAccumulators: Map<string, StatusAccumulator> = new Map();

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
    const isDegraded = missed.length > 0 || partial.length > 0 || forks.length > 0;

    // Accumulate status for periodic update
    await this.trackRound(params.chain, params.network, params.scheduleVersion, params.round,
      params.producersProduced + params.producersMissed, isDegraded, params.timestamp);

    // Only send degraded round alert when there are issues
    if (!isDegraded) return;

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

  private async trackRound(
    chain: string, network: string, scheduleVersion: number, round: number,
    producerCount: number, isDegraded: boolean, timestamp: string
  ): Promise<void> {
    const key = `${chain}:${network}`;
    let acc = this.statusAccumulators.get(key);
    if (!acc) {
      acc = {
        firstSchedule: scheduleVersion,
        firstRound: round,
        lastSchedule: scheduleVersion,
        lastRound: round,
        totalRounds: 0,
        degradedRounds: 0,
        producerCount,
        lastUpdate: Date.now(),
      };
      this.statusAccumulators.set(key, acc);
    }

    acc.lastSchedule = scheduleVersion;
    acc.lastRound = round;
    acc.totalRounds++;
    if (isDegraded) acc.degradedRounds++;
    acc.producerCount = producerCount;

    const now = Date.now();
    if (now - acc.lastUpdate >= STATUS_INTERVAL_MS) {
      const perfect = acc.totalRounds - acc.degradedRounds;
      const reliability = acc.totalRounds > 0
        ? ((perfect / acc.totalRounds) * 100).toFixed(2) : '100.00';

      const roundSpan = acc.firstSchedule === acc.lastSchedule
        ? `${acc.firstSchedule}:${acc.firstRound} \u2192 ${acc.lastSchedule}:${acc.lastRound}`
        : `${acc.firstSchedule}:${acc.firstRound} \u2192 ${acc.lastSchedule}:${acc.lastRound}`;

      const lines: string[] = [];
      if (acc.degradedRounds > 0) {
        lines.push(`${acc.totalRounds} rounds evaluated, ${perfect} perfect, ${acc.degradedRounds} degraded`);
      } else {
        lines.push(`${acc.totalRounds} rounds evaluated, ${acc.totalRounds} perfect`);
      }
      lines.push(`${acc.producerCount} active producers, ${reliability}% reliability`);
      lines.push(`See https://monitor.cryptobloks.io/ for details.`);

      await this.sendAlert({
        severity: 'info',
        chain,
        network,
        title: `Status Update \u2014 Rounds ${roundSpan}`,
        body: lines.join('\n'),
        timestamp,
      });

      // Reset accumulator
      acc.firstSchedule = acc.lastSchedule;
      acc.firstRound = acc.lastRound;
      acc.totalRounds = 0;
      acc.degradedRounds = 0;
      acc.lastUpdate = now;
    }
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
      title = `Schedule v${params.version} Now Active — Key Update`;
    } else {
      title = `Schedule v${params.version} Now Active`;
    }

    const lines: string[] = [];
    lines.push(`Schedule v${params.version} (${params.producers.length} producers) at block ${params.blockNum}`);
    if (added.length > 0) lines.push(`\u271A ${added.join(', ')}`);
    if (removed.length > 0) lines.push(`\u2212 ${removed.join(', ')}`);
    if (keyUpdates.length > 0) lines.push(`\uD83D\uDD11 ${keyUpdates.join(', ')}`);
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
