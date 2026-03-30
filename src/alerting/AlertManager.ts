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

  // Convenience methods

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
      title: `${params.producer} [ Schedule ${params.scheduleVersion} / Round ${roundNum} ]`,
      body: `Producer ${params.producer} failed to produce 1 or more blocks in Schedule ${params.scheduleVersion} Round ${roundNum} (${params.blocksMissed} blocks missed)`,
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
      title: `${params.producer} [ Schedule ${params.scheduleVersion} / Round ${roundNum} ]`,
      body: `Producer ${params.producer} failed to produce 1 or more blocks in Schedule ${params.scheduleVersion} Round ${roundNum} (${params.blocksMissed} of ${params.blocksExpected} blocks missed)`,
      timestamp: params.timestamp,
    });
  }

  async roundComplete(params: {
    chain: string;
    network: string;
    round: number;
    producersProduced: number;
    producersMissed: number;
    scheduleVersion: number;
    timestamp: string;
  }): Promise<void> {
    // Only send round summaries when there are issues, or periodically
    if (params.producersMissed === 0) return;

    const roundNum = params.round.toLocaleString();
    await this.sendAlert({
      severity: params.producersMissed > 0 ? 'warn' : 'info',
      chain: params.chain,
      network: params.network,
      title: `Round Complete [ Schedule ${params.scheduleVersion} / Round ${roundNum} ]`,
      body: `${params.producersProduced} produced, ${params.producersMissed} missed`,
      timestamp: params.timestamp,
    });
  }

  async scheduleChange(params: {
    chain: string;
    network: string;
    version: number;
    producers: string[];
    blockNum: number;
    timestamp: string;
  }): Promise<void> {
    await this.sendAlert({
      severity: 'info',
      chain: params.chain,
      network: params.network,
      title: `Schedule change to v${params.version}`,
      body: `New schedule (${params.producers.length} producers) at block ${params.blockNum}\nProducers: ${params.producers.join(', ')}`,
      timestamp: params.timestamp,
    });
  }

  async producerAction(params: {
    chain: string;
    network: string;
    action: string;
    data: any;
    blockNum: number;
    timestamp: string;
  }): Promise<void> {
    const actionLabel = params.action === 'regproducer' ? 'registered' : 'unregistered';
    await this.sendAlert({
      severity: 'info',
      chain: params.chain,
      network: params.network,
      title: `Producer ${actionLabel}`,
      body: `${params.action} at block ${params.blockNum}`,
      timestamp: params.timestamp,
    });
  }
}
