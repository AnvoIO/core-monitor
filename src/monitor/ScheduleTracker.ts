import { logger } from '../utils/logger.js';
import { Database } from '../store/Database.js';

const log = logger.child({ module: 'ScheduleTracker' });

export interface ProducerSchedule {
  version: number;
  producers: string[];
  producerKeys?: Record<string, string>;
}

export class ScheduleTracker {
  private chain: string;
  private network: string;
  private db: Database;
  private statePrefix: string;
  private currentSchedule: ProducerSchedule | null = null;

  constructor(chain: string, network: string, db: Database, statePrefix: string = '') {
    this.chain = chain;
    this.network = network;
    this.db = db;
    this.statePrefix = statePrefix;
  }

  async init(): Promise<void> {
    const saved = await this.db.getState(this.chain, this.network, `${this.statePrefix}schedule`);
    if (saved) {
      try {
        this.currentSchedule = JSON.parse(saved);
        log.info(
          { version: this.currentSchedule!.version, producers: this.currentSchedule!.producers.length },
          'Restored schedule from state'
        );
      } catch {
        // ignore parse errors
      }
    }
  }

  get schedule(): ProducerSchedule | null {
    return this.currentSchedule;
  }

  get version(): number {
    return this.currentSchedule?.version ?? 0;
  }

  get producers(): string[] {
    return this.currentSchedule?.producers ?? [];
  }

  get producerKeys(): Record<string, string> {
    return this.currentSchedule?.producerKeys ?? {};
  }

  async updateSchedule(
    version: number,
    producers: Array<{ producer_name: string; block_signing_key: string }>,
    blockNum: number,
    timestamp: string
  ): Promise<false | { added: string[]; removed: string[]; keyUpdates: string[] }> {
    if (this.currentSchedule && version <= this.currentSchedule.version) {
      return false;
    }

    const newProducers = producers.map((p) => p.producer_name);
    const oldProducers = this.currentSchedule?.producers ?? [];
    const oldKeys = this.currentSchedule?.producerKeys ?? {};

    const added = newProducers.filter((p) => !oldProducers.includes(p));
    const removed = oldProducers.filter((p) => !newProducers.includes(p));

    // Detect key-only changes: same producer name, different signing key
    const newKeys: Record<string, string> = {};
    for (const p of producers) {
      newKeys[p.producer_name] = p.block_signing_key;
    }

    const keyUpdates: string[] = [];
    for (const p of newProducers) {
      if (oldKeys[p] && newKeys[p] && oldKeys[p] !== newKeys[p] && !added.includes(p)) {
        keyUpdates.push(p);
      }
    }

    log.info(
      {
        chain: this.chain,
        network: this.network,
        oldVersion: this.currentSchedule?.version ?? 'none',
        newVersion: version,
        producerCount: newProducers.length,
        added: added.length > 0 ? added : undefined,
        removed: removed.length > 0 ? removed : undefined,
        keyUpdates: keyUpdates.length > 0 ? keyUpdates : undefined,
      },
      'Schedule change detected'
    );

    this.currentSchedule = { version, producers: newProducers, producerKeys: newKeys };

    await this.db.setState(
      this.chain,
      this.network,
      `${this.statePrefix}schedule`,
      JSON.stringify(this.currentSchedule)
    );

    await this.db.insertScheduleChange({
      chain: this.chain,
      network: this.network,
      schedule_version: version,
      producers_added: JSON.stringify(added),
      producers_removed: JSON.stringify(removed),
      producers_key_updates: JSON.stringify(keyUpdates),
      producer_list: JSON.stringify(newProducers),
      block_number: blockNum,
      timestamp,
    });

    return { added, removed, keyUpdates };
  }

  getProducerPosition(producer: string): number {
    if (!this.currentSchedule) return -1;
    return this.currentSchedule.producers.indexOf(producer);
  }

  getExpectedProducerForPosition(position: number): string | null {
    if (!this.currentSchedule) return null;
    if (position < 0 || position >= this.currentSchedule.producers.length) return null;
    return this.currentSchedule.producers[position];
  }
}
