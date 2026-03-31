import { logger } from '../utils/logger.js';
import { Database } from '../store/Database.js';

const log = logger.child({ module: 'ScheduleTracker' });

export interface ProducerSchedule {
  version: number;
  producers: string[];
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

  async updateSchedule(
    version: number,
    producers: Array<{ producer_name: string; block_signing_key: string }>,
    blockNum: number,
    timestamp: string
  ): Promise<boolean> {
    if (this.currentSchedule && version <= this.currentSchedule.version) {
      return false;
    }

    const newProducers = producers.map((p) => p.producer_name);
    const oldProducers = this.currentSchedule?.producers ?? [];

    const added = newProducers.filter((p) => !oldProducers.includes(p));
    const removed = oldProducers.filter((p) => !newProducers.includes(p));

    log.info(
      {
        chain: this.chain,
        network: this.network,
        oldVersion: this.currentSchedule?.version ?? 'none',
        newVersion: version,
        producerCount: newProducers.length,
        added: added.length > 0 ? added : undefined,
        removed: removed.length > 0 ? removed : undefined,
      },
      'Schedule change detected'
    );

    this.currentSchedule = { version, producers: newProducers };

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
      producer_list: JSON.stringify(newProducers),
      block_number: blockNum,
      timestamp,
    });

    return true;
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
