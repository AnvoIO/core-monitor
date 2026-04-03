import { logger } from '../utils/logger.js';
import { Database } from '../store/Database.js';
import { ScheduleTracker } from './ScheduleTracker.js';
import type { ChainConfig } from '../config.js';

const log = logger.child({ module: 'RoundEvaluator' });

/** Antelope epoch: 2000-01-01T00:00:00.000Z */
const ANTELOPE_EPOCH_MS = Date.UTC(2000, 0, 1);

export interface BlockRecord {
  block_num: number;
  producer: string;
  timestamp: string;
  schedule_version: number;
}

export interface RoundFork {
  blockNumber: number;
  originalProducer: string;
  replacementProducer: string;
}

export interface RoundResult {
  roundNumber: number;
  scheduleVersion: number;
  timestampStart: string;
  timestampEnd: string;
  producerResults: ProducerRoundResult[];
  producersProduced: number;
  producersMissed: number;
  forks: RoundFork[];
}

export interface ProducerRoundResult {
  producer: string;
  position: number;
  blocksExpected: number;
  blocksProduced: number;
  blocksMissed: number;
  firstBlock: number | null;
  lastBlock: number | null;
}

export function timestampToSlot(timestamp: string): number {
  const ms = new Date(timestamp).getTime();
  return Math.floor((ms - ANTELOPE_EPOCH_MS) / 500);
}

function slotToGlobalRound(slot: number, scheduleSize: number, blocksPerBp: number): number {
  return Math.floor(slot / (scheduleSize * blocksPerBp));
}

export interface RoundEvaluatorOptions {
  /** Write last_block state every N blocks instead of every block. Default: 1. */
  stateWriteInterval?: number;
  /** Use batch inserts for round persistence. Default: false. */
  batchInserts?: boolean;
}

export class RoundEvaluator {
  private config: ChainConfig;
  private db: Database;
  private schedule: ScheduleTracker;
  private statePrefix: string;
  private currentGlobalRound: number = -1;
  private scheduleActivationGlobalRound: number = 0;
  private lastBlockNum: number = 0;
  private firstRoundIsPartial: boolean = true;
  private stateWriteInterval: number;
  private batchInserts: boolean;
  private blocksSinceLastStateWrite: number = 0;

  private roundBlocks: Map<string, { count: number; firstBlock: number; lastBlock: number }> = new Map();
  private roundForks: RoundFork[] = [];
  private roundStartTimestamp: string = '';
  private roundEndTimestamp: string = '';

  /** In-progress outage streaks per producer (live writer only). */
  private outageStreaks: Map<string, {
    count: number;
    startRound: number;
    startTimestamp: string;
    endRound: number;
    endTimestamp: string;
    scheduleVersion: number;
  }> = new Map();

  constructor(config: ChainConfig, db: Database, schedule: ScheduleTracker, statePrefix: string = '', options: RoundEvaluatorOptions = {}) {
    this.config = config;
    this.db = db;
    this.schedule = schedule;
    this.statePrefix = statePrefix;
    this.stateWriteInterval = options.stateWriteInterval ?? 1;
    this.batchInserts = options.batchInserts ?? false;
  }

  async init(): Promise<void> {
    const savedBlock = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}last_block`);
    this.lastBlockNum = savedBlock ? parseInt(savedBlock, 10) : 0;

    if (this.batchInserts) {
      // Catchup mode: don't restore currentGlobalRound — let processBlock
      // recompute it from the first block in the stream. Restoring a stale
      // value would prevent round boundaries from triggering if the stream
      // resumes from a different position.
      this.currentGlobalRound = -1;
    } else {
      const savedGlobalRound = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}current_global_round`);
      this.currentGlobalRound = savedGlobalRound ? parseInt(savedGlobalRound, 10) : -1;
    }
    // Always discard the first round — in-memory roundBlocks is empty after
    // restart, so the first round boundary would evaluate an incomplete round.
    this.firstRoundIsPartial = true;

    const savedActivation = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}schedule_activation_global_round`);
    this.scheduleActivationGlobalRound = savedActivation ? parseInt(savedActivation, 10) : 0;

    log.info(
      {
        chain: this.config.chain, network: this.config.network,
        globalRound: this.currentGlobalRound,
        scheduleRound: this.displayRound,
        lastBlock: this.lastBlockNum,
      },
      'RoundEvaluator initialized'
    );
  }

  get displayRound(): number {
    if (this.currentGlobalRound < 0) return 0;
    return this.currentGlobalRound - this.scheduleActivationGlobalRound;
  }

  get round(): number {
    return this.displayRound;
  }

  async setScheduleActivation(activationTimestamp: string): Promise<void> {
    const slot = timestampToSlot(activationTimestamp);
    this.scheduleActivationGlobalRound = slotToGlobalRound(
      slot, this.config.scheduleSize, this.config.blocksPerBp
    );

    // Flush any open outage streaks — producers may no longer be in the new schedule
    await this.flushOutageStreaks();

    // Discard the current in-progress round — it spans the schedule transition
    // and would be evaluated against the new producer list with incomplete data
    this.roundBlocks.clear();
    this.roundForks = [];
    this.roundStartTimestamp = '';
    this.roundEndTimestamp = '';
    this.firstRoundIsPartial = true;

    await this.db.setState(
      this.config.chain, this.config.network,
      `${this.statePrefix}schedule_activation_global_round`,
      String(this.scheduleActivationGlobalRound)
    );
    log.info(
      {
        chain: this.config.chain, network: this.config.network,
        activationTimestamp,
        activationGlobalRound: this.scheduleActivationGlobalRound,
      },
      'Schedule activation point set'
    );
  }

  async processBlock(block: BlockRecord): Promise<RoundResult | null> {
    const { producer, block_num, timestamp } = block;
    const slot = timestampToSlot(timestamp);
    const globalRound = slotToGlobalRound(slot, this.config.scheduleSize, this.config.blocksPerBp);

    if (this.currentGlobalRound === -1) {
      this.currentGlobalRound = globalRound;
      this.roundStartTimestamp = timestamp;
      this.roundBlocks.clear();
    this.roundForks = [];
    }

    if (globalRound > this.currentGlobalRound) {
      let result: RoundResult | null = null;

      if (this.firstRoundIsPartial) {
        // Discard the first round — we joined mid-round and don't have complete data
        log.info(
          {
            chain: this.config.chain,
            network: this.config.network,
            discardedRound: this.currentGlobalRound,
            observedProducers: this.roundBlocks.size,
          },
          'Discarding partial round (joined mid-round)'
        );
        this.firstRoundIsPartial = false;
      } else {
        result = await this.evaluateRound();
      }

      this.currentGlobalRound = globalRound;
      this.roundBlocks.clear();
    this.roundForks = [];
      this.roundStartTimestamp = timestamp;
      this.roundEndTimestamp = '';

      await this.db.setState(this.config.chain, this.config.network, `${this.statePrefix}current_global_round`, String(this.currentGlobalRound));

      this.addBlock(producer, block_num);
      this.roundEndTimestamp = timestamp;
      this.lastBlockNum = block_num;
      // Always flush last_block at round boundaries for consistency
      await this.db.setState(this.config.chain, this.config.network, `${this.statePrefix}last_block`, String(block_num));
      this.blocksSinceLastStateWrite = 0;

      return result;
    }

    this.addBlock(producer, block_num);
    this.roundEndTimestamp = timestamp;
    this.lastBlockNum = block_num;
    this.blocksSinceLastStateWrite++;
    if (this.blocksSinceLastStateWrite >= this.stateWriteInterval) {
      await this.db.setState(this.config.chain, this.config.network, `${this.statePrefix}last_block`, String(block_num));
      this.blocksSinceLastStateWrite = 0;
    }

    return null;
  }

  recordFork(blockNumber: number, originalProducer: string, replacementProducer: string): void {
    this.roundForks.push({ blockNumber, originalProducer, replacementProducer });
  }

  private addBlock(producer: string, blockNum: number): void {
    const existing = this.roundBlocks.get(producer);
    if (existing) {
      existing.count++;
      existing.lastBlock = blockNum;
    } else {
      this.roundBlocks.set(producer, { count: 1, firstBlock: blockNum, lastBlock: blockNum });
    }
  }

  private async evaluateRound(): Promise<RoundResult> {
    const producers = this.schedule.producers;
    const producerResults: ProducerRoundResult[] = [];
    let producersProduced = 0;
    let producersMissed = 0;
    const displayRound = this.currentGlobalRound - this.scheduleActivationGlobalRound;

    if (producers.length === 0) {
      return {
        roundNumber: displayRound,
        scheduleVersion: this.schedule.version,
        timestampStart: this.roundStartTimestamp,
        timestampEnd: this.roundEndTimestamp,
        producerResults: [],
        producersProduced: 0,
        producersMissed: 0,
        forks: [],
      };
    }

    for (let i = 0; i < producers.length; i++) {
      const producer = producers[i];
      const data = this.roundBlocks.get(producer);
      const blocksProduced = data ? Math.min(data.count, this.config.blocksPerBp) : 0;
      const blocksMissed = this.config.blocksPerBp - blocksProduced;

      producerResults.push({
        producer,
        position: i,
        blocksExpected: this.config.blocksPerBp,
        blocksProduced,
        blocksMissed,
        firstBlock: data?.firstBlock ?? null,
        lastBlock: data?.lastBlock ?? null,
      });

      if (blocksProduced > 0) {
        producersProduced++;
      } else {
        producersMissed++;
      }
    }

    const roundResult: RoundResult = {
      roundNumber: displayRound,
      scheduleVersion: this.schedule.version,
      timestampStart: this.roundStartTimestamp,
      timestampEnd: this.roundEndTimestamp,
      producerResults,
      producersProduced,
      producersMissed,
      forks: [...this.roundForks],
    };

    await this.persistRound(roundResult);

    log.info(
      {
        chain: this.config.chain,
        network: this.config.network,
        scheduleRound: displayRound,
        globalRound: this.currentGlobalRound,
        produced: producersProduced,
        missed: producersMissed,
        scheduleVersion: this.schedule.version,
      },
      'Round evaluated'
    );

    return roundResult;
  }

  private async persistRound(result: RoundResult): Promise<void> {
    if (result.producerResults.length === 0) return;

    if (this.batchInserts) {
      await this.persistRoundBatch(result);
    } else {
      await this.persistRoundIndividual(result);
    }
  }

  private async persistRoundIndividual(result: RoundResult): Promise<void> {
    const roundId = await this.db.insertRound({
      chain: this.config.chain,
      network: this.config.network,
      round_number: result.roundNumber,
      schedule_version: result.scheduleVersion,
      timestamp_start: result.timestampStart,
      timestamp_end: result.timestampEnd,
      producers_scheduled: result.producerResults.length,
      producers_produced: result.producersProduced,
      producers_missed: result.producersMissed,
    });

    const day = result.timestampStart.substring(0, 10);

    for (const pr of result.producerResults) {
      await this.db.insertRoundProducer({
        round_id: roundId,
        producer: pr.producer,
        position: pr.position,
        blocks_expected: pr.blocksExpected,
        blocks_produced: pr.blocksProduced,
        blocks_missed: pr.blocksMissed,
        first_block: pr.firstBlock,
        last_block: pr.lastBlock,
      });

      if (pr.blocksProduced === 0) {
        await this.db.insertMissedBlockEvent({
          chain: this.config.chain,
          network: this.config.network,
          producer: pr.producer,
          round_id: roundId,
          blocks_missed: pr.blocksExpected,
          block_number: null,
          timestamp: result.timestampEnd,
        });
      } else if (pr.blocksMissed > 0) {
        await this.db.insertMissedBlockEvent({
          chain: this.config.chain,
          network: this.config.network,
          producer: pr.producer,
          round_id: roundId,
          blocks_missed: pr.blocksMissed,
          block_number: pr.lastBlock,
          timestamp: result.timestampEnd,
        });
      }

      // Update daily summary
      await this.db.upsertProducerStatsDaily({
        chain: this.config.chain,
        network: this.config.network,
        day,
        producer: pr.producer,
        blocks_expected: pr.blocksExpected,
        blocks_produced: pr.blocksProduced,
        blocks_missed: pr.blocksMissed,
        produced: pr.blocksProduced > 0,
      });

      // Track outage streaks
      this.updateOutageStreak(pr, result);
    }

    // Update round counts daily
    await this.db.upsertRoundCountsDaily({
      chain: this.config.chain,
      network: this.config.network,
      day,
      perfect: result.producersMissed === 0,
    });

    await this.checkFirstCompleteRound(result.roundNumber);
  }

  private async persistRoundBatch(result: RoundResult): Promise<void> {
    await this.db.transaction(async (client) => {
      const roundId = await this.db.insertRound({
        chain: this.config.chain,
        network: this.config.network,
        round_number: result.roundNumber,
        schedule_version: result.scheduleVersion,
        timestamp_start: result.timestampStart,
        timestamp_end: result.timestampEnd,
        producers_scheduled: result.producerResults.length,
        producers_produced: result.producersProduced,
        producers_missed: result.producersMissed,
      }, client);

      if (roundId === null) return; // duplicate round, skip

      await this.db.insertRoundProducersBatch(
        result.producerResults.map(pr => ({
          round_id: roundId,
          producer: pr.producer,
          position: pr.position,
          blocks_expected: pr.blocksExpected,
          blocks_produced: pr.blocksProduced,
          blocks_missed: pr.blocksMissed,
          first_block: pr.firstBlock,
          last_block: pr.lastBlock,
        })),
        client
      );

      const missedRows = result.producerResults
        .filter(pr => pr.blocksMissed > 0)
        .map(pr => ({
          chain: this.config.chain,
          network: this.config.network,
          producer: pr.producer,
          round_id: roundId,
          blocks_missed: pr.blocksProduced === 0 ? pr.blocksExpected : pr.blocksMissed,
          block_number: pr.blocksProduced === 0 ? null : pr.lastBlock,
          timestamp: result.timestampEnd,
        }));
      await this.db.insertMissedBlockEventsBatch(missedRows, client);

      const firstRound = await this.db.getState(
        this.config.chain, this.config.network,
        `${this.statePrefix}first_complete_round`, client
      );
      if (!firstRound) {
        await this.db.setState(
          this.config.chain, this.config.network,
          `${this.statePrefix}first_complete_round`,
          String(result.roundNumber), client
        );
      }
    });
  }

  private updateOutageStreak(pr: ProducerRoundResult, result: RoundResult): void {
    if (this.batchInserts) return; // catchup writer doesn't track live streaks

    const streak = this.outageStreaks.get(pr.producer);

    if (pr.blocksProduced === 0) {
      // Extend or start an outage streak
      if (streak) {
        streak.count++;
        streak.endRound = result.roundNumber;
        streak.endTimestamp = result.timestampEnd;
      } else {
        this.outageStreaks.set(pr.producer, {
          count: 1,
          startRound: result.roundNumber,
          startTimestamp: result.timestampStart,
          endRound: result.roundNumber,
          endTimestamp: result.timestampEnd,
          scheduleVersion: result.scheduleVersion,
        });
      }
    } else if (streak) {
      // Producer came back — close the outage event
      this.db.insertOutageEvent({
        chain: this.config.chain,
        network: this.config.network,
        producer: pr.producer,
        rounds_count: streak.count,
        start_round_number: streak.startRound,
        end_round_number: streak.endRound,
        schedule_version: streak.scheduleVersion,
        timestamp_start: streak.startTimestamp,
        timestamp_end: streak.endTimestamp,
      }).catch(err => {
        log.error({ err, producer: pr.producer }, 'Failed to insert outage event');
      });
      this.outageStreaks.delete(pr.producer);
    }
  }

  /** Flush any open outage streaks (e.g. on schedule change). */
  async flushOutageStreaks(): Promise<void> {
    for (const [producer, streak] of this.outageStreaks) {
      await this.db.insertOutageEvent({
        chain: this.config.chain,
        network: this.config.network,
        producer,
        rounds_count: streak.count,
        start_round_number: streak.startRound,
        end_round_number: streak.endRound,
        schedule_version: streak.scheduleVersion,
        timestamp_start: streak.startTimestamp,
        timestamp_end: streak.endTimestamp,
      });
    }
    this.outageStreaks.clear();
  }

  private async checkFirstCompleteRound(roundNumber: number): Promise<void> {
    const firstRound = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}first_complete_round`);
    if (!firstRound) {
      await this.db.setState(
        this.config.chain, this.config.network,
        `${this.statePrefix}first_complete_round`,
        String(roundNumber)
      );
    }
  }
}
