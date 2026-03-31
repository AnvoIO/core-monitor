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

export class RoundEvaluator {
  private config: ChainConfig;
  private db: Database;
  private schedule: ScheduleTracker;
  private statePrefix: string;
  private currentGlobalRound: number = -1;
  private scheduleActivationGlobalRound: number = 0;
  private lastBlockNum: number = 0;
  private firstRoundIsPartial: boolean = true;

  private roundBlocks: Map<string, { count: number; firstBlock: number; lastBlock: number }> = new Map();
  private roundForks: RoundFork[] = [];
  private roundStartTimestamp: string = '';
  private roundEndTimestamp: string = '';

  constructor(config: ChainConfig, db: Database, schedule: ScheduleTracker, statePrefix: string = '') {
    this.config = config;
    this.db = db;
    this.schedule = schedule;
    this.statePrefix = statePrefix;
  }

  async init(): Promise<void> {
    const savedBlock = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}last_block`);
    this.lastBlockNum = savedBlock ? parseInt(savedBlock, 10) : 0;

    const savedGlobalRound = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}current_global_round`);
    this.currentGlobalRound = savedGlobalRound ? parseInt(savedGlobalRound, 10) : -1;
    // Only discard the first round on fresh start (no saved state).
    // When resuming from last_block, we pick up exactly where we left off.
    this.firstRoundIsPartial = !savedGlobalRound;

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
      await this.db.setState(this.config.chain, this.config.network, `${this.statePrefix}last_block`, String(block_num));

      return result;
    }

    this.addBlock(producer, block_num);
    this.roundEndTimestamp = timestamp;
    this.lastBlockNum = block_num;
    await this.db.setState(this.config.chain, this.config.network, `${this.statePrefix}last_block`, String(block_num));

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

    const firstRound = await this.db.getState(this.config.chain, this.config.network, `${this.statePrefix}first_complete_round`);
    if (!firstRound) {
      await this.db.setState(
        this.config.chain, this.config.network,
        `${this.statePrefix}first_complete_round`,
        String(displayRound)
      );
    }

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
    }
  }
}
