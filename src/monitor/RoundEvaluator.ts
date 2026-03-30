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

export interface RoundResult {
  roundNumber: number;
  scheduleVersion: number;
  timestampStart: string;
  timestampEnd: string;
  producerResults: ProducerRoundResult[];
  producersProduced: number;
  producersMissed: number;
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

/**
 * Calculate the slot number from a block timestamp.
 * Antelope uses 0.5s slots from epoch 2000-01-01T00:00:00.
 */
function timestampToSlot(timestamp: string): number {
  const ms = new Date(timestamp).getTime();
  return Math.floor((ms - ANTELOPE_EPOCH_MS) / 500);
}

/**
 * Calculate which round a slot belongs to.
 * round = floor(slot / (scheduleSize * blocksPerBp))
 */
function slotToRound(slot: number, scheduleSize: number, blocksPerBp: number): number {
  return Math.floor(slot / (scheduleSize * blocksPerBp));
}

/**
 * Calculate which producer position a slot maps to.
 * position = floor((slot % (scheduleSize * blocksPerBp)) / blocksPerBp)
 */
function slotToProducerPosition(slot: number, scheduleSize: number, blocksPerBp: number): number {
  const slotsPerRound = scheduleSize * blocksPerBp;
  return Math.floor((slot % slotsPerRound) / blocksPerBp);
}

export class RoundEvaluator {
  private config: ChainConfig;
  private db: Database;
  private schedule: ScheduleTracker;
  private currentRound: number = -1;
  private lastBlockNum: number = 0;

  // Accumulate blocks per-producer for the current round
  private roundBlocks: Map<string, { count: number; firstBlock: number; lastBlock: number }> = new Map();
  private roundStartTimestamp: string = '';
  private roundEndTimestamp: string = '';

  constructor(config: ChainConfig, db: Database, schedule: ScheduleTracker) {
    this.config = config;
    this.db = db;
    this.schedule = schedule;

    // Restore state
    const savedBlock = this.db.getState(config.chain, config.network, 'last_block');
    this.lastBlockNum = savedBlock ? parseInt(savedBlock, 10) : 0;

    const savedRound = this.db.getState(config.chain, config.network, 'current_round');
    this.currentRound = savedRound ? parseInt(savedRound, 10) : -1;

    log.info(
      { chain: config.chain, network: config.network, round: this.currentRound, lastBlock: this.lastBlockNum },
      'RoundEvaluator initialized'
    );
  }

  get round(): number {
    return this.currentRound;
  }

  setRound(round: number): void {
    this.currentRound = round;
    this.db.setState(
      this.config.chain,
      this.config.network,
      'current_round',
      String(round)
    );
    log.info(
      { chain: this.config.chain, network: this.config.network, round },
      'Round number set from external source'
    );
  }

  processBlock(block: BlockRecord): RoundResult | null {
    const { producer, block_num, timestamp } = block;
    const slot = timestampToSlot(timestamp);
    const blockRound = slotToRound(slot, this.config.scheduleSize, this.config.blocksPerBp);

    // First block ever — initialize
    if (this.currentRound === -1) {
      this.currentRound = blockRound;
      this.roundStartTimestamp = timestamp;
      this.roundBlocks.clear();
    }

    // Did we cross into a new round?
    if (blockRound > this.currentRound) {
      // Evaluate the completed round
      const result = this.evaluateRound();

      // Advance to new round
      this.currentRound = blockRound;
      this.roundBlocks.clear();
      this.roundStartTimestamp = timestamp;
      this.roundEndTimestamp = '';

      // Save state
      this.db.setState(this.config.chain, this.config.network, 'current_round', String(this.currentRound));

      // Add this block to the new round
      this.addBlock(producer, block_num);
      this.roundEndTimestamp = timestamp;
      this.lastBlockNum = block_num;
      this.db.setState(this.config.chain, this.config.network, 'last_block', String(block_num));

      return result;
    }

    // Same round — accumulate
    this.addBlock(producer, block_num);
    this.roundEndTimestamp = timestamp;
    this.lastBlockNum = block_num;
    this.db.setState(this.config.chain, this.config.network, 'last_block', String(block_num));

    return null;
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

  private evaluateRound(): RoundResult {
    const producers = this.schedule.producers;
    const producerResults: ProducerRoundResult[] = [];
    let producersProduced = 0;
    let producersMissed = 0;

    if (producers.length === 0) {
      // No schedule — can't evaluate
      return {
        roundNumber: this.currentRound,
        scheduleVersion: this.schedule.version,
        timestampStart: this.roundStartTimestamp,
        timestampEnd: this.roundEndTimestamp,
        producerResults: [],
        producersProduced: 0,
        producersMissed: 0,
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
      roundNumber: this.currentRound,
      scheduleVersion: this.schedule.version,
      timestampStart: this.roundStartTimestamp,
      timestampEnd: this.roundEndTimestamp,
      producerResults,
      producersProduced,
      producersMissed,
    };

    // Persist
    this.persistRound(roundResult);

    // Track first complete round
    const firstRound = this.db.getState(this.config.chain, this.config.network, 'first_complete_round');
    if (!firstRound) {
      this.db.setState(
        this.config.chain,
        this.config.network,
        'first_complete_round',
        String(this.currentRound)
      );
    }

    log.info(
      {
        chain: this.config.chain,
        network: this.config.network,
        round: this.currentRound,
        produced: producersProduced,
        missed: producersMissed,
        scheduleVersion: this.schedule.version,
      },
      'Round evaluated'
    );

    return roundResult;
  }

  private persistRound(result: RoundResult): void {
    if (result.producerResults.length === 0) return;

    this.db.transaction(() => {
      const roundId = this.db.insertRound({
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
        this.db.insertRoundProducer({
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
          this.db.insertMissedBlockEvent({
            chain: this.config.chain,
            network: this.config.network,
            producer: pr.producer,
            round_id: roundId,
            blocks_missed: pr.blocksExpected,
            block_number: null,
            timestamp: result.timestampEnd,
          });
        } else if (pr.blocksMissed > 0) {
          this.db.insertMissedBlockEvent({
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
    });
  }
}
