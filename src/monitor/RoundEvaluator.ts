import { logger } from '../utils/logger.js';
import { Database } from '../store/Database.js';
import { ScheduleTracker } from './ScheduleTracker.js';
import type { ChainConfig } from '../config.js';

const log = logger.child({ module: 'RoundEvaluator' });

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

export class RoundEvaluator {
  private config: ChainConfig;
  private db: Database;
  private schedule: ScheduleTracker;
  private currentRound: number;
  private lastScheduleVersion: number;
  private roundBlocks: Map<string, BlockRecord[]> = new Map();
  private roundStartTimestamp: string = '';
  private lastProducer: string = '';
  private lastBlockNum: number = 0;
  private producerBlockCount: number = 0;
  private roundProducerIndex: number = 0;
  private roundIsComplete: boolean = false;

  constructor(config: ChainConfig, db: Database, schedule: ScheduleTracker) {
    this.config = config;
    this.db = db;
    this.schedule = schedule;

    // Restore round number and schedule version from state
    const savedRound = this.db.getState(config.chain, config.network, 'current_round');
    this.currentRound = savedRound ? parseInt(savedRound, 10) : 0;

    const savedScheduleVersion = this.db.getState(config.chain, config.network, 'last_schedule_version');
    this.lastScheduleVersion = savedScheduleVersion ? parseInt(savedScheduleVersion, 10) : 0;

    const savedBlock = this.db.getState(config.chain, config.network, 'last_block');
    this.lastBlockNum = savedBlock ? parseInt(savedBlock, 10) : 0;

    // If we have no prior state, we need to wait for a full round before publishing
    this.roundIsComplete = false;

    log.info(
      { chain: config.chain, network: config.network, round: this.currentRound, scheduleVersion: this.lastScheduleVersion, lastBlock: this.lastBlockNum },
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
    const { producer, block_num, timestamp, schedule_version } = block;

    // Detect schedule version change — reset round counter and mark incomplete
    if (schedule_version > this.lastScheduleVersion && this.lastScheduleVersion > 0) {
      log.info(
        {
          chain: this.config.chain,
          network: this.config.network,
          oldVersion: this.lastScheduleVersion,
          newVersion: schedule_version,
          roundsInPreviousSchedule: this.currentRound,
        },
        'Schedule version changed, resetting round counter'
      );
      this.currentRound = 0;
      this.roundBlocks.clear();
      this.roundStartTimestamp = '';
      this.lastProducer = '';
      this.producerBlockCount = 0;
      this.roundProducerIndex = 0;
      this.roundIsComplete = false;
    }
    this.lastScheduleVersion = schedule_version;
    this.db.setState(
      this.config.chain,
      this.config.network,
      'last_schedule_version',
      String(schedule_version)
    );

    // Detect round boundary: producer changed and the new producer's position
    // in the schedule is <= the previous producer's position, meaning we've
    // wrapped around, OR the first block of a new producer's slot
    if (producer !== this.lastProducer) {
      if (this.lastProducer !== '') {
        // Record the completed producer's blocks
        this.recordProducerBlocks(this.lastProducer, this.producerBlockCount);
      }

      this.producerBlockCount = 0;

      // Check if this is a new round
      const isNewRound = this.isRoundBoundary(producer);

      if (isNewRound && this.roundBlocks.size > 0) {
        let result: RoundResult | null = null;

        if (this.roundIsComplete) {
          // Only evaluate and persist complete rounds
          result = this.evaluateRound(timestamp);
          this.currentRound++;
          this.db.setState(
            this.config.chain,
            this.config.network,
            'current_round',
            String(this.currentRound)
          );
        } else {
          // First round boundary after startup or schedule change — discard the partial round
          log.info(
            {
              chain: this.config.chain,
              network: this.config.network,
              discardedProducers: this.roundBlocks.size,
              scheduleSize: this.schedule.producers.length,
            },
            'Discarding partial round (joined mid-schedule)'
          );
          // The NEXT round will be our first complete one
          this.roundIsComplete = true;
        }

        // Start new round
        this.roundBlocks.clear();
        this.roundStartTimestamp = timestamp;
        this.roundProducerIndex = 0;

        this.lastProducer = producer;
        this.producerBlockCount = 1;
        this.lastBlockNum = block_num;

        // Add this block to the new round
        this.addBlockToRound(block);

        this.db.setState(
          this.config.chain,
          this.config.network,
          'last_block',
          String(block_num)
        );

        return result;
      }

      if (this.roundBlocks.size === 0) {
        this.roundStartTimestamp = timestamp;
      }

      this.roundProducerIndex++;
    }

    this.lastProducer = producer;
    this.producerBlockCount++;
    this.lastBlockNum = block_num;
    this.addBlockToRound(block);

    this.db.setState(
      this.config.chain,
      this.config.network,
      'last_block',
      String(block_num)
    );

    return null;
  }

  private isRoundBoundary(newProducer: string): boolean {
    if (!this.schedule.schedule) return false;

    const producers = this.schedule.producers;
    const newPos = producers.indexOf(newProducer);
    const lastPos = producers.indexOf(this.lastProducer);

    // If either producer is not in the schedule, can't determine
    if (newPos === -1 || lastPos === -1) return false;

    // A new round starts when the producer position wraps around
    // (new position is at or before the first producer we saw this round)
    if (newPos <= lastPos && this.roundBlocks.size >= producers.length) {
      return true;
    }

    // Also detect if we've seen enough unique producers for a full round
    if (newPos === 0 && this.roundBlocks.size > 0) {
      return true;
    }

    return false;
  }

  private addBlockToRound(block: BlockRecord): void {
    const existing = this.roundBlocks.get(block.producer) || [];
    existing.push(block);
    this.roundBlocks.set(block.producer, existing);
  }

  private recordProducerBlocks(producer: string, count: number): void {
    // This is tracked via roundBlocks map, nothing extra needed here
  }

  private evaluateRound(endTimestamp: string): RoundResult {
    const producers = this.schedule.producers;
    const producerResults: ProducerRoundResult[] = [];
    let producersProduced = 0;
    let producersMissed = 0;

    for (let i = 0; i < producers.length; i++) {
      const producer = producers[i];
      const blocks = this.roundBlocks.get(producer) || [];
      const blocksProduced = blocks.length;
      const blocksMissed = Math.max(0, this.config.blocksPerBp - blocksProduced);

      const result: ProducerRoundResult = {
        producer,
        position: i,
        blocksExpected: this.config.blocksPerBp,
        blocksProduced,
        blocksMissed,
        firstBlock: blocks.length > 0 ? blocks[0].block_num : null,
        lastBlock: blocks.length > 0 ? blocks[blocks.length - 1].block_num : null,
      };

      producerResults.push(result);

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
      timestampEnd: endTimestamp,
      producerResults,
      producersProduced,
      producersMissed,
    };

    // Persist to database
    this.persistRound(roundResult);

    // Track first complete round for the dashboard
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

        // Record missed block events for producers that missed their entire slot
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
