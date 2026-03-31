import { EventEmitter } from 'events';
import { ShipClient } from '../ship/ShipClient.js';
import { ScheduleTracker } from './ScheduleTracker.js';
import { RoundEvaluator, type RoundResult, type BlockRecord } from './RoundEvaluator.js';
import { Database } from '../store/Database.js';
import { chainLogger } from '../utils/logger.js';
import type { ChainConfig } from '../config.js';
import type { ShipResult, ShipGetStatusResult } from '../ship/types.js';

export class ChainMonitor extends EventEmitter {
  private config: ChainConfig;
  private db: Database;
  private ship!: ShipClient;
  private schedule: ScheduleTracker;
  private roundEvaluator: RoundEvaluator;
  private log: ReturnType<typeof chainLogger>;
  private blocksProcessed: number = 0;
  private lastLogTime: number = 0;
  private lastBlockId: string = '';
  private lastBlockProducer: string = '';
  private pendingSchedule: { version: number; producers: Array<{ producer_name: string; block_signing_key: string }> } | null = null;

  constructor(config: ChainConfig, db: Database) {
    super();
    this.config = config;
    this.db = db;
    this.log = chainLogger(config.chain, config.network);
    this.schedule = new ScheduleTracker(config.chain, config.network, db);
    this.roundEvaluator = new RoundEvaluator(config, db, this.schedule);
  }

  async start(): Promise<void> {
    this.log.info({ shipUrl: this.config.shipUrl }, 'Starting chain monitor');

    // Initialize components (async state restore)
    await this.schedule.init();
    await this.roundEvaluator.init();

    // Bootstrap schedule if needed
    if (!this.schedule.schedule) {
      await this.fetchSchedule();
    }

    // Determine start block
    const lastBlock = await this.db.getState(this.config.chain, this.config.network, 'last_block');
    const startBlock = lastBlock ? parseInt(lastBlock, 10) + 1 : 0;

    this.ship = new ShipClient({
      url: this.config.shipUrl,
      failoverUrl: this.config.shipFailoverUrl,
      startBlock,
      fetchBlock: true,
      fetchTraces: true,
      fetchDeltas: true,
    });

    this.ship.on('status', (status: ShipGetStatusResult) => {
      this.log.info(
        { head: status.head.block_num, chainId: status.chain_id },
        'Connected to chain'
      );
      if (status.chain_id !== this.config.chainId) {
        this.log.error(
          { expected: this.config.chainId, got: status.chain_id },
          'Chain ID mismatch — wrong endpoint?'
        );
        this.ship.disconnect();
      }
    });

    this.ship.on('block', (result: ShipResult) => {
      this.processBlock(result).catch(err => {
        this.log.error({ err }, 'Error processing block');
      });
    });

    this.ship.on('max_reconnects', () => {
      this.log.error('Max reconnection attempts reached, monitor stopping');
      this.emit('fatal', new Error('Max reconnection attempts reached'));
    });

    await this.ship.connect();
  }

  private async fetchSchedule(): Promise<void> {
    try {
      this.log.info('Fetching producer schedule via RPC');
      const response = await fetch(`${this.config.apiUrl}/v1/chain/get_producer_schedule`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });

      if (!response.ok) {
        this.log.warn({ status: response.status }, 'Failed to fetch schedule via RPC');
        return;
      }

      const data = await response.json() as any;
      const active = data.active;

      if (active && active.producers && active.producers.length > 0) {
        const version = active.version;
        const producers = active.producers.map((p: any) => ({
          producer_name: p.producer_name,
          block_signing_key: p.block_signing_key || p.authority?.[1]?.keys?.[0]?.key || 'unknown',
        }));

        let activationTimestamp: string | null = null;
        if (this.config.hyperionUrl) {
          activationTimestamp = await this.fetchScheduleActivation(version);
        }

        const timestamp = activationTimestamp || new Date().toISOString();

        await this.schedule.updateSchedule(version, producers, 0, timestamp);

        if (activationTimestamp) {
          await this.roundEvaluator.setScheduleActivation(activationTimestamp);
          this.log.info(
            { version, producers: producers.length, activationTimestamp },
            'Schedule bootstrapped from RPC + Hyperion'
          );
        } else {
          this.log.info(
            { version, producers: producers.length },
            'Schedule bootstrapped from RPC (no activation timestamp)'
          );
        }
      }
    } catch (err) {
      this.log.warn({ err }, 'Failed to bootstrap schedule — will detect from SHiP blocks');
    }
  }

  private async fetchScheduleActivation(version: number): Promise<string | null> {
    try {
      const url = `${this.config.hyperionUrl}/history/get_schedule?version=${version}`;
      this.log.info({ url }, 'Fetching schedule activation from Hyperion');

      const response = await fetch(url);
      if (!response.ok) {
        this.log.warn({ status: response.status }, 'Hyperion schedule lookup failed');
        return null;
      }

      const data = await response.json() as any;
      if (data.timestamp) {
        this.log.info(
          { version, activatedAt: data.timestamp },
          'Schedule activation timestamp from Hyperion'
        );
        return data.timestamp;
      }
    } catch (err) {
      this.log.warn({ err }, 'Failed to fetch schedule activation from Hyperion');
    }
    return null;
  }

  stop(): void {
    this.log.info('Stopping chain monitor');
    if (this.ship) this.ship.disconnect();
  }

  private async processBlock(result: ShipResult): Promise<void> {
    if (!result.this_block || !result.block) return;

    const blockNum = result.this_block.block_num;
    const blockId = result.this_block.block_id;
    const block = result.block;

    // Fork detection
    if (this.lastBlockId && result.prev_block) {
      const prevBlockId = result.prev_block.block_id;
      if (prevBlockId !== this.lastBlockId) {
        const forkBlockNum = result.prev_block.block_num;
        this.log.warn(
          {
            forkAt: forkBlockNum,
            expectedPrev: this.lastBlockId.substring(0, 16),
            actualPrev: prevBlockId.substring(0, 16),
            currentBlock: blockNum,
            originalProducer: this.lastBlockProducer,
            newProducer: block.producer,
          },
          'Fork detected — block replaced'
        );

        await this.db.insertForkEvent({
          chain: this.config.chain,
          network: this.config.network,
          round_id: null,
          block_number: forkBlockNum,
          original_producer: this.lastBlockProducer,
          replacement_producer: block.producer,
          timestamp: block.timestamp,
        });

        this.emit('fork', {
          chain: this.config.chain,
          network: this.config.network,
          blockNumber: forkBlockNum,
          originalProducer: this.lastBlockProducer,
          replacementProducer: block.producer,
          timestamp: block.timestamp,
        });
      }
    }

    this.lastBlockId = blockId;
    this.lastBlockProducer = block.producer;

    // Schedule change detection
    // new_producers from block header (legacy) or header extensions (WTMSIG)
    // represents a PROPOSED schedule, not yet active. Store it as pending.
    if (block.new_producers && block.new_producers.version > this.schedule.version) {
      this.pendingSchedule = block.new_producers;
      this.log.info(
        { version: block.new_producers.version, blockNum },
        'Pending schedule detected (not yet active)'
      );
    }

    // The schedule_version in the block header tells us which schedule is
    // actually ACTIVE. When it increments, apply the pending schedule.
    if (block.schedule_version > this.schedule.version && this.pendingSchedule) {
      const changed = await this.schedule.updateSchedule(
        this.pendingSchedule.version,
        this.pendingSchedule.producers,
        blockNum,
        block.timestamp
      );
      if (changed) {
        await this.roundEvaluator.setScheduleActivation(block.timestamp);

        this.emit('schedule_change', {
          chain: this.config.chain,
          network: this.config.network,
          version: this.pendingSchedule.version,
          producers: this.pendingSchedule.producers.map((p) => p.producer_name),
          blockNum,
          timestamp: block.timestamp,
        });
      }
      this.pendingSchedule = null;
    }

    // Producer registration events
    for (const trace of result.traces) {
      for (const actionTrace of trace.action_traces) {
        const { account, name } = actionTrace.act;
        if (account === 'eosio' && (name === 'regproducer' || name === 'unregprod' || name === 'kickbp')) {
          let producerName = '';

          if (name === 'kickbp') {
            // kickbp is executed by eosio — find the kicked producer from the
            // unregprod inline action in the same transaction
            for (const inlineTrace of trace.action_traces) {
              if (inlineTrace.act.account === 'eosio' && inlineTrace.act.name === 'unregprod') {
                producerName = inlineTrace.act.authorization?.[0]?.actor || '';
                break;
              }
            }
            if (!producerName) {
              // Fallback: try to read from action data
              const actData = actionTrace.act.data || {};
              producerName = String(actData.producer || actData.producer_name || '');
            }
          } else {
            producerName = actionTrace.act.authorization?.[0]?.actor
              || String(actionTrace.act.data?.producer || '');
          }

          await this.db.insertProducerEvent({
            chain: this.config.chain,
            network: this.config.network,
            producer: producerName,
            action: name,
            block_number: blockNum,
            timestamp: block.timestamp,
          });

          this.emit('producer_action', {
            chain: this.config.chain,
            network: this.config.network,
            action: name,
            producer: producerName,
            data: actionTrace.act.data,
            blockNum,
            timestamp: block.timestamp,
          });
        }
      }
    }

    // Feed to round evaluator
    const blockRecord: BlockRecord = {
      block_num: blockNum,
      producer: block.producer,
      timestamp: block.timestamp,
      schedule_version: block.schedule_version,
    };

    const roundResult = await this.roundEvaluator.processBlock(blockRecord);

    if (roundResult) {
      this.emitRoundResult(roundResult);
    }

    // Progress logging
    this.blocksProcessed++;
    const now = Date.now();
    if (now - this.lastLogTime >= 10000) {
      const headBlock = result.head.block_num;
      const behind = headBlock - blockNum;
      this.log.info(
        {
          blockNum,
          headBlock,
          behind,
          producer: block.producer,
          round: this.roundEvaluator.round,
          scheduleVersion: block.schedule_version,
          blocksProcessed: this.blocksProcessed,
        },
        behind > 10 ? 'Catching up' : 'Streaming'
      );
      this.lastLogTime = now;
      this.blocksProcessed = 0;
    }
  }

  private emitRoundResult(result: RoundResult): void {
    const missed = result.producerResults.filter((p) => p.blocksProduced === 0);
    const partial = result.producerResults.filter(
      (p) => p.blocksProduced > 0 && p.blocksMissed > 0
    );

    if (missed.length > 0) {
      for (const m of missed) {
        this.emit('missed_round', {
          chain: this.config.chain,
          network: this.config.network,
          producer: m.producer,
          round: result.roundNumber,
          scheduleVersion: result.scheduleVersion,
          blocksMissed: m.blocksExpected,
          timestamp: result.timestampEnd,
        });
      }
    }

    if (partial.length > 0) {
      for (const p of partial) {
        this.emit('missed_blocks', {
          chain: this.config.chain,
          network: this.config.network,
          producer: p.producer,
          round: result.roundNumber,
          scheduleVersion: result.scheduleVersion,
          blocksProduced: p.blocksProduced,
          blocksMissed: p.blocksMissed,
          blocksExpected: p.blocksExpected,
          timestamp: result.timestampEnd,
        });
      }
    }

    this.emit('round_complete', {
      chain: this.config.chain,
      network: this.config.network,
      round: result.roundNumber,
      producersProduced: result.producersProduced,
      producersMissed: result.producersMissed,
      scheduleVersion: result.scheduleVersion,
      timestamp: result.timestampEnd,
    });
  }
}
