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
  private ship: ShipClient;
  private schedule: ScheduleTracker;
  private roundEvaluator: RoundEvaluator;
  private log: ReturnType<typeof chainLogger>;
  private blocksProcessed: number = 0;
  private lastLogTime: number = 0;
  private lastBlockId: string = '';
  private lastBlockProducer: string = '';

  constructor(config: ChainConfig, db: Database) {
    super();
    this.config = config;
    this.db = db;
    this.log = chainLogger(config.chain, config.network);

    this.schedule = new ScheduleTracker(config.chain, config.network, db);
    this.roundEvaluator = new RoundEvaluator(config, db, this.schedule);

    // Determine start block — resume from last processed or start from head
    const lastBlock = db.getState(config.chain, config.network, 'last_block');
    const startBlock = lastBlock ? parseInt(lastBlock, 10) + 1 : 0;

    this.ship = new ShipClient({
      url: config.shipUrl,
      failoverUrl: config.shipFailoverUrl,
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

      // Validate chain ID
      if (status.chain_id !== config.chainId) {
        this.log.error(
          { expected: config.chainId, got: status.chain_id },
          'Chain ID mismatch — wrong endpoint?'
        );
        this.ship.disconnect();
      }
    });

    this.ship.on('block', (result: ShipResult) => {
      this.processBlock(result);
    });

    this.ship.on('max_reconnects', () => {
      this.log.error('Max reconnection attempts reached, monitor stopping');
      this.emit('fatal', new Error('Max reconnection attempts reached'));
    });
  }

  async start(): Promise<void> {
    this.log.info(
      { shipUrl: this.config.shipUrl },
      'Starting chain monitor'
    );

    // Bootstrap the schedule via RPC if we don't have one
    if (!this.schedule.schedule) {
      await this.fetchSchedule();
    }

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

        // Try to get schedule activation timestamp from Hyperion
        let activationTimestamp: string | null = null;
        if (this.config.hyperionUrl) {
          activationTimestamp = await this.fetchScheduleActivation(version);
        }

        const timestamp = activationTimestamp || new Date().toISOString();

        this.schedule.updateSchedule(
          version,
          active.producers.map((p: any) => ({
            producer_name: p.producer_name,
            block_signing_key: p.block_signing_key || p.authority?.[1]?.keys?.[0]?.key || 'unknown',
          })),
          0,
          timestamp
        );

        // Calculate accurate round number if we have the activation timestamp
        if (activationTimestamp) {
          const activationTime = new Date(activationTimestamp).getTime();
          const now = Date.now();
          const elapsedMs = now - activationTime;
          const blocksPerRound = this.config.scheduleSize * this.config.blocksPerBp;
          const roundDurationMs = blocksPerRound * this.config.blockTimeMs;
          const currentRound = Math.floor(elapsedMs / roundDurationMs);

          this.roundEvaluator.setRound(currentRound);
          this.log.info(
            {
              version,
              producers: active.producers.length,
              activationTimestamp,
              calculatedRound: currentRound,
            },
            'Schedule bootstrapped with accurate round number from Hyperion'
          );
        } else {
          this.log.info(
            { version, producers: active.producers.length },
            'Schedule bootstrapped from RPC (no Hyperion data for round calculation)'
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
    this.ship.disconnect();
  }

  private processBlock(result: ShipResult): void {
    if (!result.this_block || !result.block) return;

    const blockNum = result.this_block.block_num;
    const blockId = result.this_block.block_id;
    const block = result.block;

    // Fork detection: if prev_block doesn't match our last seen block ID,
    // the chain has forked and blocks were replaced
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

        this.db.insertForkEvent({
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

    // Check for schedule change via new_producers in block header
    if (block.new_producers) {
      const changed = this.schedule.updateSchedule(
        block.new_producers.version,
        block.new_producers.producers,
        blockNum,
        block.timestamp
      );
      if (changed) {
        this.emit('schedule_change', {
          chain: this.config.chain,
          network: this.config.network,
          version: block.new_producers.version,
          producers: block.new_producers.producers.map((p) => p.producer_name),
          blockNum,
          timestamp: block.timestamp,
        });
      }
    }

    // Check for schedule changes in traces (regproducer / unregprod actions)
    for (const trace of result.traces) {
      for (const actionTrace of trace.action_traces) {
        const { account, name } = actionTrace.act;
        if (account === 'eosio' && (name === 'regproducer' || name === 'unregprod')) {
          // Extract producer name from action data
          const producerName = actionTrace.act.authorization?.[0]?.actor
            || String(actionTrace.act.data?.producer || '');

          this.db.insertProducerEvent({
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

    // Feed block to round evaluator
    const blockRecord: BlockRecord = {
      block_num: blockNum,
      producer: block.producer,
      timestamp: block.timestamp,
      schedule_version: block.schedule_version,
    };

    const roundResult = this.roundEvaluator.processBlock(blockRecord);

    if (roundResult) {
      this.emitRoundResult(roundResult);
    }

    // Periodic progress logging
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
    // Emit events for alerting
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
