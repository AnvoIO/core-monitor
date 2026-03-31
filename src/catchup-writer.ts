/**
 * Catchup Writer — backfills historical data from a full-history SHiP node.
 *
 * Connects to SHiP, streams blocks from genesis (or configured start),
 * evaluates rounds, and writes directly to PostgreSQL. Automatically
 * stops when it reaches the earliest block processed by the live writer.
 *
 * Usage:
 *   node dist/catchup-writer.js
 *
 * Environment:
 *   CATCHUP_SHIP_URL — SHiP endpoint for full history node (required)
 *   CATCHUP_START_BLOCK — start block (default: 1)
 *   POSTGRES_URL — PostgreSQL connection string (required)
 *   CHAINS — chain config (same as live writer)
 */

import { loadConfig } from './config.js';
import { Database } from './store/Database.js';
import { ShipClient } from './ship/ShipClient.js';
import { ScheduleTracker } from './monitor/ScheduleTracker.js';
import { RoundEvaluator, timestampToSlot, type BlockRecord } from './monitor/RoundEvaluator.js';
import { logger } from './utils/logger.js';
import type { ChainConfig } from './config.js';
import type { ShipResult, ShipGetStatusResult } from './ship/types.js';
import { Name, Serializer } from '@wharfkit/antelope';

const log = logger.child({ module: 'catchup' });

const PROGRESS_INTERVAL = 10000;

interface CatchupChain {
  config: ChainConfig;
  shipUrl: string;
  ship: ShipClient;
  schedule: ScheduleTracker;
  evaluator: RoundEvaluator;
  db: Database;
  blocksProcessed: number;
  headBlock: number;
  currentBlock: number;
  endBlock: number;
  startTime: number;
  lastProgressTime: number;
  pendingSchedule: any;
  processingBlock: Promise<void>;
}

async function findLiveWriterStartBlock(db: Database, chain: string, network: string): Promise<number> {
  // Find the earliest round the live writer has recorded
  const earliest = await db.getEarliestRounds(chain, network, 1);
  if (earliest.length > 0) {
    // The timestamp_start of the earliest round tells us approximately
    // where the live writer started. Convert to a block number estimate.
    const ts = earliest[0].timestamp_start;
    log.info({ chain, network, earliestRound: earliest[0].round_number, timestamp: ts },
      'Found live writer start point');

    // We can't convert timestamp to exact block number without the chain,
    // but the last_block state gives us the live writer's current position
    const lastBlock = await db.getState(chain, network, 'last_block');
    if (lastBlock) {
      // Use a block before the earliest round as our end point
      // The earliest round's blocks would be ~252 blocks before the round was recorded
      return parseInt(lastBlock, 10) - 1000; // safety margin
    }
  }

  // No live data yet — catch up to HEAD
  return 0;
}

async function startCatchup(chainConfig: ChainConfig, db: Database): Promise<void> {
  const shipUrl = process.env[`${chainConfig.id.toUpperCase().replace(/[^A-Z0-9]/g, '_')}_CATCHUP_SHIP_URL`]
    || process.env.CATCHUP_SHIP_URL;

  if (!shipUrl) {
    log.warn({ chain: chainConfig.id }, 'No CATCHUP_SHIP_URL configured, skipping');
    return;
  }

  const startBlock = parseInt(process.env.CATCHUP_START_BLOCK || '1', 10);
  const endBlock = await findLiveWriterStartBlock(db, chainConfig.chain, chainConfig.network);

  if (endBlock > 0 && startBlock >= endBlock) {
    log.info({ chain: chainConfig.id, startBlock, endBlock }, 'Already caught up');
    return;
  }

  log.info({
    chain: chainConfig.id,
    shipUrl,
    startBlock,
    endBlock: endBlock || 'until live writer start',
  }, 'Starting catchup');

  const schedule = new ScheduleTracker(chainConfig.chain, chainConfig.network, db);
  await schedule.init();
  const evaluator = new RoundEvaluator(chainConfig, db, schedule);
  await evaluator.init();

  // Bootstrap schedule from RPC
  try {
    const res = await fetch(`${chainConfig.apiUrl}/v1/chain/get_producer_schedule`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
    });
    if (res.ok) {
      const data = await res.json() as any;
      if (data.active?.producers) {
        await schedule.updateSchedule(
          data.active.version,
          data.active.producers.map((p: any) => ({
            producer_name: p.producer_name,
            block_signing_key: p.block_signing_key || 'unknown',
          })),
          0, new Date().toISOString()
        );
        log.info({ version: data.active.version }, 'Schedule bootstrapped from RPC');
      }
    }
  } catch (err) {
    log.warn({ err }, 'Failed — will detect from block headers');
  }

  const state: CatchupChain = {
    config: chainConfig,
    shipUrl,
    ship: null as any,
    schedule,
    evaluator,
    db,
    blocksProcessed: 0,
    headBlock: 0,
    currentBlock: startBlock,
    endBlock,
    startTime: Date.now(),
    lastProgressTime: Date.now(),
    pendingSchedule: null,
    processingBlock: Promise.resolve(),
  };

  const ship = new ShipClient({
    url: shipUrl,
    startBlock,
    endBlock: endBlock > 0 ? endBlock : 0xffffffff,
    fetchBlock: true,
    fetchTraces: true,
    fetchDeltas: false,
  });
  state.ship = ship;

  ship.on('status', (status: ShipGetStatusResult) => {
    state.headBlock = status.head.block_num;
    if (!endBlock) {
      // Re-check live writer position periodically
      findLiveWriterStartBlock(db, chainConfig.chain, chainConfig.network).then(eb => {
        if (eb > 0) state.endBlock = eb;
      });
    }
    log.info({
      chain: chainConfig.id,
      head: status.head.block_num,
      startBlock,
      endBlock: state.endBlock || 'HEAD',
    }, 'Catchup connected');
  });

  ship.on('block', (result: ShipResult) => {
    state.processingBlock = state.processingBlock.then(() =>
      processCatchupBlock(state, result)
    ).catch(err => {
      log.error({ err, chain: chainConfig.id }, 'Catchup block processing error');
    });
  });

  ship.on('max_reconnects', () => {
    log.error({ chain: chainConfig.id }, 'Catchup max reconnects reached');
  });

  await ship.connect();
}

async function processCatchupBlock(state: CatchupChain, result: ShipResult): Promise<void> {
  if (!result.this_block || !result.block) return;

  const blockNum = result.this_block.block_num;
  const block = result.block;
  state.currentBlock = blockNum;
  state.headBlock = result.head.block_num;

  // Check if we've reached the live writer's territory
  if (state.endBlock > 0 && blockNum >= state.endBlock) {
    log.info({
      chain: state.config.id,
      blockNum,
      endBlock: state.endBlock,
      roundsProcessed: state.blocksProcessed,
    }, 'Catchup complete — reached live writer start point');
    state.ship.disconnect();
    return;
  }

  // Schedule change detection (same as live writer)
  if (block.new_producers && block.new_producers.version > state.schedule.version) {
    state.pendingSchedule = block.new_producers;
  }

  const account = String(block.schedule_version);
  if (block.schedule_version > state.schedule.version && state.pendingSchedule) {
    await state.schedule.updateSchedule(
      state.pendingSchedule.version,
      state.pendingSchedule.producers,
      blockNum,
      block.timestamp
    );
    await state.evaluator.setScheduleActivation(block.timestamp);
    state.pendingSchedule = null;
  }

  // Producer events (regproducer/unregprod/kickbp)
  for (const trace of result.traces) {
    for (const actionTrace of trace.action_traces) {
      const actAccount = String(actionTrace.act.account);
      const actName = String(actionTrace.act.name);
      if (actAccount === 'eosio' && (actName === 'regproducer' || actName === 'unregprod' || actName === 'kickbp')) {
        let producerName = '';
        if (actName === 'kickbp') {
          try {
            const rawData = actionTrace.act.data;
            const dataBytes = rawData?.array ?? rawData;
            if (dataBytes && dataBytes.length >= 8) {
              const decoded = Name.from(Serializer.decode({ data: dataBytes, type: 'name' }));
              producerName = String(decoded);
            }
          } catch (err) {
    log.warn({ err }, 'Failed — will detect from block headers');
  }
        } else {
          producerName = String(actionTrace.act.authorization?.[0]?.actor || '');
        }
        if (producerName && producerName !== 'eosio') {
          await state.db.insertProducerEvent({
            chain: state.config.chain, network: state.config.network,
            producer: producerName, action: actName,
            block_number: blockNum, timestamp: block.timestamp,
          });
          if (actName === 'kickbp') {
            await state.db.insertProducerEvent({
              chain: state.config.chain, network: state.config.network,
              producer: producerName, action: 'unregprod',
              block_number: blockNum, timestamp: block.timestamp,
            });
          }
        }
      }
    }
  }

  // Round evaluation
  const blockRecord: BlockRecord = {
    block_num: blockNum,
    producer: block.producer,
    timestamp: block.timestamp,
    schedule_version: block.schedule_version,
  };
  await state.evaluator.processBlock(blockRecord);

  state.blocksProcessed++;

  // Progress logging
  const now = Date.now();
  if (now - state.lastProgressTime >= PROGRESS_INTERVAL) {
    state.lastProgressTime = now;
    const elapsed = (now - state.startTime) / 1000;
    const bps = Math.round(state.blocksProcessed / elapsed);
    const target = state.endBlock || state.headBlock;
    const remaining = target - blockNum;
    const etaSec = bps > 0 ? Math.round(remaining / bps) : 0;
    const etaMin = Math.floor(etaSec / 60);
    const etaHr = Math.floor(etaMin / 60);
    const pct = target > 0 ? ((blockNum / target) * 100).toFixed(1) : '?';
    const eta = etaHr > 0 ? `${etaHr}h ${etaMin % 60}m` : `${etaMin}m`;

    log.info({
      chain: state.config.id,
      block: blockNum,
      target,
      pct: `${pct}%`,
      bps,
      schedule: `v${state.schedule.version}`,
      eta,
    }, 'Catchup progress');
  }
}

async function main(): Promise<void> {
  log.info('AnvoIO Core Monitor — Catchup Writer starting');

  const config = loadConfig();
  const db = new Database(config.postgresUrl);
  await db.init();

  const chains = config.chains;
  log.info({ chains: chains.map(c => c.id) }, 'Chains to catch up');

  // Run catchup for each chain sequentially to avoid overloading
  for (const chainConfig of chains) {
    await startCatchup(chainConfig, db);
  }

  log.info('Catchup Writer — all chains queued');

  // Keep process alive
  const shutdown = async () => {
    log.info('Catchup Writer shutting down');
    await db.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  const msg = err?.message?.replace(/postgresql:\/\/[^@]*@/g, 'postgresql://***@') || 'Unknown error';
  log.error({ err: msg }, 'Fatal error');
  process.exit(1);
});
