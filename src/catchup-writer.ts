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
 *   CATCHUP_START_BLOCK — start block (default: 1). Savanna chains must start at 1.
 *   CATCHUP_END_BLOCK — optional override; stop at this block instead of the
 *     live writer's position. Per-chain form: {CHAIN_ID}_CATCHUP_END_BLOCK.
 *     Use during recovery to let catchup outpace a far-behind live writer.
 *   POSTGRES_URL — PostgreSQL connection string (required)
 *   CHAINS — chain config (same as live writer)
 *
 * Without an override, a fresh run stops at the first block of the live writer's earliest
 * round and saves that as catchup_end_block, so a resumed run (by then the catchup's own
 * rounds are the earliest) still stops in the same place.
 */

import { loadConfig } from './config.js';
import { Database } from './store/Database.js';
import { ShipClient } from './ship/ShipClient.js';
import { ScheduleTracker } from './monitor/ScheduleTracker.js';
import { RoundEvaluator, type BlockRecord } from './monitor/RoundEvaluator.js';
import { SavannaCatchupSchedule } from './monitor/SavannaCatchupSchedule.js';
import { logger } from './utils/logger.js';
import type { ChainConfig } from './config.js';
import type { ShipResult, ShipGetStatusResult } from './ship/types.js';
import { Name, Serializer } from '@wharfkit/antelope';

const log = logger.child({ module: 'catchup' });

const PROGRESS_INTERVAL = 10000;
/** Unacknowledged blocks SHiP may send ahead of processing. */
const CATCHUP_MAX_IN_FLIGHT = 1000;

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
  /** Set for Savanna chains on their first block. */
  savanna: SavannaCatchupSchedule | null;
  done: boolean;
  processingBlock: Promise<void>;
}

const END_BLOCK_KEY = 'catchup_end_block';

/** First block of the live writer's earliest round; only meaningful before catchup writes rounds. */
async function findLiveWriterFirstBlock(db: Database, chain: string, network: string): Promise<number> {
  const first = await db.getEarliestRoundFirstBlock(chain, network);
  if (first === null) return 0;
  log.info({ chain, network, firstBlock: first }, 'Found live writer start point');
  return first;
}

/**
 * Pre-catchup_end_block fallback for resumed runs, and for runs that started before the live
 * writer had any rounds: shortly before the live writer's current block.
 */
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

  const configStartBlock = parseInt(process.env.CATCHUP_START_BLOCK || '1', 10);

  // Optional override: chase a specific block instead of stopping at the live
  // writer's position. Used during recovery when the writer is far behind and
  // we want catchup (3000+ bps) to fill the gap instead of waiting for the
  // writer (~100 bps) to grind through it.
  const chainPrefix = chainConfig.id.toUpperCase().replace(/[^A-Z0-9]/g, '_');
  const endBlockOverrideStr = process.env[`${chainPrefix}_CATCHUP_END_BLOCK`]
    || process.env.CATCHUP_END_BLOCK;

  // Resume from last processed block if available
  const { chain, network } = chainConfig;
  const savedBlock = await db.getState(chain, network, 'catchup_last_block');
  const resumeBlock = savedBlock ? parseInt(savedBlock, 10) + 1 : 0;
  const resuming = resumeBlock > configStartBlock;
  // A Savanna schedule tracker may have been saved before the evaluator's last block;
  // resume early enough that it sees every block (no-op for other chains).
  const startBlock = resuming
    ? await SavannaCatchupSchedule.resumeBlock(db, chainConfig, resumeBlock)
    : configStartBlock;

  let endBlock: number;
  const savedEnd = await db.getState(chain, network, END_BLOCK_KEY);
  if (endBlockOverrideStr) {
    endBlock = parseInt(endBlockOverrideStr, 10);
  } else if (savedEnd) {
    endBlock = parseInt(savedEnd, 10);
  } else if (!resuming) {
    endBlock = await findLiveWriterFirstBlock(db, chain, network);
    if (endBlock > 0) await db.setState(chain, network, END_BLOCK_KEY, String(endBlock));
  } else {
    endBlock = await findLiveWriterStartBlock(db, chain, network);
  }

  if (endBlock > 0 && startBlock > endBlock) {
    log.info({ chain: chainConfig.id, startBlock, endBlock }, 'Already caught up');
    return;
  }

  log.info({
    chain: chainConfig.id,
    shipUrl,
    startBlock,
    endBlock: endBlock || 'until live writer start',
    resumed: resuming ? `from block ${resumeBlock}` : false,
  }, 'Starting catchup');

  const schedule = new ScheduleTracker(chainConfig.chain, chainConfig.network, db, 'catchup_');
  const evaluator = new RoundEvaluator(chainConfig, db, schedule, 'catchup_', {
    stateWriteInterval: 100,
    batchInserts: true,
  });

  if (resuming) {
    // Resuming — restore schedule and evaluator state from the database
    await schedule.init();
    await evaluator.init();
    log.info({ chain: chainConfig.id, scheduleVersion: schedule.version },
      'Resumed catchup — restored schedule and evaluator state from DB');
  } else {
    // Fresh start from genesis — empty schedule, detect from block headers
    await evaluator.init();
    log.info({ chain: chainConfig.id }, 'Starting with empty schedule — will detect from block headers');
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
    savanna: null,
    done: false,
    processingBlock: Promise.resolve(),
  };

  const ship = new ShipClient({
    url: shipUrl,
    access: chainConfig.shipAccess,
    startBlock,
    // SHiP's end block is exclusive; the catchup processes endBlock itself to close its last round.
    endBlock: endBlock > 0 ? endBlock + 1 : 0xffffffff,
    fetchBlock: true,
    fetchTraces: true,
    fetchDeltas: false,
    blockTimestampEpochMs: chainConfig.blockTimestampEpochMs,
    // Bulk read: with SHiP's default-sized window a remote endpoint (e.g. through Cloudflare,
    // ~25 ms away) caps the stream near 300 blocks/s.
    maxMessagesInFlight: CATCHUP_MAX_IN_FLIGHT,
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

async function finishCatchup(state: CatchupChain, blockNum: number, timestamp: string): Promise<void> {
  state.done = true;
  state.ship.disconnect();
  await state.savanna?.persist();

  // Batch inserts don't maintain daily stats or outage events; derive them for everything
  // before the live writer's start.
  const { chain, network } = state.config;
  const from = await state.db.getEarliestRoundTimestamp(chain, network);
  if (from && from < timestamp) {
    await state.db.rebuildDerivedStats(chain, network, from, timestamp);
    log.info({ chain: state.config.id, from, to: timestamp }, 'Rebuilt daily stats and outage events');
  }

  log.info({
    chain: state.config.id,
    blockNum,
    endBlock: state.endBlock,
    blocksProcessed: state.blocksProcessed,
  }, 'Catchup complete — reached live writer start point');
}

async function processCatchupBlock(state: CatchupChain, result: ShipResult): Promise<void> {
  if (state.done || !result.this_block || !result.block) return;

  const blockNum = result.this_block.block_num;
  const block = result.block;
  state.currentBlock = blockNum;
  state.headBlock = result.head.block_num;

  // Past the live writer's start (e.g. a resumed stream): nothing to do.
  if (state.endBlock > 0 && blockNum > state.endBlock) {
    await finishCatchup(state, blockNum, block.timestamp);
    return;
  }

  // The genesis block has no producer; there's nothing to evaluate.
  if (block.savanna && !block.producer) return;

  if (block.savanna) {
    // Savanna chains: schedules come from proposer policy diffs, not schedule_version.
    state.savanna ??= new SavannaCatchupSchedule(state.config, state.db, state.schedule, state.evaluator);
    if (!(await state.savanna.process({ producer: block.producer, timestamp: block.timestamp, savanna: block.savanna }, blockNum))) {
      state.done = true;
      state.ship.disconnect();
      return;
    }
  } else if (block.new_producers && block.new_producers.version > state.schedule.version) {
    // Schedule change detection (same as live writer)
    // Store proposed schedule from block headers
    state.pendingSchedule = block.new_producers;
    log.info({ chain: state.config.id, version: block.new_producers.version, blockNum },
      'Pending schedule detected');
  }

  // Apply when schedule_version in block header actually increments
  if (!block.savanna && block.schedule_version > state.schedule.version && state.pendingSchedule) {
    const changed = await state.schedule.updateSchedule(
      state.pendingSchedule.version,
      state.pendingSchedule.producers,
      blockNum,
      block.timestamp
    );
    if (changed) {
      await state.evaluator.setScheduleActivation(block.timestamp);
      log.info({ chain: state.config.id, version: state.pendingSchedule.version, blockNum },
        'Schedule activated');
    }
    state.pendingSchedule = null;
  }

  // Producer events (regproducer/unregprod/kickbp)
  const sys = state.config.systemAccount;
  for (const trace of result.traces) {
    for (const actionTrace of trace.action_traces) {
      const actAccount = String(actionTrace.act.account);
      const actName = String(actionTrace.act.name);
      if (actAccount === sys && (actName === 'regproducer' || actName === 'unregprod' || actName === 'kickbp')) {
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
            log.warn({ err, blockNum }, 'Could not decode kickbp data; skipping event');
          }
        } else {
          producerName = String(actionTrace.act.authorization?.[0]?.actor || '');
        }
        if (producerName && producerName !== sys) {
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
    schedule_version: state.savanna?.activeVersion ?? block.schedule_version,
  };
  await state.evaluator.processBlock(blockRecord);

  state.blocksProcessed++;

  // The live writer starts at endBlock. Processing it closed the catchup's last round.
  if (state.endBlock > 0 && blockNum >= state.endBlock) {
    await finishCatchup(state, blockNum, block.timestamp);
    return;
  }

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
      schedule: `v${state.savanna?.activeVersion ?? state.schedule.version}`,
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
