/**
 * Bulk Loader — loads historical block data from a SHiP endpoint
 *
 * Phase 1: Stream blocks from SHiP → write CSV files
 * Phase 2: Bulk import CSVs into SQLite
 *
 * Uses slot-based round detection (same as live monitor).
 *
 * Usage:
 *   npx tsx src/bulk-loader.ts <ship_url> <api_url> <chain> <network> <start_block> [<end_block>]
 */

import { ShipClient } from './ship/ShipClient.js';
import { Database } from './store/Database.js';
import { createWriteStream, existsSync, readFileSync, unlinkSync } from 'fs';
import path from 'path';
import pg from 'pg';
import type { ShipResult, ShipGetStatusResult } from './ship/types.js';

const args = process.argv.slice(2);
if (args.length < 5) {
  console.error('Usage: npx tsx src/bulk-loader.ts <ship_url> <api_url> <chain> <network> <start_block> [<end_block>]');
  process.exit(1);
}

const [shipUrl, apiUrl, chain, network, startBlockStr, endBlockStr] = args;
const startBlock = parseInt(startBlockStr, 10);
const endBlock = endBlockStr ? parseInt(endBlockStr, 10) : 0;

const DATA_DIR = process.env.DATA_DIR || './data';
const SCHEDULE_SIZE = parseInt(process.env.SCHEDULE_SIZE || '21', 10);
const BLOCKS_PER_BP = parseInt(process.env.BLOCKS_PER_BP || '12', 10);
const COMMIT_INTERVAL = 500;
const PROGRESS_INTERVAL = 5000;

/** Antelope epoch: 2000-01-01T00:00:00.000Z */
const ANTELOPE_EPOCH_MS = Date.UTC(2000, 0, 1);
const SLOTS_PER_ROUND = SCHEDULE_SIZE * BLOCKS_PER_BP;

function timestampToSlot(timestamp: string): number {
  return Math.floor((new Date(timestamp).getTime() - ANTELOPE_EPOCH_MS) / 500);
}

function slotToGlobalRound(slot: number): number {
  return Math.floor(slot / SLOTS_PER_ROUND);
}

let scheduleActivationGlobalRound = 0;

console.log(`AnvoIO Core Monitor — Bulk Loader (CSV + slot-based)`);
console.log(`Chain: ${chain} ${network}`);
console.log(`SHiP: ${shipUrl}`);
console.log(`Start block: ${startBlock.toLocaleString()}`);
console.log(`End block: ${endBlock > 0 ? endBlock.toLocaleString() : 'HEAD'}`);
console.log('');

// CSV output files
const roundsCsv = path.join(DATA_DIR, `bulk-rounds-${chain}-${network}.csv`);
const rpCsv = path.join(DATA_DIR, `bulk-round-producers-${chain}-${network}.csv`);
const missedCsv = path.join(DATA_DIR, `bulk-missed-events-${chain}-${network}.csv`);
const scheduleCsv = path.join(DATA_DIR, `bulk-schedule-changes-${chain}-${network}.csv`);

for (const f of [roundsCsv, rpCsv, missedCsv, scheduleCsv]) {
  if (existsSync(f)) unlinkSync(f);
}

const roundsStream = createWriteStream(roundsCsv);
const rpStream = createWriteStream(rpCsv);
const missedStream = createWriteStream(missedCsv);
const scheduleStream = createWriteStream(scheduleCsv);

// State
let scheduleVersion = 0;
let scheduleProducers: string[] = [];
let currentRound = -1;
let headBlockNum = 0;
let currentBlockNum = 0;
let blocksProcessed = 0;
let roundsWritten = 0;
let roundIdCounter = 1;
let lastProgressTime = Date.now();
let startTime = Date.now();
let firstCompleteRound = -1;

// Per-round accumulator
let roundBlocks = new Map<string, { count: number; firstBlock: number; lastBlock: number }>();
let roundStartTimestamp = '';
let roundEndTimestamp = '';

function csvEscape(s: string): string {
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeRound() {
  if (scheduleProducers.length === 0) return;

  const roundId = roundIdCounter++;
  const displayRound = currentRound - scheduleActivationGlobalRound;
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);

  const producersData = scheduleProducers.map((p, i) => {
    const data = roundBlocks.get(p);
    const produced = data ? Math.min(data.count, BLOCKS_PER_BP) : 0;
    const missed = BLOCKS_PER_BP - produced;
    return { producer: p, position: i, produced, missed, first: data?.firstBlock ?? null, last: data?.lastBlock ?? null };
  });

  const producersProduced = producersData.filter(p => p.produced > 0).length;
  const producersMissed = producersData.filter(p => p.produced === 0).length;

  roundsStream.write(
    `${roundId},${chain},${network},${displayRound},${scheduleVersion},${roundStartTimestamp},${roundEndTimestamp},${scheduleProducers.length},${producersProduced},${producersMissed},${now}\n`
  );

  for (const p of producersData) {
    rpStream.write(
      `${roundId},${p.producer},${p.position},${BLOCKS_PER_BP},${p.produced},${p.missed},${p.first ?? ''},${p.last ?? ''}\n`
    );

    if (p.produced === 0) {
      missedStream.write(
        `${chain},${network},${p.producer},${roundId},${BLOCKS_PER_BP},,${roundEndTimestamp},${now}\n`
      );
    } else if (p.missed > 0) {
      missedStream.write(
        `${chain},${network},${p.producer},${roundId},${p.missed},,${roundEndTimestamp},${now}\n`
      );
    }
  }

  roundsWritten++;
  if (firstCompleteRound < 0) firstCompleteRound = displayRound;
}

function processBlock(blockNum: number, producer: string, timestamp: string, sv: number, newProducers?: any) {
  // Schedule change detection
  if (newProducers) {
    const v = newProducers.version;
    if (v > scheduleVersion) {
      const oldProducers = [...scheduleProducers];
      scheduleProducers = newProducers.producers.map((p: any) => String(p.producer_name));

      const added = scheduleProducers.filter((p: string) => !oldProducers.includes(p));
      const removed = oldProducers.filter((p: string) => !scheduleProducers.includes(p));
      const now = new Date().toISOString().replace('T', ' ').substring(0, 19);

      scheduleStream.write(
        `${chain},${network},${v},${csvEscape(JSON.stringify(added))},${csvEscape(JSON.stringify(removed))},${csvEscape(JSON.stringify(scheduleProducers))},${blockNum},${timestamp},${now}\n`
      );

      console.log(`Schedule v${scheduleVersion} -> v${v} (${scheduleProducers.length} producers) at block ${blockNum.toLocaleString()}`);
      scheduleVersion = v;
      // Update schedule activation point for display round offset
      scheduleActivationGlobalRound = slotToGlobalRound(timestampToSlot(timestamp));
    }
  }

  if (scheduleProducers.length === 0) return;

  // Slot-based round detection
  const slot = timestampToSlot(timestamp);
  const blockRound = slotToGlobalRound(slot);

  if (currentRound === -1) {
    currentRound = blockRound;
    roundStartTimestamp = timestamp;
    roundBlocks.clear();
  }

  if (blockRound > currentRound) {
    // Evaluate completed round
    writeRound();

    // Start new round
    currentRound = blockRound;
    roundBlocks.clear();
    roundStartTimestamp = timestamp;
    roundEndTimestamp = '';
  }

  // Accumulate
  const existing = roundBlocks.get(producer);
  if (existing) {
    existing.count++;
    existing.lastBlock = blockNum;
  } else {
    roundBlocks.set(producer, { count: 1, firstBlock: blockNum, lastBlock: blockNum });
  }
  roundEndTimestamp = timestamp;
}

function logProgress() {
  const now = Date.now();
  if (now - lastProgressTime < PROGRESS_INTERVAL) return;
  lastProgressTime = now;

  const elapsed = (now - startTime) / 1000;
  const bps = Math.round(blocksProcessed / elapsed);
  const remaining = headBlockNum - currentBlockNum;
  const etaSec = bps > 0 ? Math.round(remaining / bps) : 0;
  const etaMin = Math.floor(etaSec / 60);
  const etaHr = Math.floor(etaMin / 60);
  const pct = headBlockNum > startBlock
    ? ((currentBlockNum - startBlock) / (headBlockNum - startBlock) * 100).toFixed(1)
    : '0';
  const eta = etaHr > 0 ? `${etaHr}h ${etaMin % 60}m` : `${etaMin}m`;

  console.log(
    `${currentBlockNum.toLocaleString()} / ${headBlockNum.toLocaleString()} (${pct}%) | ` +
    `${bps.toLocaleString()} blk/s | Rounds: ${roundsWritten.toLocaleString()} | v${scheduleVersion} | ETA: ${eta}`
  );
}

// Phase 2: import CSVs into PostgreSQL
async function importCsvs() {
  const pgUrl = process.env.POSTGRES_URL;
  if (!pgUrl) {
    console.log('\nPhase 2: POSTGRES_URL not set — CSVs saved for manual import.');
    return;
  }

  console.log('');
  console.log('Phase 2: Importing CSVs into PostgreSQL...');

  const db = new Database(pgUrl);
  await db.init();
  const pool = new pg.Pool({ connectionString: pgUrl, max: 2 });
  const idMap = new Map<number, number>();

  if (existsSync(roundsCsv)) {
    console.log('  Importing rounds...');
    const lines = readFileSync(roundsCsv, 'utf8').split('\n').filter(Boolean);
    for (const line of lines) {
      const p = line.split(',');
      const csvId = parseInt(p[0]);
      const result = await pool.query(
        `INSERT INTO rounds (chain,network,round_number,schedule_version,timestamp_start,timestamp_end,producers_scheduled,producers_produced,producers_missed,created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
        [p[1], p[2], parseInt(p[3]), parseInt(p[4]), p[5], p[6], parseInt(p[7]), parseInt(p[8]), parseInt(p[9]), p[10]]
      );
      idMap.set(csvId, result.rows[0].id);
    }
    console.log(`    ${lines.length.toLocaleString()} rounds imported`);
  }

  if (existsSync(rpCsv)) {
    console.log('  Importing round producers...');
    const lines = readFileSync(rpCsv, 'utf8').split('\n').filter(Boolean);
    for (const line of lines) {
      const p = line.split(',');
      const dbRoundId = idMap.get(parseInt(p[0]));
      if (!dbRoundId) continue;
      await pool.query(
        `INSERT INTO round_producers (round_id,producer,position,blocks_expected,blocks_produced,blocks_missed,first_block,last_block)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [dbRoundId, p[1], parseInt(p[2]), parseInt(p[3]), parseInt(p[4]), parseInt(p[5]), p[6] || null, p[7] || null]
      );
    }
    console.log(`    ${lines.length.toLocaleString()} round_producers imported`);
  }

  if (existsSync(missedCsv)) {
    console.log('  Importing missed block events...');
    const lines = readFileSync(missedCsv, 'utf8').split('\n').filter(Boolean);
    for (const line of lines) {
      const p = line.split(',');
      const dbRoundId = idMap.get(parseInt(p[3]));
      await pool.query(
        `INSERT INTO missed_block_events (chain,network,producer,round_id,blocks_missed,block_number,timestamp,created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [p[0], p[1], p[2], dbRoundId || null, parseInt(p[4]), p[5] || null, p[6], p[7]]
      );
    }
    console.log(`    ${lines.length.toLocaleString()} missed events imported`);
  }

  if (existsSync(scheduleCsv)) {
    console.log('  Importing schedule changes...');
    const lines = readFileSync(scheduleCsv, 'utf8').split('\n').filter(Boolean);
    for (const line of lines) {
      const parts: string[] = [];
      let current = '';
      let inQuotes = false;
      for (const ch of line) {
        if (ch === '"') { inQuotes = !inQuotes; continue; }
        if (ch === ',' && !inQuotes) { parts.push(current); current = ''; continue; }
        current += ch;
      }
      parts.push(current);
      await pool.query(
        `INSERT INTO schedule_changes (chain,network,schedule_version,producers_added,producers_removed,producer_list,block_number,timestamp,created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, parts
      );
    }
    console.log(`    ${lines.length} schedule changes imported`);
  }

  // Save state
  const setState = async (key: string, value: string) => {
    await pool.query(
      `INSERT INTO monitor_state (chain,network,key,value,updated_at) VALUES ($1,$2,$3,$4,NOW())
       ON CONFLICT(chain,network,key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()`,
      [chain, network, key, value]
    );
  };
  await setState('last_block', String(currentBlockNum));
  await setState('current_global_round', String(currentRound));
  await setState('schedule_activation_global_round', String(scheduleActivationGlobalRound));
  if (firstCompleteRound >= 0) await setState('first_complete_round', String(firstCompleteRound));
  if (scheduleProducers.length > 0) {
    await setState('schedule', JSON.stringify({ version: scheduleVersion, producers: scheduleProducers }));
  }
  console.log('  State saved');

  await pool.end();
  await db.close();

  for (const f of [roundsCsv, rpCsv, missedCsv, scheduleCsv]) {
    if (existsSync(f)) unlinkSync(f);
  }
  console.log('  CSVs cleaned up');
}

// SHiP
const ship = new ShipClient({
  url: shipUrl,
  startBlock,
  endBlock: endBlock > 0 ? endBlock : 0xffffffff,
  fetchBlock: true,
  fetchTraces: false,
  fetchDeltas: false,
});

ship.on('status', (status: ShipGetStatusResult) => {
  headBlockNum = status.head.block_num;
  console.log(`Connected — head: ${headBlockNum.toLocaleString()}, to process: ${(headBlockNum - startBlock).toLocaleString()}`);
  console.log('');
});

ship.on('block', (result: ShipResult) => {
  if (!result.this_block || !result.block) return;

  currentBlockNum = result.this_block.block_num;
  headBlockNum = result.head.block_num;

  processBlock(
    currentBlockNum,
    result.block.producer,
    result.block.timestamp,
    result.block.schedule_version,
    result.block.new_producers
  );

  blocksProcessed++;
  logProgress();

  if (endBlock > 0 && currentBlockNum >= endBlock) {
    finish();
  } else if (currentBlockNum >= headBlockNum - 2) {
    finish();
  }
});

function finish() {
  // Write final round
  if (roundBlocks.size > 0 && scheduleProducers.length > 0) {
    writeRound();
  }

  roundsStream.end();
  rpStream.end();
  missedStream.end();
  scheduleStream.end();

  const elapsed = (Date.now() - startTime) / 1000;
  const min = Math.floor(elapsed / 60);
  const bps = Math.round(blocksProcessed / elapsed);

  console.log('');
  console.log(`Phase 1 complete — CSV files written`);
  console.log(`  Blocks: ${blocksProcessed.toLocaleString()}`);
  console.log(`  Rounds: ${roundsWritten.toLocaleString()}`);
  console.log(`  Speed: ${bps.toLocaleString()} blocks/sec`);
  console.log(`  Duration: ${min}m ${Math.floor(elapsed % 60)}s`);

  ship.disconnect();

  setTimeout(async () => {
    if (process.env.POSTGRES_URL) {
      await importCsvs();
    } else {
      console.log('');
      console.log('CSV files ready for import:');
      console.log(`  ${roundsCsv}`);
      console.log(`  ${rpCsv}`);
      console.log(`  ${missedCsv}`);
      console.log(`  ${scheduleCsv}`);
      console.log('');
      console.log('Run the import script on the target host:');
      console.log(`  POSTGRES_URL=... npx tsx src/import-csv.ts ${chain} ${network} data/`);
    }
    const totalElapsed = (Date.now() - startTime) / 1000;
    console.log('');
    console.log(`Total time: ${Math.floor(totalElapsed / 60)}m ${Math.floor(totalElapsed % 60)}s`);
    process.exit(0);
  }, 1000);
}

async function bootstrapSchedule() {
  try {
    console.log('Bootstrapping schedule from RPC...');
    const response = await fetch(`${apiUrl}/v1/chain/get_producer_schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    if (!response.ok) {
      console.log('  Failed to fetch schedule, will detect from blocks');
      return;
    }
    const data = await response.json() as any;
    const active = data.active;
    if (active && active.producers && active.producers.length > 0) {
      scheduleVersion = active.version;
      scheduleProducers = active.producers.map((p: any) => p.producer_name);
      console.log(`  Schedule v${scheduleVersion} loaded (${scheduleProducers.length} producers)`);
    }
  } catch (err) {
    console.log('  Failed to bootstrap schedule:', (err as Error).message);
  }
}

ship.on('max_reconnects', () => {
  console.error('Max reconnection attempts reached');
  process.exit(1);
});

console.log('Phase 1: Streaming blocks to CSV...');
bootstrapSchedule().then(() => {
  ship.connect().catch((err) => {
    console.error('Failed to connect:', err.message);
    process.exit(1);
  });
});
