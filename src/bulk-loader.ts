/**
 * Bulk Loader — loads historical block data from a SHiP endpoint
 *
 * Phase 1: Stream blocks from SHiP → write CSV files (rounds.csv, round_producers.csv, missed_block_events.csv)
 * Phase 2: Bulk import CSVs into SQLite
 *
 * Usage:
 *   npx tsx src/bulk-loader.ts <ship_url> <chain> <network> <start_block> [<end_block>]
 */

import { ShipClient } from './ship/ShipClient.js';
import { Database } from './store/Database.js';
import { createWriteStream, existsSync, unlinkSync } from 'fs';
import path from 'path';
import type { ShipResult, ShipGetStatusResult } from './ship/types.js';
import BetterSqlite3 from 'better-sqlite3';

const args = process.argv.slice(2);
if (args.length < 5) {
  console.error('Usage: npx tsx src/bulk-loader.ts <ship_url> <api_url> <chain> <network> <start_block> [<end_block>]');
  process.exit(1);
}

const [shipUrl, apiUrl, chain, network, startBlockStr, endBlockStr] = args;
const startBlock = parseInt(startBlockStr, 10);
const endBlock = endBlockStr ? parseInt(endBlockStr, 10) : 0;

const DATA_DIR = process.env.DATA_DIR || './data';
const BLOCKS_PER_BP = parseInt(process.env.BLOCKS_PER_BP || '12', 10);
const PROGRESS_INTERVAL = 5000;

// CSV output files
const roundsCsv = path.join(DATA_DIR, `bulk-rounds-${chain}-${network}.csv`);
const rpCsv = path.join(DATA_DIR, `bulk-round-producers-${chain}-${network}.csv`);
const missedCsv = path.join(DATA_DIR, `bulk-missed-events-${chain}-${network}.csv`);
const scheduleCsv = path.join(DATA_DIR, `bulk-schedule-changes-${chain}-${network}.csv`);

// Clean up old files
for (const f of [roundsCsv, rpCsv, missedCsv, scheduleCsv]) {
  if (existsSync(f)) unlinkSync(f);
}

const roundsStream = createWriteStream(roundsCsv);
const rpStream = createWriteStream(rpCsv);
const missedStream = createWriteStream(missedCsv);
const scheduleStream = createWriteStream(scheduleCsv);

console.log(`AnvoIO Core Monitor — Bulk Loader (CSV mode)`);
console.log(`Chain: ${chain} ${network}`);
console.log(`SHiP: ${shipUrl}`);
console.log(`Start block: ${startBlock.toLocaleString()}`);
console.log(`End block: ${endBlock > 0 ? endBlock.toLocaleString() : 'HEAD'}`);
console.log('');

// In-memory state — zero DB writes during streaming
let scheduleVersion = 0;
let scheduleProducers: string[] = [];
let currentRound = 0;
let lastProducer = '';
let roundBlocks = new Map<string, number>();
let roundStartTimestamp = '';
let roundIsComplete = false;
let headBlockNum = 0;
let currentBlockNum = 0;
let blocksProcessed = 0;
let roundsWritten = 0;
let roundIdCounter = 1;
let lastProgressTime = Date.now();
let startTime = Date.now();
let firstCompleteRound = -1;

function csvEscape(s: string): string {
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeRound(endTimestamp: string) {
  const roundId = roundIdCounter++;
  const producersData = scheduleProducers.map((p, i) => {
    const produced = roundBlocks.get(p) || 0;
    const missed = Math.max(0, BLOCKS_PER_BP - produced);
    return { producer: p, position: i, produced, missed };
  });

  const producersProduced = producersData.filter(p => p.produced > 0).length;
  const producersMissed = producersData.filter(p => p.produced === 0).length;
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);

  // rounds: id,chain,network,round_number,schedule_version,timestamp_start,timestamp_end,producers_scheduled,producers_produced,producers_missed,created_at
  roundsStream.write(
    `${roundId},${chain},${network},${currentRound},${scheduleVersion},${roundStartTimestamp},${endTimestamp},${scheduleProducers.length},${producersProduced},${producersMissed},${now}\n`
  );

  for (const p of producersData) {
    // round_producers: round_id,producer,position,blocks_expected,blocks_produced,blocks_missed,first_block,last_block
    rpStream.write(
      `${roundId},${p.producer},${p.position},${BLOCKS_PER_BP},${p.produced},${p.missed},,\n`
    );

    if (p.produced === 0) {
      missedStream.write(
        `${chain},${network},${p.producer},${roundId},${BLOCKS_PER_BP},,${endTimestamp},${now}\n`
      );
    } else if (p.missed > 0) {
      missedStream.write(
        `${chain},${network},${p.producer},${roundId},${p.missed},,${endTimestamp},${now}\n`
      );
    }
  }

  roundsWritten++;

  if (firstCompleteRound < 0) {
    firstCompleteRound = currentRound;
  }
}

function isRoundBoundary(newProducer: string): boolean {
  if (scheduleProducers.length === 0) return false;
  const newPos = scheduleProducers.indexOf(newProducer);
  const lastPos = scheduleProducers.indexOf(lastProducer);
  if (newPos === -1 || lastPos === -1) return false;
  if (newPos === 0 && roundBlocks.size > 0) return true;
  if (newPos <= lastPos && roundBlocks.size >= scheduleProducers.length) return true;
  return false;
}

function processBlock(blockNum: number, producer: string, timestamp: string, sv: number, newProducers?: any) {
  if (newProducers) {
    const v = newProducers.version;
    if (v > scheduleVersion) {
      const oldProducers = [...scheduleProducers];
      scheduleProducers = newProducers.producers.map((p: any) => String(p.producer_name));

      const added = scheduleProducers.filter(p => !oldProducers.includes(p));
      const removed = oldProducers.filter(p => !scheduleProducers.includes(p));
      const now = new Date().toISOString().replace('T', ' ').substring(0, 19);

      scheduleStream.write(
        `${chain},${network},${v},${csvEscape(JSON.stringify(added))},${csvEscape(JSON.stringify(removed))},${csvEscape(JSON.stringify(scheduleProducers))},${blockNum},${timestamp},${now}\n`
      );

      console.log(`Schedule v${scheduleVersion} -> v${v} (${scheduleProducers.length} producers) at block ${blockNum.toLocaleString()}`);

      if (scheduleVersion > 0) {
        currentRound = 0;
        roundBlocks.clear();
        roundStartTimestamp = '';
        lastProducer = '';
        roundIsComplete = false;
      }
      scheduleVersion = v;
    }
  }

  if (sv > scheduleVersion && scheduleVersion > 0) {
    scheduleVersion = sv;
    currentRound = 0;
    roundBlocks.clear();
    roundStartTimestamp = '';
    lastProducer = '';
    roundIsComplete = false;
  }

  if (scheduleProducers.length === 0) return;

  if (producer !== lastProducer && lastProducer !== '') {
    if (isRoundBoundary(producer)) {
      if (roundIsComplete) {
        writeRound(timestamp);
        currentRound++;
      } else {
        roundIsComplete = true;
      }
      roundBlocks.clear();
      roundStartTimestamp = timestamp;
    }
  }

  if (roundBlocks.size === 0 && !roundStartTimestamp) {
    roundStartTimestamp = timestamp;
  }

  roundBlocks.set(producer, (roundBlocks.get(producer) || 0) + 1);
  lastProducer = producer;
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

// Phase 2: import CSVs into SQLite
function importCsvs() {
  console.log('');
  console.log('Phase 2: Importing CSVs into SQLite...');

  // Ensure schema exists
  const tempDb = new Database(DATA_DIR);
  tempDb.close();

  const dbPath = path.join(DATA_DIR, 'core-monitor.db');
  const raw = new BetterSqlite3(dbPath);
  raw.pragma('journal_mode = WAL');
  raw.pragma('synchronous = OFF');

  let count = 0;

  // Import rounds
  if (existsSync(roundsCsv)) {
    console.log('  Importing rounds...');
    const insertRound = raw.prepare(`
      INSERT INTO rounds (id, chain, network, round_number, schedule_version,
        timestamp_start, timestamp_end, producers_scheduled,
        producers_produced, producers_missed, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const lines = require('fs').readFileSync(roundsCsv, 'utf8').split('\n').filter(Boolean);
    const batch = raw.transaction((rows: string[]) => {
      for (const line of rows) {
        const parts = line.split(',');
        insertRound.run(...parts);
      }
    });
    batch(lines);
    count = lines.length;
    console.log(`    ${count.toLocaleString()} rounds imported`);
  }

  // Import round_producers
  if (existsSync(rpCsv)) {
    console.log('  Importing round producers...');
    const insertRp = raw.prepare(`
      INSERT INTO round_producers (round_id, producer, position,
        blocks_expected, blocks_produced, blocks_missed,
        first_block, last_block)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const lines = require('fs').readFileSync(rpCsv, 'utf8').split('\n').filter(Boolean);
    // Process in chunks to avoid memory issues
    const chunkSize = 50000;
    const batch = raw.transaction((rows: string[]) => {
      for (const line of rows) {
        const parts = line.split(',');
        insertRp.run(
          parseInt(parts[0]), parts[1], parseInt(parts[2]),
          parseInt(parts[3]), parseInt(parts[4]), parseInt(parts[5]),
          parts[6] || null, parts[7] || null
        );
      }
    });

    for (let i = 0; i < lines.length; i += chunkSize) {
      batch(lines.slice(i, i + chunkSize));
    }
    console.log(`    ${lines.length.toLocaleString()} round_producers imported`);
  }

  // Import missed block events
  if (existsSync(missedCsv)) {
    console.log('  Importing missed block events...');
    const insertMissed = raw.prepare(`
      INSERT INTO missed_block_events (chain, network, producer,
        round_id, blocks_missed, block_number, timestamp, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const lines = require('fs').readFileSync(missedCsv, 'utf8').split('\n').filter(Boolean);
    const batch = raw.transaction((rows: string[]) => {
      for (const line of rows) {
        const parts = line.split(',');
        insertMissed.run(
          parts[0], parts[1], parts[2],
          parseInt(parts[3]), parseInt(parts[4]),
          parts[5] || null, parts[6], parts[7]
        );
      }
    });

    for (let i = 0; i < lines.length; i += 50000) {
      batch(lines.slice(i, i + 50000));
    }
    console.log(`    ${lines.length.toLocaleString()} missed events imported`);
  }

  // Import schedule changes
  if (existsSync(scheduleCsv)) {
    console.log('  Importing schedule changes...');
    const insertSched = raw.prepare(`
      INSERT INTO schedule_changes (chain, network, schedule_version,
        producers_added, producers_removed, producer_list,
        block_number, timestamp, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const lines = require('fs').readFileSync(scheduleCsv, 'utf8').split('\n').filter(Boolean);
    for (const line of lines) {
      // CSV with possible quoted JSON fields
      const parts: string[] = [];
      let current = '';
      let inQuotes = false;
      for (const ch of line) {
        if (ch === '"') { inQuotes = !inQuotes; continue; }
        if (ch === ',' && !inQuotes) { parts.push(current); current = ''; continue; }
        current += ch;
      }
      parts.push(current);
      insertSched.run(...parts);
    }
    console.log(`    ${lines.length} schedule changes imported`);
  }

  // Save state
  const stateInsert = raw.prepare(`
    INSERT INTO monitor_state (chain, network, key, value, updated_at)
    VALUES (?, ?, ?, ?, datetime('now'))
    ON CONFLICT(chain, network, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
  `);

  stateInsert.run(chain, network, 'last_block', String(currentBlockNum));
  stateInsert.run(chain, network, 'current_round', String(currentRound));
  stateInsert.run(chain, network, 'last_schedule_version', String(scheduleVersion));
  if (firstCompleteRound >= 0) {
    stateInsert.run(chain, network, 'first_complete_round', String(firstCompleteRound));
  }
  if (scheduleProducers.length > 0) {
    stateInsert.run(chain, network, 'schedule', JSON.stringify({
      version: scheduleVersion,
      producers: scheduleProducers,
    }));
  }

  raw.close();
  console.log('  State saved');

  // Clean up CSV files
  for (const f of [roundsCsv, rpCsv, missedCsv, scheduleCsv]) {
    if (existsSync(f)) unlinkSync(f);
  }
  console.log('  CSV files cleaned up');
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
  // Close CSV streams
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

  // Wait for streams to flush
  setTimeout(() => {
    importCsvs();

    const totalElapsed = (Date.now() - startTime) / 1000;
    console.log('');
    console.log(`Total time: ${Math.floor(totalElapsed / 60)}m ${Math.floor(totalElapsed % 60)}s`);
    process.exit(0);
  }, 1000);
}

ship.on('max_reconnects', () => {
  console.error('Max reconnection attempts reached');
  process.exit(1);
});

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
      scheduleProducers = active.producers.map((p: any) =>
        p.producer_name || p.producer_name
      );
      console.log(`  Schedule v${scheduleVersion} loaded (${scheduleProducers.length} producers)`);
      // The first round boundary we see will still be discarded (roundIsComplete = false)
      // which is correct — we don't know where in the round we're starting
    }
  } catch (err) {
    console.log('  Failed to bootstrap schedule:', (err as Error).message);
  }
}

console.log('Phase 1: Streaming blocks to CSV...');
bootstrapSchedule().then(() => {
  ship.connect().catch((err) => {
    console.error('Failed to connect:', err.message);
    process.exit(1);
  });
});
