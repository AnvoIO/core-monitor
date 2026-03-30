/**
 * Import CSV files from bulk loader into PostgreSQL.
 *
 * Usage:
 *   POSTGRES_URL=postgresql://... npx tsx src/import-csv.ts <chain> <network> <csv_dir>
 */

import pg from 'pg';
import { Database } from './store/Database.js';
import { readFileSync, existsSync } from 'fs';
import path from 'path';

const args = process.argv.slice(2);
if (args.length < 3) {
  console.error('Usage: POSTGRES_URL=... npx tsx src/import-csv.ts <chain> <network> <csv_dir>');
  process.exit(1);
}

const [chain, network, csvDir] = args;
const pgUrl = process.env.POSTGRES_URL;
if (!pgUrl) {
  console.error('POSTGRES_URL environment variable is required');
  process.exit(1);
}

const roundsCsv = path.join(csvDir, `bulk-rounds-${chain}-${network}.csv`);
const rpCsv = path.join(csvDir, `bulk-round-producers-${chain}-${network}.csv`);
const missedCsv = path.join(csvDir, `bulk-missed-events-${chain}-${network}.csv`);
const scheduleCsv = path.join(csvDir, `bulk-schedule-changes-${chain}-${network}.csv`);

async function main() {
  console.log(`Importing CSVs for ${chain} ${network} into PostgreSQL`);
  console.log(`Source: ${csvDir}`);

  // Ensure schema exists
  const db = new Database(pgUrl!);
  await db.init();

  const pool = new pg.Pool({ connectionString: pgUrl, max: 5 });
  const idMap = new Map<number, number>();
  const startTime = Date.now();

  // Rounds
  if (existsSync(roundsCsv)) {
    console.log('Importing rounds...');
    const lines = readFileSync(roundsCsv, 'utf8').split('\n').filter(Boolean);
    let count = 0;
    for (const line of lines) {
      const p = line.split(',');
      const csvId = parseInt(p[0]);
      const result = await pool.query(
        `INSERT INTO rounds (chain,network,round_number,schedule_version,timestamp_start,timestamp_end,producers_scheduled,producers_produced,producers_missed,created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
        [p[1], p[2], parseInt(p[3]), parseInt(p[4]), p[5], p[6], parseInt(p[7]), parseInt(p[8]), parseInt(p[9]), p[10]]
      );
      idMap.set(csvId, result.rows[0].id);
      count++;
      if (count % 1000 === 0) process.stdout.write(`  ${count.toLocaleString()} rounds...\r`);
    }
    console.log(`  ${count.toLocaleString()} rounds imported`);
  }

  // Round producers
  if (existsSync(rpCsv)) {
    console.log('Importing round producers...');
    const lines = readFileSync(rpCsv, 'utf8').split('\n').filter(Boolean);
    let count = 0;
    for (const line of lines) {
      const p = line.split(',');
      const dbRoundId = idMap.get(parseInt(p[0]));
      if (!dbRoundId) continue;
      await pool.query(
        `INSERT INTO round_producers (round_id,producer,position,blocks_expected,blocks_produced,blocks_missed,first_block,last_block)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [dbRoundId, p[1], parseInt(p[2]), parseInt(p[3]), parseInt(p[4]), parseInt(p[5]), p[6] || null, p[7] || null]
      );
      count++;
      if (count % 5000 === 0) process.stdout.write(`  ${count.toLocaleString()} round_producers...\r`);
    }
    console.log(`  ${count.toLocaleString()} round_producers imported`);
  }

  // Missed block events
  if (existsSync(missedCsv)) {
    console.log('Importing missed block events...');
    const lines = readFileSync(missedCsv, 'utf8').split('\n').filter(Boolean);
    let count = 0;
    for (const line of lines) {
      const p = line.split(',');
      const dbRoundId = idMap.get(parseInt(p[3]));
      await pool.query(
        `INSERT INTO missed_block_events (chain,network,producer,round_id,blocks_missed,block_number,timestamp,created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [p[0], p[1], p[2], dbRoundId || null, parseInt(p[4]), p[5] || null, p[6], p[7]]
      );
      count++;
      if (count % 1000 === 0) process.stdout.write(`  ${count.toLocaleString()} missed events...\r`);
    }
    console.log(`  ${count.toLocaleString()} missed events imported`);
  }

  // Schedule changes
  if (existsSync(scheduleCsv)) {
    console.log('Importing schedule changes...');
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
    console.log(`  ${lines.length} schedule changes imported`);
  }

  // Read last state from the CSVs to save monitor state
  // Parse last round from rounds CSV
  if (existsSync(roundsCsv)) {
    const lines = readFileSync(roundsCsv, 'utf8').split('\n').filter(Boolean);
    if (lines.length > 0) {
      const lastLine = lines[lines.length - 1].split(',');
      const firstLine = lines[0].split(',');
      const lastRoundNum = parseInt(lastLine[3]);
      const firstRoundNum = parseInt(firstLine[3]);
      const schedVersion = parseInt(lastLine[4]);

      const setState = async (key: string, value: string) => {
        await pool.query(
          `INSERT INTO monitor_state (chain,network,key,value,updated_at) VALUES ($1,$2,$3,$4,NOW())
           ON CONFLICT(chain,network,key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()`,
          [chain, network, key, value]
        );
      };

      await setState('first_complete_round', String(firstRoundNum));

      console.log(`  State: first round=${firstRoundNum}, last round=${lastRoundNum}, schedule=v${schedVersion}`);
    }
  }

  await pool.end();
  await db.close();

  const elapsed = (Date.now() - startTime) / 1000;
  console.log(`\nImport complete in ${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`);
}

main().catch(err => {
  console.error('Import failed:', err);
  process.exit(1);
});
