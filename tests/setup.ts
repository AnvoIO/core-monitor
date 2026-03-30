/**
 * Shared test setup — provides a clean PostgreSQL database for each test file.
 */
import pg from 'pg';
import { Database } from '../src/store/Database.js';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://test:test@localhost:15432/core_monitor_test';
if (!process.env.TEST_POSTGRES_URL) {
  console.warn('TEST_POSTGRES_URL not set — using default localhost:15432');
}

export function getTestPgUrl(): string {
  return TEST_PG_URL;
}

export async function createTestDb(): Promise<Database> {
  const db = new Database(TEST_PG_URL);
  await db.init();
  return db;
}

export async function cleanTestDb(): Promise<void> {
  const pool = new pg.Pool({ connectionString: TEST_PG_URL, max: 1 });
  await pool.query(`
    DELETE FROM round_producers;
    DELETE FROM missed_block_events;
    DELETE FROM fork_events;
    DELETE FROM schedule_changes;
    DELETE FROM producer_events;
    DELETE FROM weekly_summaries;
    DELETE FROM monthly_summaries;
    DELETE FROM rounds;
    DELETE FROM monitor_state;
  `);
  await pool.end();
}
