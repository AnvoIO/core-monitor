import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Database } from '../../src/store/Database.js';
import { Retention } from '../../src/store/Retention.js';
import fs from 'fs';
import path from 'path';
import os from 'os';
import BetterSqlite3 from 'better-sqlite3';

describe('Retention', () => {
  let db: Database;
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-retention-'));
    db = new Database(tmpDir);
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should purge rounds older than retention period', () => {
    // Insert a round with a manually backdated created_at
    const rawDb = new BetterSqlite3(path.join(tmpDir, 'core-monitor.db'));

    rawDb.prepare(`
      INSERT INTO rounds (chain, network, round_number, schedule_version,
        timestamp_start, timestamp_end, producers_scheduled,
        producers_produced, producers_missed, created_at)
      VALUES ('libre', 'mainnet', 1, 1,
        '2024-01-01T00:00:00', '2024-01-01T00:02:00', 21, 21, 0,
        '2024-01-01T00:00:00')
    `).run();

    // Insert a recent round
    rawDb.prepare(`
      INSERT INTO rounds (chain, network, round_number, schedule_version,
        timestamp_start, timestamp_end, producers_scheduled,
        producers_produced, producers_missed, created_at)
      VALUES ('libre', 'mainnet', 2, 1,
        '2026-03-30T00:00:00', '2026-03-30T00:02:00', 21, 21, 0,
        datetime('now'))
    `).run();

    rawDb.close();

    // Run retention with 548 days (18 months)
    const retention = new Retention(tmpDir, 548);
    const deleted = retention.purgeOldRounds();
    retention.close();

    expect(deleted).toBe(1);

    // Verify only the recent round remains
    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(1);
    expect(rounds[0].round_number).toBe(2);
  });

  it('should NOT purge missed_block_events regardless of age', () => {
    const rawDb = new BetterSqlite3(path.join(tmpDir, 'core-monitor.db'));

    rawDb.prepare(`
      INSERT INTO missed_block_events (chain, network, producer, blocks_missed,
        timestamp, created_at)
      VALUES ('libre', 'mainnet', 'oldbp', 12,
        '2020-01-01T00:00:00', '2020-01-01T00:00:00')
    `).run();
    rawDb.close();

    const retention = new Retention(tmpDir, 548);
    retention.runAll();
    retention.close();

    const events = db.getMissedBlockEvents('libre', 'mainnet');
    expect(events).toHaveLength(1);
    expect(events[0].producer).toBe('oldbp');
  });

  it('should NOT purge fork_events regardless of age', () => {
    const rawDb = new BetterSqlite3(path.join(tmpDir, 'core-monitor.db'));

    rawDb.prepare(`
      INSERT INTO fork_events (chain, network, block_number,
        original_producer, replacement_producer, timestamp, created_at)
      VALUES ('libre', 'mainnet', 1000,
        'bp_a', 'bp_b', '2020-01-01T00:00:00', '2020-01-01T00:00:00')
    `).run();
    rawDb.close();

    const retention = new Retention(tmpDir, 548);
    retention.runAll();
    retention.close();

    const events = db.getForkEvents('libre', 'mainnet');
    expect(events).toHaveLength(1);
  });

  it('should purge old schedule changes', () => {
    const rawDb = new BetterSqlite3(path.join(tmpDir, 'core-monitor.db'));

    rawDb.prepare(`
      INSERT INTO schedule_changes (chain, network, schedule_version,
        producers_added, producers_removed, producer_list,
        block_number, timestamp, created_at)
      VALUES ('libre', 'mainnet', 1, '[]', '[]', '["bp1"]',
        1000, '2020-01-01T00:00:00', '2020-01-01T00:00:00')
    `).run();
    rawDb.close();

    const retention = new Retention(tmpDir, 548);
    retention.runAll();
    retention.close();

    const changes = db.getScheduleChanges('libre', 'mainnet');
    expect(changes).toHaveLength(0);
  });
});
