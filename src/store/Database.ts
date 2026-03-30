import BetterSqlite3 from 'better-sqlite3';
import path from 'path';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'Database' });

export class Database {
  private db: BetterSqlite3.Database;

  constructor(dataDir: string) {
    const dbPath = path.join(dataDir, 'core-monitor.db');
    log.info({ path: dbPath }, 'Opening database');
    this.db = new BetterSqlite3(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
    this.migrate();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
      );

      INSERT OR IGNORE INTO schema_version (version) VALUES (0);
    `);

    const currentVersion = this.db.prepare(
      'SELECT MAX(version) as version FROM schema_version'
    ).get() as { version: number };

    if (currentVersion.version < 1) {
      log.info('Running migration v1: initial schema');
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS rounds (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          round_number INTEGER NOT NULL,
          schedule_version INTEGER NOT NULL,
          timestamp_start TEXT NOT NULL,
          timestamp_end TEXT,
          producers_scheduled INTEGER NOT NULL DEFAULT 0,
          producers_produced INTEGER NOT NULL DEFAULT 0,
          producers_missed INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_rounds_chain_network
          ON rounds(chain, network, round_number DESC);

        CREATE INDEX IF NOT EXISTS idx_rounds_created
          ON rounds(created_at);

        CREATE TABLE IF NOT EXISTS round_producers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
          producer TEXT NOT NULL,
          position INTEGER NOT NULL,
          blocks_expected INTEGER NOT NULL DEFAULT 12,
          blocks_produced INTEGER NOT NULL DEFAULT 0,
          blocks_missed INTEGER NOT NULL DEFAULT 0,
          first_block INTEGER,
          last_block INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_round_producers_round
          ON round_producers(round_id);

        CREATE INDEX IF NOT EXISTS idx_round_producers_producer
          ON round_producers(producer, round_id DESC);

        CREATE TABLE IF NOT EXISTS missed_block_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          producer TEXT NOT NULL,
          round_id INTEGER REFERENCES rounds(id) ON DELETE SET NULL,
          blocks_missed INTEGER NOT NULL,
          block_number INTEGER,
          timestamp TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_missed_events_chain
          ON missed_block_events(chain, network, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_missed_events_producer
          ON missed_block_events(producer, timestamp DESC);

        CREATE TABLE IF NOT EXISTS fork_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          round_id INTEGER REFERENCES rounds(id) ON DELETE SET NULL,
          block_number INTEGER NOT NULL,
          original_producer TEXT NOT NULL,
          replacement_producer TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_fork_events_chain
          ON fork_events(chain, network, timestamp DESC);

        CREATE TABLE IF NOT EXISTS schedule_changes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          schedule_version INTEGER NOT NULL,
          producers_added TEXT,
          producers_removed TEXT,
          producer_list TEXT NOT NULL,
          block_number INTEGER NOT NULL,
          timestamp TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_schedule_changes_chain
          ON schedule_changes(chain, network, schedule_version DESC);

        CREATE TABLE IF NOT EXISTS weekly_summaries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          week_start TEXT NOT NULL,
          week_end TEXT NOT NULL,
          producer TEXT NOT NULL,
          rounds_scheduled INTEGER NOT NULL DEFAULT 0,
          rounds_produced INTEGER NOT NULL DEFAULT 0,
          rounds_missed INTEGER NOT NULL DEFAULT 0,
          blocks_expected INTEGER NOT NULL DEFAULT 0,
          blocks_produced INTEGER NOT NULL DEFAULT 0,
          blocks_missed INTEGER NOT NULL DEFAULT 0,
          fork_count INTEGER NOT NULL DEFAULT 0,
          reliability_pct REAL NOT NULL DEFAULT 100.0,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_summaries_unique
          ON weekly_summaries(chain, network, week_start, producer);

        CREATE TABLE IF NOT EXISTS monthly_summaries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          month_start TEXT NOT NULL,
          month_end TEXT NOT NULL,
          producer TEXT NOT NULL,
          rounds_scheduled INTEGER NOT NULL DEFAULT 0,
          rounds_produced INTEGER NOT NULL DEFAULT 0,
          rounds_missed INTEGER NOT NULL DEFAULT 0,
          blocks_expected INTEGER NOT NULL DEFAULT 0,
          blocks_produced INTEGER NOT NULL DEFAULT 0,
          blocks_missed INTEGER NOT NULL DEFAULT 0,
          fork_count INTEGER NOT NULL DEFAULT 0,
          reliability_pct REAL NOT NULL DEFAULT 100.0,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_summaries_unique
          ON monthly_summaries(chain, network, month_start, producer);

        CREATE TABLE IF NOT EXISTS monitor_state (
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY (chain, network, key)
        );

        INSERT INTO schema_version (version) VALUES (1);
      `);
    }

    if (currentVersion.version < 2) {
      log.info('Running migration v2: producer_events table');
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS producer_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chain TEXT NOT NULL,
          network TEXT NOT NULL,
          producer TEXT NOT NULL,
          action TEXT NOT NULL,
          block_number INTEGER NOT NULL,
          timestamp TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_producer_events_chain
          ON producer_events(chain, network, timestamp DESC);

        INSERT INTO schema_version (version) VALUES (2);
      `);
    }

    log.info({ version: 2 }, 'Database schema up to date');
  }

  // -- Rounds --

  insertRound(params: {
    chain: string;
    network: string;
    round_number: number;
    schedule_version: number;
    timestamp_start: string;
    timestamp_end: string;
    producers_scheduled: number;
    producers_produced: number;
    producers_missed: number;
  }): number {
    const result = this.db.prepare(`
      INSERT INTO rounds (chain, network, round_number, schedule_version,
        timestamp_start, timestamp_end, producers_scheduled,
        producers_produced, producers_missed)
      VALUES (@chain, @network, @round_number, @schedule_version,
        @timestamp_start, @timestamp_end, @producers_scheduled,
        @producers_produced, @producers_missed)
    `).run(params);
    return Number(result.lastInsertRowid);
  }

  insertRoundProducer(params: {
    round_id: number;
    producer: string;
    position: number;
    blocks_expected: number;
    blocks_produced: number;
    blocks_missed: number;
    first_block: number | null;
    last_block: number | null;
  }): void {
    this.db.prepare(`
      INSERT INTO round_producers (round_id, producer, position,
        blocks_expected, blocks_produced, blocks_missed,
        first_block, last_block)
      VALUES (@round_id, @producer, @position,
        @blocks_expected, @blocks_produced, @blocks_missed,
        @first_block, @last_block)
    `).run(params);
  }

  // -- Missed Block Events --

  insertMissedBlockEvent(params: {
    chain: string;
    network: string;
    producer: string;
    round_id: number | null;
    blocks_missed: number;
    block_number: number | null;
    timestamp: string;
  }): void {
    this.db.prepare(`
      INSERT INTO missed_block_events (chain, network, producer,
        round_id, blocks_missed, block_number, timestamp)
      VALUES (@chain, @network, @producer,
        @round_id, @blocks_missed, @block_number, @timestamp)
    `).run(params);
  }

  // -- Fork Events --

  insertForkEvent(params: {
    chain: string;
    network: string;
    round_id: number | null;
    block_number: number;
    original_producer: string;
    replacement_producer: string;
    timestamp: string;
  }): void {
    this.db.prepare(`
      INSERT INTO fork_events (chain, network, round_id,
        block_number, original_producer, replacement_producer, timestamp)
      VALUES (@chain, @network, @round_id,
        @block_number, @original_producer, @replacement_producer, @timestamp)
    `).run(params);
  }

  // -- Schedule Changes --

  insertScheduleChange(params: {
    chain: string;
    network: string;
    schedule_version: number;
    producers_added: string;
    producers_removed: string;
    producer_list: string;
    block_number: number;
    timestamp: string;
  }): void {
    this.db.prepare(`
      INSERT INTO schedule_changes (chain, network, schedule_version,
        producers_added, producers_removed, producer_list,
        block_number, timestamp)
      VALUES (@chain, @network, @schedule_version,
        @producers_added, @producers_removed, @producer_list,
        @block_number, @timestamp)
    `).run(params);
  }

  // -- Producer Events (regproducer / unregprod) --

  insertProducerEvent(params: {
    chain: string;
    network: string;
    producer: string;
    action: string;
    block_number: number;
    timestamp: string;
  }): void {
    this.db.prepare(`
      INSERT INTO producer_events (chain, network, producer, action,
        block_number, timestamp)
      VALUES (@chain, @network, @producer, @action,
        @block_number, @timestamp)
    `).run(params);
  }

  getProducerEvents(
    chain: string,
    network: string,
    limit: number = 100,
    offset: number = 0,
    since: string | null = null,
    until: string | null = null
  ): any[] {
    let sql = 'SELECT * FROM producer_events WHERE chain = ? AND network = ?';
    const params: any[] = [chain, network];
    if (since) { sql += ' AND timestamp >= ?'; params.push(since); }
    if (until) { sql += ' AND timestamp <= ?'; params.push(until); }
    sql += ' ORDER BY timestamp DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);
    return this.db.prepare(sql).all(...params);
  }

  // -- Monitor State --

  getState(chain: string, network: string, key: string): string | null {
    const row = this.db.prepare(
      'SELECT value FROM monitor_state WHERE chain = ? AND network = ? AND key = ?'
    ).get(chain, network, key) as { value: string } | undefined;
    return row?.value ?? null;
  }

  setState(chain: string, network: string, key: string, value: string): void {
    this.db.prepare(`
      INSERT INTO monitor_state (chain, network, key, value, updated_at)
      VALUES (?, ?, ?, ?, datetime('now'))
      ON CONFLICT(chain, network, key)
      DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    `).run(chain, network, key, value);
  }

  // -- Query Methods (for API) --

  getRecentRounds(chain: string, network: string, limit: number = 100): any[] {
    return this.db.prepare(`
      SELECT * FROM rounds
      WHERE chain = ? AND network = ?
      ORDER BY round_number DESC
      LIMIT ?
    `).all(chain, network, limit);
  }

  getRoundProducers(roundId: number): any[] {
    return this.db.prepare(`
      SELECT * FROM round_producers
      WHERE round_id = ?
      ORDER BY position
    `).all(roundId);
  }

  getProducerStats(
    chain: string,
    network: string,
    producer: string,
    days: number = 30
  ): any {
    return this.db.prepare(`
      SELECT
        producer,
        COUNT(*) as total_rounds,
        SUM(CASE WHEN blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
        SUM(CASE WHEN blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
        SUM(blocks_expected) as total_blocks_expected,
        SUM(blocks_produced) as total_blocks_produced,
        SUM(blocks_missed) as total_blocks_missed,
        ROUND(
          100.0 * SUM(blocks_produced) / NULLIF(SUM(blocks_expected), 0),
          2
        ) as reliability_pct
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = ? AND r.network = ? AND rp.producer = ?
        AND r.created_at >= datetime('now', '-' || ? || ' days')
      GROUP BY producer
    `).get(chain, network, producer, days);
  }

  getAllProducerStats(chain: string, network: string, days: number = 30): any[] {
    return this.db.prepare(`
      SELECT
        producer,
        COUNT(*) as total_rounds,
        SUM(CASE WHEN blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
        SUM(CASE WHEN blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
        SUM(blocks_expected) as total_blocks_expected,
        SUM(blocks_produced) as total_blocks_produced,
        SUM(blocks_missed) as total_blocks_missed,
        ROUND(
          100.0 * SUM(blocks_produced) / NULLIF(SUM(blocks_expected), 0),
          2
        ) as reliability_pct
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = ? AND r.network = ?
        AND r.created_at >= datetime('now', '-' || ? || ' days')
      GROUP BY producer
      ORDER BY reliability_pct DESC, producer ASC
    `).all(chain, network, days);
  }

  getLongestOutages(chain: string, network: string, days: number = 30): Map<string, number> {
    // Get all round_producers ordered by round for outage streak calculation
    const rows = this.db.prepare(`
      SELECT rp.producer, rp.blocks_produced, r.round_number
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = ? AND r.network = ?
        AND r.created_at >= datetime('now', '-' || ? || ' days')
      ORDER BY rp.producer, r.round_number ASC
    `).all(chain, network, days) as Array<{ producer: string; blocks_produced: number; round_number: number }>;

    const result = new Map<string, number>();
    let currentProducer = '';
    let currentStreak = 0;
    let maxStreak = 0;

    for (const row of rows) {
      if (row.producer !== currentProducer) {
        if (currentProducer) {
          result.set(currentProducer, Math.max(maxStreak, currentStreak));
        }
        currentProducer = row.producer;
        currentStreak = 0;
        maxStreak = 0;
      }

      if (row.blocks_produced === 0) {
        currentStreak++;
      } else {
        maxStreak = Math.max(maxStreak, currentStreak);
        currentStreak = 0;
      }
    }

    // Don't forget the last producer
    if (currentProducer) {
      result.set(currentProducer, Math.max(maxStreak, currentStreak));
    }

    return result;
  }

  getMissedBlockEvents(
    chain: string,
    network: string,
    limit: number = 100,
    offset: number = 0,
    since: string | null = null,
    until: string | null = null
  ): any[] {
    let sql = 'SELECT * FROM missed_block_events WHERE chain = ? AND network = ?';
    const params: any[] = [chain, network];
    if (since) { sql += ' AND timestamp >= ?'; params.push(since); }
    if (until) { sql += ' AND timestamp <= ?'; params.push(until); }
    sql += ' ORDER BY timestamp DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);
    return this.db.prepare(sql).all(...params);
  }

  getForkEvents(
    chain: string,
    network: string,
    limit: number = 100,
    offset: number = 0,
    since: string | null = null,
    until: string | null = null
  ): any[] {
    let sql = 'SELECT * FROM fork_events WHERE chain = ? AND network = ?';
    const params: any[] = [chain, network];
    if (since) { sql += ' AND timestamp >= ?'; params.push(since); }
    if (until) { sql += ' AND timestamp <= ?'; params.push(until); }
    sql += ' ORDER BY timestamp DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);
    return this.db.prepare(sql).all(...params);
  }

  getScheduleChanges(
    chain: string,
    network: string,
    limit: number = 20
  ): any[] {
    return this.db.prepare(`
      SELECT * FROM schedule_changes
      WHERE chain = ? AND network = ?
      ORDER BY schedule_version DESC
      LIMIT ?
    `).all(chain, network, limit);
  }

  getWeeklySummaries(
    chain: string,
    network: string,
    weeks: number = 12
  ): any[] {
    return this.db.prepare(`
      SELECT * FROM weekly_summaries
      WHERE chain = ? AND network = ?
        AND week_start >= date('now', '-' || ? || ' days')
      ORDER BY week_start DESC, reliability_pct DESC
    `).all(chain, network, weeks * 7);
  }

  getMonthlySummaries(
    chain: string,
    network: string,
    months: number = 12
  ): any[] {
    return this.db.prepare(`
      SELECT * FROM monthly_summaries
      WHERE chain = ? AND network = ?
        AND month_start >= date('now', '-' || ? || ' months')
      ORDER BY month_start DESC, reliability_pct DESC
    `).all(chain, network, months);
  }

  // -- Batch operations for transactions --

  transaction<T>(fn: () => T): T {
    return this.db.transaction(fn)();
  }

  close(): void {
    this.db.close();
    log.info('Database closed');
  }
}
