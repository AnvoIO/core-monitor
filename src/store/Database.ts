import pg from 'pg';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'Database' });

export class Database {
  private pool: pg.Pool;

  constructor(connectionString: string) {
    log.info('Connecting to PostgreSQL');
    this.pool = new pg.Pool({
      connectionString,
      max: 10,
    });
  }

  async init(): Promise<void> {
    await this.migrate();
  }

  private async migrate(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS schema_version (
          version INTEGER PRIMARY KEY
        );
        INSERT INTO schema_version (version) VALUES (0) ON CONFLICT DO NOTHING;
      `);

      const result = await client.query('SELECT MAX(version) as version FROM schema_version');
      const currentVersion = result.rows[0].version;

      if (currentVersion < 1) {
        log.info('Running migration v1: initial schema');
        await client.query(`
          CREATE TABLE IF NOT EXISTS rounds (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            round_number INTEGER NOT NULL,
            schedule_version INTEGER NOT NULL,
            timestamp_start TEXT NOT NULL,
            timestamp_end TEXT,
            producers_scheduled INTEGER NOT NULL DEFAULT 0,
            producers_produced INTEGER NOT NULL DEFAULT 0,
            producers_missed INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );

          CREATE INDEX IF NOT EXISTS idx_rounds_chain_network
            ON rounds(chain, network, round_number DESC);

          CREATE INDEX IF NOT EXISTS idx_rounds_created
            ON rounds(created_at);

          CREATE TABLE IF NOT EXISTS round_producers (
            id SERIAL PRIMARY KEY,
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
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            producer TEXT NOT NULL,
            round_id INTEGER REFERENCES rounds(id) ON DELETE SET NULL,
            blocks_missed INTEGER NOT NULL,
            block_number INTEGER,
            timestamp TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );

          CREATE INDEX IF NOT EXISTS idx_missed_events_chain
            ON missed_block_events(chain, network, timestamp DESC);

          CREATE INDEX IF NOT EXISTS idx_missed_events_producer
            ON missed_block_events(producer, timestamp DESC);

          CREATE TABLE IF NOT EXISTS fork_events (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            round_id INTEGER REFERENCES rounds(id) ON DELETE SET NULL,
            block_number INTEGER NOT NULL,
            original_producer TEXT NOT NULL,
            replacement_producer TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );

          CREATE INDEX IF NOT EXISTS idx_fork_events_chain
            ON fork_events(chain, network, timestamp DESC);

          CREATE TABLE IF NOT EXISTS schedule_changes (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            schedule_version INTEGER NOT NULL,
            producers_added TEXT,
            producers_removed TEXT,
            producer_list TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );

          CREATE INDEX IF NOT EXISTS idx_schedule_changes_chain
            ON schedule_changes(chain, network, schedule_version DESC);

          CREATE TABLE IF NOT EXISTS weekly_summaries (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(chain, network, week_start, producer)
          );

          CREATE TABLE IF NOT EXISTS monthly_summaries (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(chain, network, month_start, producer)
          );

          CREATE TABLE IF NOT EXISTS monitor_state (
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chain, network, key)
          );

          INSERT INTO schema_version (version) VALUES (1);
        `);
      }

      if (currentVersion < 2) {
        log.info('Running migration v2: producer_events table');
        await client.query(`
          CREATE TABLE IF NOT EXISTS producer_events (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            producer TEXT NOT NULL,
            action TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );

          CREATE INDEX IF NOT EXISTS idx_producer_events_chain
            ON producer_events(chain, network, timestamp DESC);

          INSERT INTO schema_version (version) VALUES (2);
        `);
      }

      if (currentVersion < 3) {
        log.info('Running migration v3: performance indexes');
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_rounds_chain_ts
            ON rounds(chain, network, timestamp_start DESC);
          CREATE INDEX IF NOT EXISTS idx_rounds_chain_id
            ON rounds(chain, network, id DESC);
          CREATE INDEX IF NOT EXISTS idx_rp_roundid_stats
            ON round_producers(round_id) INCLUDE (producer, blocks_produced, blocks_expected, blocks_missed);
          CREATE INDEX IF NOT EXISTS idx_rp_round_producer
            ON round_producers(round_id, producer, blocks_produced);
          CREATE INDEX IF NOT EXISTS idx_missed_chain_ts
            ON missed_block_events(chain, network, timestamp DESC);

          INSERT INTO schema_version (version) VALUES (3);
        `);
      }

      if (currentVersion < 4) {
        log.info('Running migration v4: deduplication constraints for catchup writer');

        // Remove duplicate rows before adding unique constraints.
        // Keep the row with the lowest id (earliest insert).
        // Also clean orphaned round_producers and missed_block_events.
        await client.query(`
          DELETE FROM round_producers WHERE round_id IN (
            SELECT a.id FROM rounds a JOIN rounds b
            ON a.chain = b.chain AND a.network = b.network
              AND a.round_number = b.round_number AND a.schedule_version = b.schedule_version
              AND a.id > b.id
          );

          DELETE FROM missed_block_events WHERE round_id IN (
            SELECT a.id FROM rounds a JOIN rounds b
            ON a.chain = b.chain AND a.network = b.network
              AND a.round_number = b.round_number AND a.schedule_version = b.schedule_version
              AND a.id > b.id
          );

          DELETE FROM rounds a USING rounds b
          WHERE a.id > b.id
            AND a.chain = b.chain AND a.network = b.network
            AND a.round_number = b.round_number AND a.schedule_version = b.schedule_version;

          DELETE FROM schedule_changes a USING schedule_changes b
          WHERE a.id > b.id
            AND a.chain = b.chain AND a.network = b.network
            AND a.schedule_version = b.schedule_version;

          DELETE FROM producer_events a USING producer_events b
          WHERE a.id > b.id
            AND a.chain = b.chain AND a.network = b.network
            AND a.producer = b.producer AND a.action = b.action
            AND a.block_number = b.block_number;
        `);

        // Clean stale catchup state from previous broken runs
        await client.query(`
          DELETE FROM monitor_state WHERE key LIKE 'catchup_%';
        `);

        await client.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS idx_rounds_dedup
            ON rounds(chain, network, round_number, schedule_version);
          CREATE UNIQUE INDEX IF NOT EXISTS idx_schedule_changes_dedup
            ON schedule_changes(chain, network, schedule_version);
          CREATE UNIQUE INDEX IF NOT EXISTS idx_producer_events_dedup
            ON producer_events(chain, network, producer, action, block_number);

          INSERT INTO schema_version (version) VALUES (4);
        `);
      }

      if (currentVersion < 5) {
        log.info('Running migration v5: add producers_key_updates column to schedule_changes');

        await client.query(`
          ALTER TABLE schedule_changes ADD COLUMN IF NOT EXISTS producers_key_updates TEXT;
          INSERT INTO schema_version (version) VALUES (5);
        `);
      }

      if (currentVersion < 6) {
        log.info('Running migration v6: daily summary tables for query performance');

        await client.query(`
          CREATE TABLE IF NOT EXISTS producer_stats_daily (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            day DATE NOT NULL,
            producer TEXT NOT NULL,
            rounds_scheduled INTEGER NOT NULL DEFAULT 0,
            rounds_produced INTEGER NOT NULL DEFAULT 0,
            rounds_missed INTEGER NOT NULL DEFAULT 0,
            blocks_expected INTEGER NOT NULL DEFAULT 0,
            blocks_produced INTEGER NOT NULL DEFAULT 0,
            blocks_missed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(chain, network, day, producer)
          );

          CREATE INDEX IF NOT EXISTS idx_psd_chain_day
            ON producer_stats_daily(chain, network, day);

          CREATE TABLE IF NOT EXISTS outage_events (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            producer TEXT NOT NULL,
            rounds_count INTEGER NOT NULL,
            start_round_number INTEGER NOT NULL,
            end_round_number INTEGER NOT NULL,
            schedule_version INTEGER NOT NULL,
            timestamp_start TEXT NOT NULL,
            timestamp_end TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );

          CREATE INDEX IF NOT EXISTS idx_outage_chain_producer
            ON outage_events(chain, network, producer, timestamp_start DESC);
          CREATE INDEX IF NOT EXISTS idx_outage_chain_ts
            ON outage_events(chain, network, timestamp_start DESC);

          CREATE TABLE IF NOT EXISTS round_counts_daily (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            network TEXT NOT NULL,
            day DATE NOT NULL,
            total_rounds INTEGER NOT NULL DEFAULT 0,
            perfect_rounds INTEGER NOT NULL DEFAULT 0,
            issue_rounds INTEGER NOT NULL DEFAULT 0,
            UNIQUE(chain, network, day)
          );

          INSERT INTO schema_version (version) VALUES (6);
        `);

        // Backfill producer_stats_daily from existing data
        log.info('Backfilling producer_stats_daily from existing rounds...');
        await client.query(`
          INSERT INTO producer_stats_daily (chain, network, day, producer,
            rounds_scheduled, rounds_produced, rounds_missed,
            blocks_expected, blocks_produced, blocks_missed)
          SELECT r.chain, r.network,
            DATE(r.timestamp_start) as day,
            rp.producer,
            COUNT(*),
            SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END),
            SUM(rp.blocks_expected),
            SUM(rp.blocks_produced),
            SUM(rp.blocks_missed)
          FROM round_producers rp
          JOIN rounds r ON rp.round_id = r.id
          WHERE r.timestamp_start != ''
          GROUP BY r.chain, r.network, DATE(r.timestamp_start), rp.producer
          ON CONFLICT (chain, network, day, producer) DO UPDATE SET
            rounds_scheduled = EXCLUDED.rounds_scheduled,
            rounds_produced = EXCLUDED.rounds_produced,
            rounds_missed = EXCLUDED.rounds_missed,
            blocks_expected = EXCLUDED.blocks_expected,
            blocks_produced = EXCLUDED.blocks_produced,
            blocks_missed = EXCLUDED.blocks_missed
        `);

        // Backfill round_counts_daily
        log.info('Backfilling round_counts_daily from existing rounds...');
        await client.query(`
          INSERT INTO round_counts_daily (chain, network, day, total_rounds, perfect_rounds, issue_rounds)
          SELECT chain, network, DATE(timestamp_start),
            COUNT(*),
            SUM(CASE WHEN producers_missed = 0 THEN 1 ELSE 0 END)::INTEGER,
            SUM(CASE WHEN producers_missed > 0 THEN 1 ELSE 0 END)::INTEGER
          FROM rounds
          WHERE timestamp_start != ''
          GROUP BY chain, network, DATE(timestamp_start)
          ON CONFLICT (chain, network, day) DO UPDATE SET
            total_rounds = EXCLUDED.total_rounds,
            perfect_rounds = EXCLUDED.perfect_rounds,
            issue_rounds = EXCLUDED.issue_rounds
        `);

        // Backfill outage_events — streak detection in SQL using window functions
        log.info('Backfilling outage_events from existing rounds...');
        await client.query(`
          WITH ordered AS (
            SELECT r.chain, r.network, rp.producer, r.round_number, r.schedule_version,
              r.timestamp_start, r.timestamp_end, rp.blocks_produced,
              CASE WHEN rp.blocks_produced = 0 THEN 0 ELSE 1 END as is_producing
            FROM round_producers rp
            JOIN rounds r ON rp.round_id = r.id
            WHERE r.timestamp_start != ''
          ),
          groups AS (
            SELECT *, SUM(is_producing) OVER (
              PARTITION BY chain, network, producer
              ORDER BY timestamp_start
              ROWS UNBOUNDED PRECEDING
            ) as streak_group
            FROM ordered
          ),
          streaks AS (
            SELECT chain, network, producer,
              COUNT(*) as rounds_count,
              MIN(round_number) as start_round_number,
              MAX(round_number) as end_round_number,
              MIN(schedule_version) as schedule_version,
              MIN(timestamp_start) as timestamp_start,
              MAX(timestamp_end) as timestamp_end
            FROM groups
            WHERE is_producing = 0
            GROUP BY chain, network, producer, streak_group
          )
          INSERT INTO outage_events (chain, network, producer, rounds_count,
            start_round_number, end_round_number, schedule_version,
            timestamp_start, timestamp_end)
          SELECT chain, network, producer, rounds_count,
            start_round_number, end_round_number, schedule_version,
            timestamp_start, timestamp_end
          FROM streaks
          WHERE rounds_count >= 1
        `);

        log.info('Migration v6 complete — summary tables populated');
      }

      log.info({ version: 6 }, 'Database schema up to date');
    } finally {
      client.release();
    }
  }

  // -- Rounds --

  async insertRound(params: {
    chain: string;
    network: string;
    round_number: number;
    schedule_version: number;
    timestamp_start: string;
    timestamp_end: string;
    producers_scheduled: number;
    producers_produced: number;
    producers_missed: number;
  }, client?: pg.PoolClient): Promise<number> {
    const q = client || this.pool;
    const result = await q.query(
      `INSERT INTO rounds (chain, network, round_number, schedule_version,
        timestamp_start, timestamp_end, producers_scheduled,
        producers_produced, producers_missed)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (chain, network, round_number, schedule_version) DO NOTHING
      RETURNING id`,
      [params.chain, params.network, params.round_number, params.schedule_version,
       params.timestamp_start, params.timestamp_end, params.producers_scheduled,
       params.producers_produced, params.producers_missed]
    );
    return result.rows[0]?.id ?? null;
  }

  async insertRoundProducer(params: {
    round_id: number | null;
    producer: string;
    position: number;
    blocks_expected: number;
    blocks_produced: number;
    blocks_missed: number;
    first_block: number | null;
    last_block: number | null;
  }): Promise<void> {
    if (params.round_id === null) return; // round was a duplicate, skip
    await this.pool.query(
      `INSERT INTO round_producers (round_id, producer, position,
        blocks_expected, blocks_produced, blocks_missed,
        first_block, last_block)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [params.round_id, params.producer, params.position,
       params.blocks_expected, params.blocks_produced, params.blocks_missed,
       params.first_block, params.last_block]
    );
  }

  // -- Missed Block Events --

  async insertMissedBlockEvent(params: {
    chain: string;
    network: string;
    producer: string;
    round_id: number | null;
    blocks_missed: number;
    block_number: number | null;
    timestamp: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO missed_block_events (chain, network, producer,
        round_id, blocks_missed, block_number, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [params.chain, params.network, params.producer,
       params.round_id, params.blocks_missed, params.block_number, params.timestamp]
    );
  }

  // -- Batch Inserts (for catchup writer performance) --

  async insertRoundProducersBatch(
    rows: Array<{
      round_id: number;
      producer: string;
      position: number;
      blocks_expected: number;
      blocks_produced: number;
      blocks_missed: number;
      first_block: number | null;
      last_block: number | null;
    }>,
    client?: pg.PoolClient
  ): Promise<void> {
    if (rows.length === 0) return;
    const cols = 8;
    const values: any[] = [];
    const placeholders: string[] = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const b = i * cols;
      placeholders.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8})`);
      values.push(r.round_id, r.producer, r.position, r.blocks_expected,
                  r.blocks_produced, r.blocks_missed, r.first_block, r.last_block);
    }
    const q = client || this.pool;
    await q.query(
      `INSERT INTO round_producers (round_id, producer, position,
        blocks_expected, blocks_produced, blocks_missed, first_block, last_block)
      VALUES ${placeholders.join(',')}`,
      values
    );
  }

  async insertMissedBlockEventsBatch(
    rows: Array<{
      chain: string;
      network: string;
      producer: string;
      round_id: number | null;
      blocks_missed: number;
      block_number: number | null;
      timestamp: string;
    }>,
    client?: pg.PoolClient
  ): Promise<void> {
    if (rows.length === 0) return;
    const cols = 7;
    const values: any[] = [];
    const placeholders: string[] = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const b = i * cols;
      placeholders.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7})`);
      values.push(r.chain, r.network, r.producer, r.round_id, r.blocks_missed, r.block_number, r.timestamp);
    }
    const q = client || this.pool;
    await q.query(
      `INSERT INTO missed_block_events (chain, network, producer,
        round_id, blocks_missed, block_number, timestamp)
      VALUES ${placeholders.join(',')}`,
      values
    );
  }

  // -- Fork Events --

  async insertForkEvent(params: {
    chain: string;
    network: string;
    round_id: number | null;
    block_number: number;
    original_producer: string;
    replacement_producer: string;
    timestamp: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO fork_events (chain, network, round_id,
        block_number, original_producer, replacement_producer, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [params.chain, params.network, params.round_id,
       params.block_number, params.original_producer, params.replacement_producer, params.timestamp]
    );
  }

  // -- Schedule Changes --

  async insertScheduleChange(params: {
    chain: string;
    network: string;
    schedule_version: number;
    producers_added: string;
    producers_removed: string;
    producers_key_updates: string;
    producer_list: string;
    block_number: number;
    timestamp: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO schedule_changes (chain, network, schedule_version,
        producers_added, producers_removed, producers_key_updates, producer_list,
        block_number, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (chain, network, schedule_version) DO NOTHING`,
      [params.chain, params.network, params.schedule_version,
       params.producers_added, params.producers_removed, params.producers_key_updates,
       params.producer_list, params.block_number, params.timestamp]
    );
  }

  // -- Producer Events --

  async insertProducerEvent(params: {
    chain: string;
    network: string;
    producer: string;
    action: string;
    block_number: number;
    timestamp: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO producer_events (chain, network, producer, action,
        block_number, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (chain, network, producer, action, block_number) DO NOTHING`,
      [params.chain, params.network, params.producer, params.action,
       params.block_number, params.timestamp]
    );
  }

  getProducerEvents(
    chain: string, network: string,
    limit: number = 100, offset: number = 0,
    since: string | null = null, until: string | null = null
  ): Promise<any[]> {
    const params: any[] = [chain, network];
    let sql = 'SELECT * FROM producer_events WHERE chain = $1 AND network = $2';
    let idx = 3;
    if (since) { sql += ` AND timestamp >= $${idx++}`; params.push(since); }
    if (until) { sql += ` AND timestamp <= $${idx++}`; params.push(until); }
    sql += ` ORDER BY timestamp DESC LIMIT $${idx++} OFFSET $${idx++}`;
    params.push(limit, offset);
    return this.pool.query(sql, params).then(r => r.rows);
  }

  // -- Monitor State --

  async getState(chain: string, network: string, key: string, client?: pg.PoolClient): Promise<string | null> {
    const q = client || this.pool;
    const result = await q.query(
      'SELECT value FROM monitor_state WHERE chain = $1 AND network = $2 AND key = $3',
      [chain, network, key]
    );
    return result.rows[0]?.value ?? null;
  }

  async setState(chain: string, network: string, key: string, value: string, client?: pg.PoolClient): Promise<void> {
    const q = client || this.pool;
    await q.query(
      `INSERT INTO monitor_state (chain, network, key, value, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT(chain, network, key)
      DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
      [chain, network, key, value]
    );
  }

  // -- Query Methods (for API) --

  async getRecentRounds(chain: string, network: string, limit: number = 100, offset: number = 0, since: string | null = null): Promise<any[]> {
    const params: any[] = [chain, network];
    let sql = 'SELECT * FROM rounds WHERE chain = $1 AND network = $2';
    let idx = 3;
    if (since) { sql += ` AND timestamp_start >= $${idx++}`; params.push(since); }
    sql += ` ORDER BY timestamp_start DESC LIMIT $${idx++} OFFSET $${idx++}`;
    params.push(limit, offset);
    const result = await this.pool.query(sql, params);
    return result.rows;
  }

  async getLatestRoundProducerDetails(chain: string, network: string): Promise<Map<string, { produced: number; expected: number }>> {
    const result = await this.pool.query(
      `SELECT rp.producer, rp.blocks_produced, rp.blocks_expected
       FROM round_producers rp
       JOIN rounds r ON rp.round_id = r.id
       WHERE r.chain = $1 AND r.network = $2
         AND r.id = (SELECT id FROM rounds WHERE chain = $1 AND network = $2 ORDER BY timestamp_start DESC LIMIT 1)`,
      [chain, network]
    );
    const map = new Map<string, { produced: number; expected: number }>();
    for (const row of result.rows) {
      map.set(row.producer, { produced: row.blocks_produced, expected: row.blocks_expected });
    }
    return map;
  }

  async getRoundCounts(chain: string, network: string, since: string | null = null): Promise<{ total: number; perfect: number; issues: number }> {
    const sinceDate = since ? since.substring(0, 10) : null;
    const today = new Date().toISOString().substring(0, 10);

    // Historical from summary + today from live
    const params: any[] = [chain, network, today];
    let dayFilter = '';
    if (sinceDate) {
      dayFilter = 'AND day >= $4::DATE';
      params.push(sinceDate);
    }

    const result = await this.pool.query(
      `WITH historical AS (
        SELECT COALESCE(SUM(total_rounds), 0) as total,
          COALESCE(SUM(perfect_rounds), 0) as perfect,
          COALESCE(SUM(issue_rounds), 0) as issues
        FROM round_counts_daily
        WHERE chain = $1 AND network = $2 AND day < $3::DATE ${dayFilter}
      ),
      live AS (
        SELECT COUNT(*) as total,
          SUM(CASE WHEN producers_missed = 0 THEN 1 ELSE 0 END) as perfect,
          SUM(CASE WHEN producers_missed > 0 THEN 1 ELSE 0 END) as issues
        FROM rounds
        WHERE chain = $1 AND network = $2 AND timestamp_start >= $3::TEXT
      )
      SELECT
        (h.total + COALESCE(l.total, 0))::INTEGER as total,
        (h.perfect + COALESCE(l.perfect, 0))::INTEGER as perfect,
        (h.issues + COALESCE(l.issues, 0))::INTEGER as issues
      FROM historical h, live l`,
      params
    );
    const row = result.rows[0];
    return {
      total: parseInt(row.total) || 0,
      perfect: parseInt(row.perfect) || 0,
      issues: parseInt(row.issues) || 0,
    };
  }

  async getEarliestRounds(chain: string, network: string, limit: number = 1): Promise<any[]> {
    const result = await this.pool.query(
      `SELECT * FROM rounds WHERE chain = $1 AND network = $2
       ORDER BY round_number ASC LIMIT $3`,
      [chain, network, limit]
    );
    return result.rows;
  }

  async getRoundProducers(roundId: number): Promise<any[]> {
    const result = await this.pool.query(
      'SELECT * FROM round_producers WHERE round_id = $1 ORDER BY position',
      [roundId]
    );
    return result.rows;
  }

  async getProducerStats(chain: string, network: string, producer: string, days: number = 30): Promise<any> {
    const result = await this.pool.query(
      `SELECT
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
      WHERE r.chain = $1 AND r.network = $2 AND rp.producer = $3
        AND r.timestamp_start >= to_char(NOW() - ($4 || ' days')::INTERVAL, 'YYYY-MM-DD"T"HH24:MI:SS.MS')
      GROUP BY producer`,
      [chain, network, producer, days]
    );
    return result.rows[0];
  }

  async getAllProducerStats(chain: string, network: string, days: number | null = 30, since: string | null = null): Promise<any[]> {
    const sinceDate = since
      ? since.substring(0, 10)
      : new Date(Date.now() - (days || 30) * 86400000).toISOString().substring(0, 10);
    const today = new Date().toISOString().substring(0, 10);

    // Historical complete days from summary table + today's partial day from raw data
    const result = await this.pool.query(
      `WITH historical AS (
        SELECT producer,
          SUM(rounds_scheduled) as total_rounds,
          SUM(rounds_produced) as rounds_produced,
          SUM(rounds_missed) as rounds_missed,
          SUM(blocks_expected) as total_blocks_expected,
          SUM(blocks_produced) as total_blocks_produced,
          SUM(blocks_missed) as total_blocks_missed
        FROM producer_stats_daily
        WHERE chain = $1 AND network = $2 AND day >= $3::DATE AND day < $4::DATE
        GROUP BY producer
      ),
      live AS (
        SELECT rp.producer,
          COUNT(*) as total_rounds,
          SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
          SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
          SUM(rp.blocks_expected) as total_blocks_expected,
          SUM(rp.blocks_produced) as total_blocks_produced,
          SUM(rp.blocks_missed) as total_blocks_missed
        FROM round_producers rp
        JOIN rounds r ON rp.round_id = r.id
        WHERE r.chain = $1 AND r.network = $2 AND r.timestamp_start >= $4::TEXT
        GROUP BY rp.producer
      ),
      combined AS (
        SELECT
          COALESCE(h.producer, l.producer) as producer,
          COALESCE(h.total_rounds, 0) + COALESCE(l.total_rounds, 0) as total_rounds,
          COALESCE(h.rounds_produced, 0) + COALESCE(l.rounds_produced, 0) as rounds_produced,
          COALESCE(h.rounds_missed, 0) + COALESCE(l.rounds_missed, 0) as rounds_missed,
          COALESCE(h.total_blocks_expected, 0) + COALESCE(l.total_blocks_expected, 0) as total_blocks_expected,
          COALESCE(h.total_blocks_produced, 0) + COALESCE(l.total_blocks_produced, 0) as total_blocks_produced,
          COALESCE(h.total_blocks_missed, 0) + COALESCE(l.total_blocks_missed, 0) as total_blocks_missed
        FROM historical h
        FULL OUTER JOIN live l ON h.producer = l.producer
      )
      SELECT producer, total_rounds, rounds_produced, rounds_missed,
        total_blocks_expected, total_blocks_produced, total_blocks_missed,
        ROUND(100.0 * total_blocks_produced / NULLIF(total_blocks_expected, 0), 2) as reliability_pct
      FROM combined
      ORDER BY reliability_pct DESC, producer ASC`,
      [chain, network, sinceDate, today]
    );
    return result.rows;
  }

  async getLongestOutages(chain: string, network: string, days: number | null = 30, since: string | null = null): Promise<Map<string, number>> {
    const sinceDate = since || new Date(Date.now() - (days || 30) * 86400000).toISOString();

    // Historical completed outages from summary table
    const historicalResult = await this.pool.query(
      `SELECT producer, MAX(rounds_count) as longest
      FROM outage_events
      WHERE chain = $1 AND network = $2 AND timestamp_start >= $3
      GROUP BY producer`,
      [chain, network, sinceDate]
    );

    const outages = new Map<string, number>();
    for (const row of historicalResult.rows) {
      outages.set(row.producer, parseInt(row.longest));
    }

    // Current-day live scan for any in-progress or today-only streaks
    const today = new Date().toISOString().substring(0, 10);
    const liveResult = await this.pool.query(
      `SELECT rp.producer, rp.blocks_produced
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = $1 AND r.network = $2 AND r.timestamp_start >= $3::TEXT
      ORDER BY rp.producer, r.timestamp_start ASC`,
      [chain, network, today]
    );

    let currentProducer = '';
    let currentStreak = 0;
    let maxStreak = 0;
    for (const row of liveResult.rows) {
      if (row.producer !== currentProducer) {
        if (currentProducer) {
          const live = Math.max(maxStreak, currentStreak);
          outages.set(currentProducer, Math.max(outages.get(currentProducer) || 0, live));
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
    if (currentProducer) {
      const live = Math.max(maxStreak, currentStreak);
      outages.set(currentProducer, Math.max(outages.get(currentProducer) || 0, live));
    }

    return outages;
  }

  async getProducerOutageDetails(
    chain: string, network: string, producer: string,
    days: number | null = 30, since: string | null = null
  ): Promise<{
    total_outages: number;
    longest_streak: number;
    longest_streak_start: string | null;
    longest_streak_end: string | null;
    average_streak: number;
    outages: Array<{ rounds: number; start: string; end: string; start_round: number; end_round: number; schedule_version: number }>;
  }> {
    const params: any[] = [chain, network, producer];
    let timeFilter: string;
    if (since) {
      timeFilter = `AND r.timestamp_start >= $4`;
      params.push(since);
    } else {
      timeFilter = `AND r.timestamp_start >= to_char(NOW() - ($4 || ' days')::INTERVAL, 'YYYY-MM-DD"T"HH24:MI:SS.MS')`;
      params.push(days || 30);
    }
    const result = await this.pool.query(
      `SELECT rp.blocks_produced, r.round_number, r.schedule_version, r.timestamp_start, r.timestamp_end
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = $1 AND r.network = $2 AND rp.producer = $3
        ${timeFilter}
      ORDER BY r.timestamp_start ASC`,
      params
    );

    const outages: Array<{ rounds: number; start: string; end: string; start_round: number; end_round: number; schedule_version: number }> = [];
    let streakLen = 0;
    let streakStart = '';
    let streakEnd = '';
    let streakStartRound = 0;
    let streakEndRound = 0;
    let streakScheduleVersion = 0;

    for (const row of result.rows) {
      if (row.blocks_produced === 0) {
        if (streakLen === 0) {
          streakStart = row.timestamp_start;
          streakStartRound = row.round_number;
          streakScheduleVersion = row.schedule_version;
        }
        streakLen++;
        streakEnd = row.timestamp_end;
        streakEndRound = row.round_number;
      } else if (streakLen > 0) {
        outages.push({ rounds: streakLen, start: streakStart, end: streakEnd, start_round: streakStartRound, end_round: streakEndRound, schedule_version: streakScheduleVersion });
        streakLen = 0;
      }
    }
    if (streakLen > 0) {
      outages.push({ rounds: streakLen, start: streakStart, end: streakEnd, start_round: streakStartRound, end_round: streakEndRound, schedule_version: streakScheduleVersion });
    }

    let longest_streak = 0;
    let longest_streak_start: string | null = null;
    let longest_streak_end: string | null = null;
    let totalRounds = 0;
    for (const o of outages) {
      totalRounds += o.rounds;
      if (o.rounds > longest_streak) {
        longest_streak = o.rounds;
        longest_streak_start = o.start;
        longest_streak_end = o.end;
      }
    }

    return {
      total_outages: outages.length,
      longest_streak,
      longest_streak_start,
      longest_streak_end,
      average_streak: outages.length > 0 ? Math.round(totalRounds / outages.length * 10) / 10 : 0,
      outages: outages.sort((a, b) => b.rounds - a.rounds),
    };
  }

  async getMissedBlockEvents(
    chain: string, network: string,
    limit: number = 100, offset: number = 0,
    since: string | null = null, until: string | null = null
  ): Promise<any[]> {
    const params: any[] = [chain, network];
    let sql = 'SELECT * FROM missed_block_events WHERE chain = $1 AND network = $2';
    let idx = 3;
    if (since) { sql += ` AND timestamp >= $${idx++}`; params.push(since); }
    if (until) { sql += ` AND timestamp <= $${idx++}`; params.push(until); }
    sql += ` ORDER BY timestamp DESC LIMIT $${idx++} OFFSET $${idx++}`;
    params.push(limit, offset);
    const result = await this.pool.query(sql, params);
    return result.rows;
  }

  async getForkEvents(
    chain: string, network: string,
    limit: number = 100, offset: number = 0,
    since: string | null = null, until: string | null = null
  ): Promise<any[]> {
    const params: any[] = [chain, network];
    let sql = 'SELECT * FROM fork_events WHERE chain = $1 AND network = $2';
    let idx = 3;
    if (since) { sql += ` AND timestamp >= $${idx++}`; params.push(since); }
    if (until) { sql += ` AND timestamp <= $${idx++}`; params.push(until); }
    sql += ` ORDER BY timestamp DESC LIMIT $${idx++} OFFSET $${idx++}`;
    params.push(limit, offset);
    const result = await this.pool.query(sql, params);
    return result.rows;
  }

  async getScheduleChanges(chain: string, network: string, limit: number = 20): Promise<any[]> {
    const result = await this.pool.query(
      `SELECT * FROM schedule_changes WHERE chain = $1 AND network = $2
       ORDER BY schedule_version DESC LIMIT $3`,
      [chain, network, limit]
    );
    return result.rows;
  }

  async getWeeklySummaries(chain: string, network: string, weeks: number = 12): Promise<any[]> {
    const result = await this.pool.query(
      `SELECT * FROM weekly_summaries WHERE chain = $1 AND network = $2
        AND week_start >= (NOW() - ($3 || ' days')::INTERVAL)::DATE
       ORDER BY week_start DESC, reliability_pct DESC`,
      [chain, network, weeks * 7]
    );
    return result.rows;
  }

  async getMonthlySummaries(chain: string, network: string, months: number = 12): Promise<any[]> {
    const result = await this.pool.query(
      `SELECT * FROM monthly_summaries WHERE chain = $1 AND network = $2
        AND month_start >= (NOW() - ($3 || ' months')::INTERVAL)::DATE
       ORDER BY month_start DESC, reliability_pct DESC`,
      [chain, network, months]
    );
    return result.rows;
  }

  // -- Summary Table Upserts --

  async upsertProducerStatsDaily(params: {
    chain: string;
    network: string;
    day: string;
    producer: string;
    blocks_expected: number;
    blocks_produced: number;
    blocks_missed: number;
    produced: boolean; // did producer produce any blocks this round?
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO producer_stats_daily (chain, network, day, producer,
        rounds_scheduled, rounds_produced, rounds_missed,
        blocks_expected, blocks_produced, blocks_missed)
      VALUES ($1, $2, $3::DATE, $4, 1, $5, $6, $7, $8, $9)
      ON CONFLICT (chain, network, day, producer) DO UPDATE SET
        rounds_scheduled = producer_stats_daily.rounds_scheduled + 1,
        rounds_produced = producer_stats_daily.rounds_produced + EXCLUDED.rounds_produced,
        rounds_missed = producer_stats_daily.rounds_missed + EXCLUDED.rounds_missed,
        blocks_expected = producer_stats_daily.blocks_expected + EXCLUDED.blocks_expected,
        blocks_produced = producer_stats_daily.blocks_produced + EXCLUDED.blocks_produced,
        blocks_missed = producer_stats_daily.blocks_missed + EXCLUDED.blocks_missed`,
      [params.chain, params.network, params.day, params.producer,
       params.produced ? 1 : 0, params.produced ? 0 : 1,
       params.blocks_expected, params.blocks_produced, params.blocks_missed]
    );
  }

  async upsertRoundCountsDaily(params: {
    chain: string;
    network: string;
    day: string;
    perfect: boolean;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO round_counts_daily (chain, network, day, total_rounds, perfect_rounds, issue_rounds)
      VALUES ($1, $2, $3::DATE, 1, $4, $5)
      ON CONFLICT (chain, network, day) DO UPDATE SET
        total_rounds = round_counts_daily.total_rounds + 1,
        perfect_rounds = round_counts_daily.perfect_rounds + EXCLUDED.perfect_rounds,
        issue_rounds = round_counts_daily.issue_rounds + EXCLUDED.issue_rounds`,
      [params.chain, params.network, params.day,
       params.perfect ? 1 : 0, params.perfect ? 0 : 1]
    );
  }

  async insertOutageEvent(params: {
    chain: string;
    network: string;
    producer: string;
    rounds_count: number;
    start_round_number: number;
    end_round_number: number;
    schedule_version: number;
    timestamp_start: string;
    timestamp_end: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO outage_events (chain, network, producer, rounds_count,
        start_round_number, end_round_number, schedule_version,
        timestamp_start, timestamp_end)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [params.chain, params.network, params.producer, params.rounds_count,
       params.start_round_number, params.end_round_number, params.schedule_version,
       params.timestamp_start, params.timestamp_end]
    );
  }

  /** Reconcile a single day's summaries from raw data (idempotent). */
  async reconcileDay(chain: string, network: string, day: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO producer_stats_daily (chain, network, day, producer,
        rounds_scheduled, rounds_produced, rounds_missed,
        blocks_expected, blocks_produced, blocks_missed)
      SELECT r.chain, r.network, $3::DATE, rp.producer,
        COUNT(*),
        SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END),
        SUM(rp.blocks_expected), SUM(rp.blocks_produced), SUM(rp.blocks_missed)
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = $1 AND r.network = $2 AND DATE(r.timestamp_start) = $3::DATE
      GROUP BY r.chain, r.network, rp.producer
      ON CONFLICT (chain, network, day, producer) DO UPDATE SET
        rounds_scheduled = EXCLUDED.rounds_scheduled,
        rounds_produced = EXCLUDED.rounds_produced,
        rounds_missed = EXCLUDED.rounds_missed,
        blocks_expected = EXCLUDED.blocks_expected,
        blocks_produced = EXCLUDED.blocks_produced,
        blocks_missed = EXCLUDED.blocks_missed`,
      [chain, network, day]
    );

    await this.pool.query(
      `INSERT INTO round_counts_daily (chain, network, day, total_rounds, perfect_rounds, issue_rounds)
      SELECT chain, network, $3::DATE,
        COUNT(*),
        SUM(CASE WHEN producers_missed = 0 THEN 1 ELSE 0 END)::INTEGER,
        SUM(CASE WHEN producers_missed > 0 THEN 1 ELSE 0 END)::INTEGER
      FROM rounds
      WHERE chain = $1 AND network = $2 AND DATE(timestamp_start) = $3::DATE
      GROUP BY chain, network
      ON CONFLICT (chain, network, day) DO UPDATE SET
        total_rounds = EXCLUDED.total_rounds,
        perfect_rounds = EXCLUDED.perfect_rounds,
        issue_rounds = EXCLUDED.issue_rounds`,
      [chain, network, day]
    );
  }

  // -- Batch operations --

  async transaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
    log.info('Database pool closed');
  }
}
