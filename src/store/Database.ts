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

      log.info({ version: 2 }, 'Database schema up to date');
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
  }): Promise<number> {
    const result = await this.pool.query(
      `INSERT INTO rounds (chain, network, round_number, schedule_version,
        timestamp_start, timestamp_end, producers_scheduled,
        producers_produced, producers_missed)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING id`,
      [params.chain, params.network, params.round_number, params.schedule_version,
       params.timestamp_start, params.timestamp_end, params.producers_scheduled,
       params.producers_produced, params.producers_missed]
    );
    return result.rows[0].id;
  }

  async insertRoundProducer(params: {
    round_id: number;
    producer: string;
    position: number;
    blocks_expected: number;
    blocks_produced: number;
    blocks_missed: number;
    first_block: number | null;
    last_block: number | null;
  }): Promise<void> {
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
    producer_list: string;
    block_number: number;
    timestamp: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO schedule_changes (chain, network, schedule_version,
        producers_added, producers_removed, producer_list,
        block_number, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [params.chain, params.network, params.schedule_version,
       params.producers_added, params.producers_removed, params.producer_list,
       params.block_number, params.timestamp]
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
      VALUES ($1, $2, $3, $4, $5, $6)`,
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

  async getState(chain: string, network: string, key: string): Promise<string | null> {
    const result = await this.pool.query(
      'SELECT value FROM monitor_state WHERE chain = $1 AND network = $2 AND key = $3',
      [chain, network, key]
    );
    return result.rows[0]?.value ?? null;
  }

  async setState(chain: string, network: string, key: string, value: string): Promise<void> {
    await this.pool.query(
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
    sql += ` ORDER BY id DESC LIMIT $${idx++} OFFSET $${idx++}`;
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
         AND r.id = (SELECT MAX(id) FROM rounds WHERE chain = $1 AND network = $2)`,
      [chain, network]
    );
    const map = new Map<string, { produced: number; expected: number }>();
    for (const row of result.rows) {
      map.set(row.producer, { produced: row.blocks_produced, expected: row.blocks_expected });
    }
    return map;
  }

  async getRoundCounts(chain: string, network: string, since: string | null = null): Promise<{ total: number; perfect: number; issues: number }> {
    const params: any[] = [chain, network];
    let sql = `SELECT
      COUNT(*) as total,
      SUM(CASE WHEN producers_missed = 0 THEN 1 ELSE 0 END) as perfect,
      SUM(CASE WHEN producers_missed > 0 THEN 1 ELSE 0 END) as issues
    FROM rounds WHERE chain = $1 AND network = $2`;
    if (since) { sql += ` AND timestamp_start >= $3`; params.push(since); }
    const result = await this.pool.query(sql, params);
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
        AND r.created_at >= NOW() - ($4 || ' days')::INTERVAL
      GROUP BY producer`,
      [chain, network, producer, days]
    );
    return result.rows[0];
  }

  async getAllProducerStats(chain: string, network: string, days: number | null = 30, since: string | null = null): Promise<any[]> {
    const params: any[] = [chain, network];
    let timeFilter: string;
    if (since) {
      timeFilter = `AND r.created_at >= $3`;
      params.push(since);
    } else {
      timeFilter = `AND r.created_at >= NOW() - ($3 || ' days')::INTERVAL`;
      params.push(days || 30);
    }
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
      WHERE r.chain = $1 AND r.network = $2
        ${timeFilter}
      GROUP BY producer
      ORDER BY reliability_pct DESC, producer ASC`,
      params
    );
    return result.rows;
  }

  async getLongestOutages(chain: string, network: string, days: number | null = 30, since: string | null = null): Promise<Map<string, number>> {
    const params: any[] = [chain, network];
    let timeFilter: string;
    if (since) {
      timeFilter = `AND r.created_at >= $3`;
      params.push(since);
    } else {
      timeFilter = `AND r.created_at >= NOW() - ($3 || ' days')::INTERVAL`;
      params.push(days || 30);
    }
    const result = await this.pool.query(
      `SELECT rp.producer, rp.blocks_produced, r.round_number
      FROM round_producers rp
      JOIN rounds r ON rp.round_id = r.id
      WHERE r.chain = $1 AND r.network = $2
        ${timeFilter}
      ORDER BY rp.producer, r.round_number ASC`,
      params
    );

    const outages = new Map<string, number>();
    let currentProducer = '';
    let currentStreak = 0;
    let maxStreak = 0;

    for (const row of result.rows) {
      if (row.producer !== currentProducer) {
        if (currentProducer) {
          outages.set(currentProducer, Math.max(maxStreak, currentStreak));
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
      outages.set(currentProducer, Math.max(maxStreak, currentStreak));
    }

    return outages;
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

  // -- Batch operations --

  async transaction<T>(fn: () => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn();
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
