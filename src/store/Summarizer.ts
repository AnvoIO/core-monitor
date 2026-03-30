import pg from 'pg';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'Summarizer' });

export class Summarizer {
  private pool: pg.Pool;

  constructor(connectionString: string) {
    this.pool = new pg.Pool({ connectionString, max: 2 });
  }

  async generateWeeklySummary(weekStart: string, weekEnd: string): Promise<void> {
    log.info({ weekStart, weekEnd }, 'Generating weekly summary');

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const rows = (await client.query(
        `SELECT r.chain, r.network, rp.producer,
          COUNT(DISTINCT r.id) as rounds_scheduled,
          SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
          SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
          SUM(rp.blocks_expected) as blocks_expected,
          SUM(rp.blocks_produced) as blocks_produced,
          SUM(rp.blocks_missed) as blocks_missed
        FROM round_producers rp
        JOIN rounds r ON rp.round_id = r.id
        WHERE r.timestamp_start >= $1 AND r.timestamp_start < $2
        GROUP BY r.chain, r.network, rp.producer`,
        [weekStart, weekEnd]
      )).rows;

      const forkCounts = (await client.query(
        `SELECT chain, network, original_producer as producer, COUNT(*) as fork_count
        FROM fork_events WHERE timestamp >= $1 AND timestamp < $2
        GROUP BY chain, network, original_producer`,
        [weekStart, weekEnd]
      )).rows;

      const forkMap = new Map<string, number>();
      for (const fc of forkCounts) {
        forkMap.set(`${fc.chain}:${fc.network}:${fc.producer}`, parseInt(fc.fork_count));
      }

      for (const row of rows) {
        const forks = forkMap.get(`${row.chain}:${row.network}:${row.producer}`) || 0;
        const reliability = row.blocks_expected > 0
          ? Math.round((row.blocks_produced / row.blocks_expected) * 10000) / 100
          : 100;

        await client.query(
          `INSERT INTO weekly_summaries (chain, network, week_start, week_end, producer,
            rounds_scheduled, rounds_produced, rounds_missed,
            blocks_expected, blocks_produced, blocks_missed,
            fork_count, reliability_pct)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          ON CONFLICT(chain, network, week_start, producer) DO UPDATE SET
            rounds_scheduled = EXCLUDED.rounds_scheduled,
            rounds_produced = EXCLUDED.rounds_produced,
            rounds_missed = EXCLUDED.rounds_missed,
            blocks_expected = EXCLUDED.blocks_expected,
            blocks_produced = EXCLUDED.blocks_produced,
            blocks_missed = EXCLUDED.blocks_missed,
            fork_count = EXCLUDED.fork_count,
            reliability_pct = EXCLUDED.reliability_pct`,
          [row.chain, row.network, weekStart, weekEnd, row.producer,
           row.rounds_scheduled, row.rounds_produced, row.rounds_missed,
           row.blocks_expected, row.blocks_produced, row.blocks_missed,
           forks, reliability]
        );
      }

      await client.query('COMMIT');
      log.info({ producers: rows.length }, 'Weekly summary generated');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async generateMonthlySummary(monthStart: string, monthEnd: string): Promise<void> {
    log.info({ monthStart, monthEnd }, 'Generating monthly summary');

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const rows = (await client.query(
        `SELECT r.chain, r.network, rp.producer,
          COUNT(DISTINCT r.id) as rounds_scheduled,
          SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
          SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
          SUM(rp.blocks_expected) as blocks_expected,
          SUM(rp.blocks_produced) as blocks_produced,
          SUM(rp.blocks_missed) as blocks_missed
        FROM round_producers rp
        JOIN rounds r ON rp.round_id = r.id
        WHERE r.timestamp_start >= $1 AND r.timestamp_start < $2
        GROUP BY r.chain, r.network, rp.producer`,
        [monthStart, monthEnd]
      )).rows;

      const forkCounts = (await client.query(
        `SELECT chain, network, original_producer as producer, COUNT(*) as fork_count
        FROM fork_events WHERE timestamp >= $1 AND timestamp < $2
        GROUP BY chain, network, original_producer`,
        [monthStart, monthEnd]
      )).rows;

      const forkMap = new Map<string, number>();
      for (const fc of forkCounts) {
        forkMap.set(`${fc.chain}:${fc.network}:${fc.producer}`, parseInt(fc.fork_count));
      }

      for (const row of rows) {
        const forks = forkMap.get(`${row.chain}:${row.network}:${row.producer}`) || 0;
        const reliability = row.blocks_expected > 0
          ? Math.round((row.blocks_produced / row.blocks_expected) * 10000) / 100
          : 100;

        await client.query(
          `INSERT INTO monthly_summaries (chain, network, month_start, month_end, producer,
            rounds_scheduled, rounds_produced, rounds_missed,
            blocks_expected, blocks_produced, blocks_missed,
            fork_count, reliability_pct)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          ON CONFLICT(chain, network, month_start, producer) DO UPDATE SET
            rounds_scheduled = EXCLUDED.rounds_scheduled,
            rounds_produced = EXCLUDED.rounds_produced,
            rounds_missed = EXCLUDED.rounds_missed,
            blocks_expected = EXCLUDED.blocks_expected,
            blocks_produced = EXCLUDED.blocks_produced,
            blocks_missed = EXCLUDED.blocks_missed,
            fork_count = EXCLUDED.fork_count,
            reliability_pct = EXCLUDED.reliability_pct`,
          [row.chain, row.network, monthStart, monthEnd, row.producer,
           row.rounds_scheduled, row.rounds_produced, row.rounds_missed,
           row.blocks_expected, row.blocks_produced, row.blocks_missed,
           forks, reliability]
        );
      }

      await client.query('COMMIT');
      log.info({ producers: rows.length }, 'Monthly summary generated');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
