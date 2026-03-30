import BetterSqlite3 from 'better-sqlite3';
import path from 'path';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'Summarizer' });

export class Summarizer {
  private db: BetterSqlite3.Database;

  constructor(dataDir: string) {
    const dbPath = path.join(dataDir, 'core-monitor.db');
    this.db = new BetterSqlite3(dbPath);
  }

  generateWeeklySummary(weekStart: string, weekEnd: string): void {
    log.info({ weekStart, weekEnd }, 'Generating weekly summary');

    this.db.exec('BEGIN');
    try {
      const rows = this.db.prepare(`
        SELECT
          r.chain,
          r.network,
          rp.producer,
          COUNT(DISTINCT r.id) as rounds_scheduled,
          SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
          SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
          SUM(rp.blocks_expected) as blocks_expected,
          SUM(rp.blocks_produced) as blocks_produced,
          SUM(rp.blocks_missed) as blocks_missed
        FROM round_producers rp
        JOIN rounds r ON rp.round_id = r.id
        WHERE r.timestamp_start >= ? AND r.timestamp_start < ?
        GROUP BY r.chain, r.network, rp.producer
      `).all(weekStart, weekEnd) as any[];

      const forkCounts = this.db.prepare(`
        SELECT
          chain, network, original_producer as producer,
          COUNT(*) as fork_count
        FROM fork_events
        WHERE timestamp >= ? AND timestamp < ?
        GROUP BY chain, network, original_producer
      `).all(weekStart, weekEnd) as any[];

      const forkMap = new Map<string, number>();
      for (const fc of forkCounts) {
        forkMap.set(`${fc.chain}:${fc.network}:${fc.producer}`, fc.fork_count);
      }

      const insert = this.db.prepare(`
        INSERT INTO weekly_summaries (
          chain, network, week_start, week_end, producer,
          rounds_scheduled, rounds_produced, rounds_missed,
          blocks_expected, blocks_produced, blocks_missed,
          fork_count, reliability_pct
        ) VALUES (
          @chain, @network, @week_start, @week_end, @producer,
          @rounds_scheduled, @rounds_produced, @rounds_missed,
          @blocks_expected, @blocks_produced, @blocks_missed,
          @fork_count, @reliability_pct
        ) ON CONFLICT(chain, network, week_start, producer) DO UPDATE SET
          rounds_scheduled = excluded.rounds_scheduled,
          rounds_produced = excluded.rounds_produced,
          rounds_missed = excluded.rounds_missed,
          blocks_expected = excluded.blocks_expected,
          blocks_produced = excluded.blocks_produced,
          blocks_missed = excluded.blocks_missed,
          fork_count = excluded.fork_count,
          reliability_pct = excluded.reliability_pct
      `);

      for (const row of rows) {
        const forks = forkMap.get(`${row.chain}:${row.network}:${row.producer}`) || 0;
        const reliability = row.blocks_expected > 0
          ? Math.round((row.blocks_produced / row.blocks_expected) * 10000) / 100
          : 100;

        insert.run({
          chain: row.chain,
          network: row.network,
          week_start: weekStart,
          week_end: weekEnd,
          producer: row.producer,
          rounds_scheduled: row.rounds_scheduled,
          rounds_produced: row.rounds_produced,
          rounds_missed: row.rounds_missed,
          blocks_expected: row.blocks_expected,
          blocks_produced: row.blocks_produced,
          blocks_missed: row.blocks_missed,
          fork_count: forks,
          reliability_pct: reliability,
        });
      }

      this.db.exec('COMMIT');
      log.info({ producers: rows.length }, 'Weekly summary generated');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  generateMonthlySummary(monthStart: string, monthEnd: string): void {
    log.info({ monthStart, monthEnd }, 'Generating monthly summary');

    this.db.exec('BEGIN');
    try {
      const rows = this.db.prepare(`
        SELECT
          r.chain,
          r.network,
          rp.producer,
          COUNT(DISTINCT r.id) as rounds_scheduled,
          SUM(CASE WHEN rp.blocks_produced > 0 THEN 1 ELSE 0 END) as rounds_produced,
          SUM(CASE WHEN rp.blocks_produced = 0 THEN 1 ELSE 0 END) as rounds_missed,
          SUM(rp.blocks_expected) as blocks_expected,
          SUM(rp.blocks_produced) as blocks_produced,
          SUM(rp.blocks_missed) as blocks_missed
        FROM round_producers rp
        JOIN rounds r ON rp.round_id = r.id
        WHERE r.timestamp_start >= ? AND r.timestamp_start < ?
        GROUP BY r.chain, r.network, rp.producer
      `).all(monthStart, monthEnd) as any[];

      const forkCounts = this.db.prepare(`
        SELECT
          chain, network, original_producer as producer,
          COUNT(*) as fork_count
        FROM fork_events
        WHERE timestamp >= ? AND timestamp < ?
        GROUP BY chain, network, original_producer
      `).all(monthStart, monthEnd) as any[];

      const forkMap = new Map<string, number>();
      for (const fc of forkCounts) {
        forkMap.set(`${fc.chain}:${fc.network}:${fc.producer}`, fc.fork_count);
      }

      const insert = this.db.prepare(`
        INSERT INTO monthly_summaries (
          chain, network, month_start, month_end, producer,
          rounds_scheduled, rounds_produced, rounds_missed,
          blocks_expected, blocks_produced, blocks_missed,
          fork_count, reliability_pct
        ) VALUES (
          @chain, @network, @month_start, @month_end, @producer,
          @rounds_scheduled, @rounds_produced, @rounds_missed,
          @blocks_expected, @blocks_produced, @blocks_missed,
          @fork_count, @reliability_pct
        ) ON CONFLICT(chain, network, month_start, producer) DO UPDATE SET
          rounds_scheduled = excluded.rounds_scheduled,
          rounds_produced = excluded.rounds_produced,
          rounds_missed = excluded.rounds_missed,
          blocks_expected = excluded.blocks_expected,
          blocks_produced = excluded.blocks_produced,
          blocks_missed = excluded.blocks_missed,
          fork_count = excluded.fork_count,
          reliability_pct = excluded.reliability_pct
      `);

      for (const row of rows) {
        const forks = forkMap.get(`${row.chain}:${row.network}:${row.producer}`) || 0;
        const reliability = row.blocks_expected > 0
          ? Math.round((row.blocks_produced / row.blocks_expected) * 10000) / 100
          : 100;

        insert.run({
          chain: row.chain,
          network: row.network,
          month_start: monthStart,
          month_end: monthEnd,
          producer: row.producer,
          rounds_scheduled: row.rounds_scheduled,
          rounds_produced: row.rounds_produced,
          rounds_missed: row.rounds_missed,
          blocks_expected: row.blocks_expected,
          blocks_produced: row.blocks_produced,
          blocks_missed: row.blocks_missed,
          fork_count: forks,
          reliability_pct: reliability,
        });
      }

      this.db.exec('COMMIT');
      log.info({ producers: rows.length }, 'Monthly summary generated');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  close(): void {
    this.db.close();
  }
}
