import BetterSqlite3 from 'better-sqlite3';
import path from 'path';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'Retention' });

export class Retention {
  private db: BetterSqlite3.Database;
  private retentionDays: number;

  constructor(dataDir: string, retentionDays: number) {
    const dbPath = path.join(dataDir, 'core-monitor.db');
    this.db = new BetterSqlite3(dbPath);
    this.retentionDays = retentionDays;
  }

  purgeOldRounds(): number {
    const cutoff = `-${this.retentionDays} days`;
    const result = this.db.prepare(`
      DELETE FROM rounds
      WHERE created_at < datetime('now', ?)
    `).run(cutoff);

    const deleted = result.changes;
    if (deleted > 0) {
      log.info({ deleted, retentionDays: this.retentionDays }, 'Purged old rounds');
    }
    return deleted;
  }

  purgeOldScheduleChanges(): number {
    const cutoff = `-${this.retentionDays} days`;
    const result = this.db.prepare(`
      DELETE FROM schedule_changes
      WHERE created_at < datetime('now', ?)
    `).run(cutoff);
    return result.changes;
  }

  runAll(): void {
    log.info('Starting retention purge');
    const rounds = this.purgeOldRounds();
    const schedules = this.purgeOldScheduleChanges();
    log.info({ rounds, schedules }, 'Retention purge complete');
    // Note: missed_block_events and fork_events are kept forever
  }

  close(): void {
    this.db.close();
  }
}
