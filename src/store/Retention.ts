import pg from 'pg';
import { logger } from '../utils/logger.js';

const log = logger.child({ module: 'Retention' });

export class Retention {
  private pool: pg.Pool;
  private retentionDays: number;

  constructor(connectionString: string, retentionDays: number) {
    this.pool = new pg.Pool({ connectionString, max: 2 });
    this.retentionDays = retentionDays;
  }

  async purgeOldRounds(): Promise<number> {
    const result = await this.pool.query(
      `DELETE FROM rounds WHERE created_at < NOW() - ($1 || ' days')::INTERVAL`,
      [this.retentionDays]
    );
    const deleted = result.rowCount || 0;
    if (deleted > 0) {
      log.info({ deleted, retentionDays: this.retentionDays }, 'Purged old rounds');
    }
    return deleted;
  }

  async purgeOldScheduleChanges(): Promise<number> {
    const result = await this.pool.query(
      `DELETE FROM schedule_changes WHERE created_at < NOW() - ($1 || ' days')::INTERVAL`,
      [this.retentionDays]
    );
    return result.rowCount || 0;
  }

  async runAll(): Promise<void> {
    log.info('Starting retention purge');
    const rounds = await this.purgeOldRounds();
    const schedules = await this.purgeOldScheduleChanges();
    log.info({ rounds, schedules }, 'Retention purge complete');
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
