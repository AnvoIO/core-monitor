/**
 * Reader — API server and dashboard.
 *
 * Serves the REST API and static dashboard from PostgreSQL data.
 * Read-only — no SHiP connections or block processing.
 */

import { loadConfig } from './config.js';
import { Database } from './store/Database.js';
import { startServer } from './api/server.js';
import { logger } from './utils/logger.js';

const log = logger.child({ module: 'reader' });

async function main(): Promise<void> {
  log.info('AnvoIO Core Monitor — Reader starting');

  const config = loadConfig();
  log.info({ chains: config.chains.map((c) => c.id) }, 'Configuration loaded');

  const db = new Database(config.postgresUrl);
  await db.init();

  const server = await startServer(config, db);

  const shutdown = async () => {
    log.info('Reader shutting down');
    await server.close();
    await db.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  log.info('AnvoIO Core Monitor — Reader running');
}

main().catch((err) => {
  const msg = err?.message?.replace(/postgresql:\/\/[^@]*@/g, 'postgresql://***@') || 'Unknown error';
  log.error({ err: msg }, 'Fatal error');
  process.exit(1);
});
