import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';
import path from 'path';
import { logger } from '../utils/logger.js';
import { healthRoutes } from './routes/health.js';
import { chainRoutes } from './routes/chains.js';
import { producerRoutes } from './routes/producers.js';
import { roundRoutes } from './routes/rounds.js';
import { eventRoutes } from './routes/events.js';
import type { AppConfig } from '../config.js';
import type { Database } from '../store/Database.js';

const log = logger.child({ module: 'API' });

export async function createServer(config: AppConfig, db: Database) {
  const app = Fastify({
    logger: false,
  });

  // CORS for dashboard
  app.addHook('onSend', async (request, reply) => {
    reply.header('Access-Control-Allow-Origin', '*');
    reply.header('Access-Control-Allow-Methods', 'GET');
    reply.header('Access-Control-Allow-Headers', 'Content-Type');
  });

  // Serve static dashboard files
  // In CJS mode, __dirname is available natively
  const staticDir = path.resolve(__dirname, 'static');
  await app.register(fastifyStatic, {
    root: staticDir,
    prefix: '/',
  });

  // Register API routes
  await healthRoutes(app, db);
  await chainRoutes(app, config);
  await producerRoutes(app, db, config);
  await roundRoutes(app, db);
  await eventRoutes(app, db);

  return app;
}

export async function startServer(config: AppConfig, db: Database) {
  const app = await createServer(config, db);

  await app.listen({ port: config.api.port, host: config.api.host });
  log.info({ port: config.api.port, host: config.api.host }, 'API server started');

  return app;
}
