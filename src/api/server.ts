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
    logger: logger,
  });

  // CORS — configurable via API_CORS_ORIGIN env var (default: *)
  const corsOrigin = config.api.corsOrigin;
  app.addHook('onSend', async (request, reply) => {
    reply.header('Access-Control-Allow-Origin', corsOrigin);
    reply.header('Access-Control-Allow-Methods', 'GET');
    reply.header('Access-Control-Allow-Headers', 'Content-Type');
  });

  // Custom error handler — avoid leaking internals
  app.setErrorHandler(async (error: any, request, reply) => {
    log.error({ err: error, url: request.url }, 'Request error');
    const statusCode = error.statusCode || 500;
    reply.status(statusCode).send({
      error: statusCode === 404 ? 'Not Found' : 'Internal Server Error',
    });
  });

  // Serve static dashboard files
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
