import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';
import path from 'path';
import { readFileSync } from 'fs';
import { logger } from '../utils/logger.js';

const { version } = JSON.parse(readFileSync(path.resolve(__dirname, '../../package.json'), 'utf8'));
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
    logger: { level: process.env.LOG_LEVEL || 'info' },
  });

  // Security headers
  app.addHook('onSend', async (request, reply) => {
    // CORS — configurable via API_CORS_ORIGIN env var
    reply.header('Access-Control-Allow-Origin', config.api.corsOrigin);
    reply.header('Access-Control-Allow-Methods', 'GET');
    reply.header('Access-Control-Allow-Headers', 'Content-Type');
    // Security headers
    reply.header('X-Frame-Options', 'DENY');
    reply.header('X-Content-Type-Options', 'nosniff');
    reply.header('Referrer-Policy', 'strict-origin-when-cross-origin');
  });

  // Validate chain/network params against configured chains
  const validChains = new Set(config.chains.map(c => `${c.chain}:${c.network}`));
  app.addHook('preHandler', async (request, reply) => {
    const params = request.params as any;
    if (params?.chain && params?.network) {
      if (!validChains.has(`${params.chain}:${params.network}`)) {
        reply.status(404).send({ error: 'Unknown chain/network' });
      }
    }
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

  // Serve index.html with version from package.json
  const indexHtml = readFileSync(path.join(staticDir, 'index.html'), 'utf8')
    .replace('{{VERSION}}', version);
  app.get('/', async (_request, reply) => {
    reply.type('text/html').send(indexHtml);
  });

  await app.register(fastifyStatic, {
    root: staticDir,
    prefix: '/',
    index: false,
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
