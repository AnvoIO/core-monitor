import type { FastifyInstance } from 'fastify';
import type { Database } from '../../store/Database.js';

interface RoundParams {
  chain: string;
  network: string;
}

interface RoundQuery {
  limit?: string;
  offset?: string;
  since?: string;
}

export async function roundRoutes(
  app: FastifyInstance,
  db: Database
): Promise<void> {
  app.get<{ Params: RoundParams; Querystring: RoundQuery }>(
    '/api/v1/:chain/:network/rounds',
    async (request) => {
      const { chain, network } = request.params;
      const limit = Math.max(1, Math.min(parseInt(request.query.limit || '100', 10) || 100, 500));
      const offset = Math.max(0, parseInt(request.query.offset || '0', 10) || 0);
      const since = request.query.since && /^\d{4}-\d{2}-\d{2}/.test(request.query.since) ? request.query.since : null;
      const rounds = await db.getRecentRounds(chain, network, limit, offset, since);
      return { chain, network, rounds };
    }
  );

  app.get<{ Params: RoundParams }>(
    '/api/v1/:chain/:network/summaries/weekly',
    async (request) => {
      const { chain, network } = request.params;
      const summaries = await db.getWeeklySummaries(chain, network);
      return { chain, network, summaries };
    }
  );

  app.get<{ Params: RoundParams }>(
    '/api/v1/:chain/:network/summaries/monthly',
    async (request) => {
      const { chain, network } = request.params;
      const summaries = await db.getMonthlySummaries(chain, network);
      return { chain, network, summaries };
    }
  );
}
