import type { FastifyInstance } from 'fastify';
import type { Database } from '../../store/Database.js';

interface RoundParams {
  chain: string;
  network: string;
}

interface RoundQuery {
  limit?: string;
}

export async function roundRoutes(
  app: FastifyInstance,
  db: Database
): Promise<void> {
  app.get<{ Params: RoundParams; Querystring: RoundQuery }>(
    '/api/v1/:chain/:network/rounds',
    async (request) => {
      const { chain, network } = request.params;
      const limit = parseInt(request.query.limit || '100', 10);
      const rounds = await db.getRecentRounds(chain, network, Math.min(limit, 500));
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
