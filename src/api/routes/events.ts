import type { FastifyInstance } from 'fastify';
import type { Database } from '../../store/Database.js';

interface EventParams {
  chain: string;
  network: string;
}

interface EventQuery {
  type?: string;
  limit?: string;
  offset?: string;
  since?: string;
  until?: string;
}

export async function eventRoutes(
  app: FastifyInstance,
  db: Database
): Promise<void> {
  app.get<{ Params: EventParams; Querystring: EventQuery }>(
    '/api/v1/:chain/:network/events',
    async (request) => {
      const { chain, network } = request.params;
      const type = request.query.type || 'all';
      const limit = Math.min(parseInt(request.query.limit || '50', 10), 500);
      const offset = parseInt(request.query.offset || '0', 10);
      const since = request.query.since || null;
      const until = request.query.until || null;

      const result: Record<string, any> = { chain, network, type, limit, offset };

      if (type === 'missed' || type === 'all') {
        result.missed_blocks = db.getMissedBlockEvents(chain, network, limit, offset, since, until);
      }
      if (type === 'forks' || type === 'all') {
        result.forks = db.getForkEvents(chain, network, limit, offset, since, until);
      }
      if (type === 'schedule' || type === 'all') {
        result.schedule_changes = db.getScheduleChanges(chain, network, limit);
      }
      if (type === 'producer' || type === 'all') {
        result.producer_events = db.getProducerEvents(chain, network, limit, offset, since, until);
      }

      return result;
    }
  );
}
