import type { FastifyInstance } from 'fastify';
import type { Database } from '../../store/Database.js';

export async function healthRoutes(app: FastifyInstance, db?: Database): Promise<void> {
  app.get('/api/v1/health', async () => {
    return {
      status: 'ok',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
    };
  });

  if (db) {
    app.get<{ Params: { chain: string; network: string } }>(
      '/api/v1/:chain/:network/status',
      async (request) => {
        const { chain, network } = request.params;
        const currentRound = db.getState(chain, network, 'current_round');
        const firstCompleteRound = db.getState(chain, network, 'first_complete_round');
        const lastBlock = db.getState(chain, network, 'last_block');
        const scheduleJson = db.getState(chain, network, 'schedule');
        const lastScheduleVersion = db.getState(chain, network, 'last_schedule_version');

        let schedule = null;
        if (scheduleJson) {
          try {
            schedule = JSON.parse(scheduleJson);
          } catch {}
        }

        return {
          chain,
          network,
          currentRound: currentRound ? parseInt(currentRound, 10) : 0,
          firstCompleteRound: firstCompleteRound ? parseInt(firstCompleteRound, 10) : null,
          lastBlock: lastBlock ? parseInt(lastBlock, 10) : 0,
          scheduleVersion: lastScheduleVersion ? parseInt(lastScheduleVersion, 10) : 0,
          producerCount: schedule?.producers?.length ?? 0,
        };
      }
    );
  }
}
