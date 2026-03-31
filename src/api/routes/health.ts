import type { FastifyInstance } from 'fastify';
import type { Database } from '../../store/Database.js';

export async function healthRoutes(app: FastifyInstance, db?: Database): Promise<void> {
  app.get('/api/v1/health', async () => {
    return {
      status: 'ok',
      timestamp: new Date().toISOString(),
    };
  });

  if (db) {
    app.get<{ Params: { chain: string; network: string } }>(
      '/api/v1/:chain/:network/status',
      async (request) => {
        const { chain, network } = request.params;
        const currentGlobalRound = await db.getState(chain, network, 'current_global_round');
        const activationGlobalRound = await db.getState(chain, network, 'schedule_activation_global_round');
        const firstCompleteRound = await db.getState(chain, network, 'first_complete_round');
        const lastBlock = await db.getState(chain, network, 'last_block');
        const scheduleJson = await db.getState(chain, network, 'schedule');
        const pendingJson = await db.getState(chain, network, 'pending_schedule');

        let schedule = null;
        if (scheduleJson) {
          try { schedule = JSON.parse(scheduleJson); } catch {}
        }

        let pendingSchedule = null;
        if (pendingJson) {
          try { pendingSchedule = JSON.parse(pendingJson); } catch {}
        }

        const globalRound = currentGlobalRound ? parseInt(currentGlobalRound, 10) : 0;
        const activationRound = activationGlobalRound ? parseInt(activationGlobalRound, 10) : 0;
        const displayRound = globalRound - activationRound;

        const scheduleChanges = await db.getScheduleChanges(chain, network, 1);
        const latestSchedule = scheduleChanges.length > 0 ? scheduleChanges[0] : null;

        const earliestRounds = await db.getEarliestRounds(chain, network, 1);

        return {
          chain,
          network,
          currentRound: displayRound,
          firstCompleteRound: firstCompleteRound ? parseInt(firstCompleteRound, 10) : null,
          lastBlock: lastBlock ? parseInt(lastBlock, 10) : 0,
          scheduleVersion: schedule?.version ?? 0,
          producerCount: schedule?.producers?.length ?? 0,
          scheduleActivation: latestSchedule ? {
            blockNumber: latestSchedule.block_number,
            timestamp: latestSchedule.timestamp,
          } : null,
          earliestData: earliestRounds.length > 0 ? {
            roundNumber: earliestRounds[0].round_number,
            timestamp: earliestRounds[0].timestamp_start,
          } : null,
          pendingSchedule,
        };
      }
    );
  }
}
