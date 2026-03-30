import type { FastifyInstance } from 'fastify';
import type { Database } from '../../store/Database.js';
import type { AppConfig } from '../../config.js';

interface ProducerParams {
  chain: string;
  network: string;
  name?: string;
}

interface ProducerQuery {
  days?: string;
}

async function fetchRegisteredProducers(apiUrl: string): Promise<any[]> {
  try {
    const response = await fetch(`${apiUrl}/v1/chain/get_table_rows`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        code: 'eosio',
        scope: 'eosio',
        table: 'producers',
        limit: 500,
        json: true,
      }),
    });
    if (!response.ok) return [];
    const data = await response.json() as any;
    return data.rows || [];
  } catch {
    return [];
  }
}

export async function producerRoutes(
  app: FastifyInstance,
  db: Database,
  config?: AppConfig
): Promise<void> {
  app.get<{ Params: ProducerParams; Querystring: ProducerQuery }>(
    '/api/v1/:chain/:network/producers',
    async (request) => {
      const { chain, network } = request.params;
      const days = parseInt(request.query.days || '30', 10);
      const stats = db.getAllProducerStats(chain, network, days);

      // Include schedule order for client-side sorting
      let scheduleOrder: string[] = [];
      const scheduleJson = db.getState(chain, network, 'schedule');
      if (scheduleJson) {
        try {
          const schedule = JSON.parse(scheduleJson);
          scheduleOrder = schedule.producers || [];
        } catch {}
      }

      // Fetch all registered producers from chain for additional metadata
      let registeredProducers: any[] = [];
      const chainConfig = config?.chains.find(c => c.chain === chain && c.network === network);
      const chainProducerMap = new Map<string, any>();

      if (chainConfig) {
        const allRegistered = await fetchRegisteredProducers(chainConfig.apiUrl);

        for (const r of allRegistered) {
          chainProducerMap.set(r.owner, r);
        }

        const scheduledSet = new Set(scheduleOrder);

        registeredProducers = allRegistered
          .filter(r => !scheduledSet.has(r.owner))
          .map(r => ({
            producer: r.owner,
            is_active: r.is_active === 1,
            total_votes: r.total_votes,
            url: r.url,
            producer_key: r.producer_key || '',
            scheduled: false,
          }));
      }

      // Get longest outages for all producers
      const outages = db.getLongestOutages(chain, network, days);

      // Enrich scheduled producer stats with chain data (signing key, is_active, outage)
      const enrichedStats = stats.map((s: any) => {
        const chainData = chainProducerMap.get(s.producer);
        return {
          ...s,
          is_active: chainData ? chainData.is_active === 1 : true,
          producer_key: chainData?.producer_key || '',
          url: chainData?.url || '',
          scheduled: scheduleOrder.includes(s.producer),
          longest_outage: outages.get(s.producer) || 0,
        };
      });

      // For registered-but-not-scheduled producers, also look up historical stats
      const statsSet = new Set(stats.map((s: any) => s.producer));
      const enrichedRegistered = registeredProducers.map((r: any) => {
        // Check if they have historical data in the DB
        const historicalStats = !statsSet.has(r.producer)
          ? db.getProducerStats(chain, network, r.producer, days)
          : null;

        return {
          ...r,
          total_rounds: historicalStats?.total_rounds || 0,
          rounds_produced: historicalStats?.rounds_produced || 0,
          rounds_missed: historicalStats?.rounds_missed || 0,
          total_blocks_expected: historicalStats?.total_blocks_expected || 0,
          total_blocks_produced: historicalStats?.total_blocks_produced || 0,
          total_blocks_missed: historicalStats?.total_blocks_missed || 0,
          reliability_pct: historicalStats?.reliability_pct ?? null,
          longest_outage: outages.get(r.producer) || 0,
        };
      });

      return { chain, network, days, producers: enrichedStats, scheduleOrder, registeredProducers: enrichedRegistered };
    }
  );

  app.get<{ Params: Required<ProducerParams>; Querystring: ProducerQuery }>(
    '/api/v1/:chain/:network/producers/:name',
    async (request) => {
      const { chain, network, name } = request.params;
      const days = parseInt(request.query.days || '30', 10);
      const stats = db.getProducerStats(chain, network, name, days);
      if (!stats) {
        return { chain, network, producer: name, error: 'Not found' };
      }
      return { chain, network, days, producer: stats };
    }
  );
}
