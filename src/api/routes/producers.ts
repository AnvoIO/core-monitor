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
  since?: string;
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
      const since = request.query.since && /^\d{4}-\d{2}-\d{2}/.test(request.query.since) ? request.query.since : null;
      const days = since ? null : Math.max(1, Math.min(parseInt(request.query.days || '30', 10) || 30, 9999));
      const stats = await db.getAllProducerStats(chain, network, days, since);

      let scheduleOrder: string[] = [];
      const scheduleJson = await db.getState(chain, network, 'schedule');
      if (scheduleJson) {
        try {
          const schedule = JSON.parse(scheduleJson);
          scheduleOrder = schedule.producers || [];
        } catch {}
      }

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

      const outages = await db.getLongestOutages(chain, network, days, since);
      const latestRound = await db.getLatestRoundProducerDetails(chain, network);

      const enrichedStats = stats.map((s: any) => {
        const chainData = chainProducerMap.get(s.producer);
        const lastRound = latestRound.get(s.producer);
        // status: 'up' (12/12), 'degraded' (1-11/12), 'down' (0/12), null (no data)
        let producing: string | null = null;
        if (lastRound !== undefined) {
          if (lastRound.produced === 0) producing = 'down';
          else if (lastRound.produced < lastRound.expected) producing = 'degraded';
          else producing = 'up';
        }
        return {
          ...s,
          is_active: chainData ? chainData.is_active === 1 : true,
          producing,
          producer_key: chainData?.producer_key || '',
          url: chainData?.url || '',
          scheduled: scheduleOrder.includes(s.producer),
          longest_outage: outages.get(s.producer) || 0,
        };
      });

      // Match registered producers with stats from the bulk query (no per-producer lookups)
      const statsMap = new Map(stats.map((s: any) => [s.producer, s]));
      const enrichedRegistered = registeredProducers.map((r: any) => {
        const s = statsMap.get(r.producer);
        return {
          ...r,
          total_rounds: s?.total_rounds || 0,
          rounds_produced: s?.rounds_produced || 0,
          rounds_missed: s?.rounds_missed || 0,
          total_blocks_expected: s?.total_blocks_expected || 0,
          total_blocks_produced: s?.total_blocks_produced || 0,
          total_blocks_missed: s?.total_blocks_missed || 0,
          reliability_pct: s?.reliability_pct ?? null,
          longest_outage: outages.get(r.producer) || 0,
        };
      });

      return { chain, network, days, producers: enrichedStats, scheduleOrder, registeredProducers: enrichedRegistered };
    }
  );

  app.get<{ Params: Required<ProducerParams>; Querystring: ProducerQuery }>(
    '/api/v1/:chain/:network/producers/:name',
    async (request, reply) => {
      const { chain, network, name } = request.params;
      const days = Math.max(1, Math.min(parseInt(request.query.days || '30', 10) || 30, 9999));
      const stats = await db.getProducerStats(chain, network, name, days);
      if (!stats) {
        reply.status(404);
        return { chain, network, producer: name, error: 'Not found' };
      }
      return { chain, network, days, producer: stats };
    }
  );

  app.get<{ Params: Required<ProducerParams>; Querystring: ProducerQuery }>(
    '/api/v1/:chain/:network/producers/:name/outages',
    async (request) => {
      const { chain, network, name } = request.params;
      const since = request.query.since && /^\d{4}-\d{2}-\d{2}/.test(request.query.since) ? request.query.since : null;
      const days = since ? null : Math.max(1, Math.min(parseInt(request.query.days || '30', 10) || 30, 9999));
      const details = await db.getProducerOutageDetails(chain, network, name, days, since);
      return { chain, network, producer: name, days, ...details };
    }
  );
}
