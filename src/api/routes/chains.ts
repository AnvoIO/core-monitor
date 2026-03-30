import type { FastifyInstance } from 'fastify';
import type { AppConfig } from '../../config.js';

export async function chainRoutes(
  app: FastifyInstance,
  config: AppConfig
): Promise<void> {
  app.get('/api/v1/chains', async () => {
    return {
      chains: config.chains.map((c) => ({
        id: c.id,
        chain: c.chain,
        network: c.network,
        chainId: c.chainId,
        scheduleSize: c.scheduleSize,
        blocksPerBp: c.blocksPerBp,
        blockTimeMs: c.blockTimeMs,
      })),
    };
  });
}
