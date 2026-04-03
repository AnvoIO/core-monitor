import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer } from '../../src/api/server.js';
import { Database } from '../../src/store/Database.js';
import { createTestDb, cleanTestDb, getTestPgUrl } from '../setup.js';
import type { AppConfig } from '../../src/config.js';
import type { FastifyInstance } from 'fastify';

describe('API Routes', () => {
  let app: FastifyInstance;
  let db: Database;

  const config: AppConfig = {
    chains: [{
      id: 'libre_mainnet', chain: 'libre', network: 'mainnet',
      shipUrl: 'ws://localhost:8088', apiUrl: 'http://localhost:8888',
      chainId: 'abc123', scheduleSize: 21, blocksPerBp: 12, blockTimeMs: 500,
    }, {
      id: 'libre_testnet', chain: 'libre', network: 'testnet',
      shipUrl: 'ws://localhost:8087', apiUrl: 'http://localhost:8887',
      chainId: 'def456', scheduleSize: 21, blocksPerBp: 12, blockTimeMs: 500,
    }],
    telegram: { apiKey: '', enabled: false },
    slack: { webhookUrl: '', enabled: false },
    api: { port: 0, host: '127.0.0.1', corsOrigin: '*' },
    postgresUrl: getTestPgUrl(),
    retentionDays: 548,
    logLevel: 'error',
    dataDir: '',
  };

  async function seedData() {
    for (let i = 1; i <= 5; i++) {
      const roundId = await db.insertRound({
        chain: 'libre', network: 'mainnet', round_number: i,
        schedule_version: 1, timestamp_start: `2026-03-30T00:0${i}:00.000`,
        timestamp_end: `2026-03-30T00:0${i}:30.000`, producers_scheduled: 2,
        producers_produced: i <= 4 ? 2 : 1, producers_missed: i <= 4 ? 0 : 1,
      });
      await db.insertRoundProducer({
        round_id: roundId, producer: 'goodbp', position: 0,
        blocks_expected: 12, blocks_produced: 12, blocks_missed: 0,
        first_block: i * 100, last_block: i * 100 + 11,
      });
      await db.insertRoundProducer({
        round_id: roundId, producer: 'okbp', position: 1,
        blocks_expected: 12, blocks_produced: i <= 4 ? 12 : 0,
        blocks_missed: i <= 4 ? 0 : 12,
        first_block: i <= 4 ? i * 100 + 12 : null,
        last_block: i <= 4 ? i * 100 + 23 : null,
      });
    }
    await db.insertMissedBlockEvent({
      chain: 'libre', network: 'mainnet', producer: 'okbp',
      round_id: null, blocks_missed: 12, block_number: 500,
      timestamp: '2026-03-30T00:05:30.000',
    });
    await db.insertForkEvent({
      chain: 'libre', network: 'mainnet', round_id: null,
      block_number: 350, original_producer: 'okbp',
      replacement_producer: 'goodbp', timestamp: '2026-03-30T00:03:15.000',
    });
    await db.insertScheduleChange({
      chain: 'libre', network: 'mainnet', schedule_version: 1,
      producers_added: '["goodbp","okbp"]', producers_removed: '[]',
      producer_list: '["goodbp","okbp"]', block_number: 1,
      timestamp: '2026-03-30T00:00:00.000',
    });
  }

  beforeAll(async () => {
    db = await createTestDb();
    await cleanTestDb();
    await seedData();
    await db.reconcileDay('libre', 'mainnet', '2026-03-30');
    app = await createServer(config, db);
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
    await db.close();
  });

  describe('GET /api/v1/health', () => {
    it('should return health status', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.status).toBe('ok');
      expect(body.timestamp).toBeDefined();
    });
  });

  describe('GET /api/v1/chains', () => {
    it('should return configured chains without sensitive data', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/chains' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.chains).toHaveLength(2);
      expect(body.chains[0].shipUrl).toBeUndefined();
      expect(body.chains[0].apiUrl).toBeUndefined();
    });
  });

  describe('GET /api/v1/:chain/:network/rounds', () => {
    it('should return recent rounds', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.rounds).toHaveLength(5);
      expect(body.rounds[0].round_number).toBe(5);
    });

    it('should respect limit', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds?limit=2' });
      expect(res.json().rounds).toHaveLength(2);
    });

    it('should return 404 for unknown chain', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/unknown/mainnet/rounds' });
      expect(res.statusCode).toBe(404);
    });

    it('should include round counts', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds' });
      const body = res.json();
      expect(body.counts).toBeDefined();
      expect(body.counts.total).toBeGreaterThanOrEqual(0);
      expect(body.counts.perfect).toBeDefined();
      expect(body.counts.issues).toBeDefined();
    });
  });

  describe('GET /api/v1/:chain/:network/producers', () => {
    it('should return producer stats', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/producers' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.producers.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('GET /api/v1/:chain/:network/events', () => {
    it('should return all event types', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.missed_blocks).toBeDefined();
      expect(body.forks).toBeDefined();
      expect(body.schedule_changes).toBeDefined();
    });

    it('should filter by type', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?type=missed' });
      const body = res.json();
      expect(body.missed_blocks).toHaveLength(1);
      expect(body.forks).toBeUndefined();
    });

    it('should support pagination', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?limit=1&offset=0' });
      expect(res.statusCode).toBe(200);
    });
  });

  describe('CORS', () => {
    it('should include CORS headers', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['access-control-allow-origin']).toBe('*');
    });
  });
});
