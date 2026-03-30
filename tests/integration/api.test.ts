import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer } from '../../src/api/server.js';
import { Database } from '../../src/store/Database.js';
import type { AppConfig } from '../../src/config.js';
import type { FastifyInstance } from 'fastify';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('API Routes', () => {
  let app: FastifyInstance;
  let db: Database;
  let tmpDir: string;

  const config: AppConfig = {
    chains: [
      {
        id: 'libre_mainnet',
        chain: 'libre',
        network: 'mainnet',
        shipUrl: 'ws://localhost:8088',
        apiUrl: 'http://localhost:8888',
        chainId: 'abc123',
        scheduleSize: 21,
        blocksPerBp: 12,
        blockTimeMs: 500,
      },
      {
        id: 'libre_testnet',
        chain: 'libre',
        network: 'testnet',
        shipUrl: 'ws://localhost:8087',
        apiUrl: 'http://localhost:8887',
        chainId: 'def456',
        scheduleSize: 21,
        blocksPerBp: 12,
        blockTimeMs: 500,
      },
    ],
    telegram: { apiKey: '' },
    slack: { webhookUrl: '' },
    api: { port: 0, host: '127.0.0.1' },
    retentionDays: 548,
    logLevel: 'error',
    dataDir: '',
  };

  function seedData() {
    // Insert rounds with producers
    for (let i = 1; i <= 5; i++) {
      const roundId = db.insertRound({
        chain: 'libre',
        network: 'mainnet',
        round_number: i,
        schedule_version: 1,
        timestamp_start: `2026-03-30T00:0${i}:00.000`,
        timestamp_end: `2026-03-30T00:0${i}:30.000`,
        producers_scheduled: 2,
        producers_produced: i <= 4 ? 2 : 1,
        producers_missed: i <= 4 ? 0 : 1,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'goodbp',
        position: 0,
        blocks_expected: 12,
        blocks_produced: 12,
        blocks_missed: 0,
        first_block: i * 100,
        last_block: i * 100 + 11,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'okbp',
        position: 1,
        blocks_expected: 12,
        blocks_produced: i <= 4 ? 12 : 0,
        blocks_missed: i <= 4 ? 0 : 12,
        first_block: i <= 4 ? i * 100 + 12 : null,
        last_block: i <= 4 ? i * 100 + 23 : null,
      });
    }

    // Missed block events
    db.insertMissedBlockEvent({
      chain: 'libre',
      network: 'mainnet',
      producer: 'okbp',
      round_id: null,
      blocks_missed: 12,
      block_number: 500,
      timestamp: '2026-03-30T00:05:30.000',
    });

    // Fork events
    db.insertForkEvent({
      chain: 'libre',
      network: 'mainnet',
      round_id: null,
      block_number: 350,
      original_producer: 'okbp',
      replacement_producer: 'goodbp',
      timestamp: '2026-03-30T00:03:15.000',
    });

    // Schedule changes
    db.insertScheduleChange({
      chain: 'libre',
      network: 'mainnet',
      schedule_version: 1,
      producers_added: '["goodbp","okbp"]',
      producers_removed: '[]',
      producer_list: '["goodbp","okbp"]',
      block_number: 1,
      timestamp: '2026-03-30T00:00:00.000',
    });
  }

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-api-'));
    config.dataDir = tmpDir;
    db = new Database(tmpDir);
    seedData();
    app = await createServer(config, db);
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  describe('GET /api/v1/health', () => {
    it('should return health status', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.status).toBe('ok');
      expect(body.timestamp).toBeDefined();
      expect(body.uptime).toBeGreaterThan(0);
    });
  });

  describe('GET /api/v1/chains', () => {
    it('should return configured chains', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/chains' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.chains).toHaveLength(2);
      expect(body.chains[0].id).toBe('libre_mainnet');
      expect(body.chains[1].id).toBe('libre_testnet');
      expect(body.chains[0].scheduleSize).toBe(21);
    });

    it('should not expose sensitive config (shipUrl, etc)', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/chains' });
      const body = res.json();
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
      expect(body.rounds[0].round_number).toBe(5); // most recent first
    });

    it('should respect limit parameter', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds?limit=2' });
      const body = res.json();
      expect(body.rounds).toHaveLength(2);
    });

    it('should cap limit at 500', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds?limit=9999' });
      expect(res.statusCode).toBe(200);
      // Doesn't error, just caps
    });

    it('should return empty array for unknown chain', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/unknown/mainnet/rounds' });
      const body = res.json();
      expect(body.rounds).toHaveLength(0);
    });
  });

  describe('GET /api/v1/:chain/:network/producers', () => {
    it('should return all producer stats', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/producers' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.producers).toHaveLength(2);
      expect(body.days).toBe(30);

      // goodbp should rank first (100% reliability)
      expect(body.producers[0].producer).toBe('goodbp');
      expect(body.producers[0].reliability_pct).toBe(100);
    });

    it('should accept days parameter', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/producers?days=7' });
      const body = res.json();
      expect(body.days).toBe(7);
    });
  });

  describe('GET /api/v1/:chain/:network/producers/:name', () => {
    it('should return single producer stats', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/producers/goodbp' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.producer).toBeDefined();
      expect(body.producer.producer).toBe('goodbp');
      expect(body.producer.reliability_pct).toBe(100);
    });

    it('should handle unknown producer', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/producers/nobody' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.error).toBe('Not found');
    });
  });

  describe('GET /api/v1/:chain/:network/events', () => {
    it('should return all event types by default', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.missed_blocks).toBeDefined();
      expect(body.forks).toBeDefined();
      expect(body.schedule_changes).toBeDefined();
    });

    it('should filter by type=missed', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?type=missed' });
      const body = res.json();
      expect(body.missed_blocks).toHaveLength(1);
      expect(body.forks).toBeUndefined();
      expect(body.schedule_changes).toBeUndefined();
    });

    it('should filter by type=forks', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?type=forks' });
      const body = res.json();
      expect(body.forks).toHaveLength(1);
      expect(body.missed_blocks).toBeUndefined();
    });

    it('should filter by type=schedule', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?type=schedule' });
      const body = res.json();
      expect(body.schedule_changes).toHaveLength(1);
      expect(body.missed_blocks).toBeUndefined();
    });

    it('should respect limit parameter', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?limit=1' });
      expect(res.statusCode).toBe(200);
    });
  });

  describe('GET /api/v1/:chain/:network/summaries/weekly', () => {
    it('should return empty when no summaries generated', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/summaries/weekly' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.summaries).toEqual([]);
    });
  });

  describe('GET /api/v1/:chain/:network/summaries/monthly', () => {
    it('should return empty when no summaries generated', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/summaries/monthly' });
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.summaries).toEqual([]);
    });
  });

  describe('CORS', () => {
    it('should include CORS headers', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['access-control-allow-origin']).toBe('*');
    });
  });
});
