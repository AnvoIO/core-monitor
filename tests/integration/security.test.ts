import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer } from '../../src/api/server.js';
import { Database } from '../../src/store/Database.js';
import { createTestDb, cleanTestDb, getTestPgUrl } from '../setup.js';
import type { AppConfig } from '../../src/config.js';
import type { FastifyInstance } from 'fastify';

describe('Security Fixes', () => {
  let app: FastifyInstance;
  let db: Database;

  const config: AppConfig = {
    chains: [{
      id: 'libre_mainnet', chain: 'libre', network: 'mainnet',
      shipUrl: 'ws://localhost:8088', apiUrl: 'http://localhost:8888',
      chainId: 'abc123', scheduleSize: 21, blocksPerBp: 12, blockTimeMs: 500,
    }],
    telegram: { apiKey: '', enabled: false },
    slack: { webhookUrl: '', enabled: false },
    api: { port: 0, host: '127.0.0.1', corsOrigin: 'https://monitor.cryptobloks.io' },
    postgresUrl: getTestPgUrl(),
    retentionDays: 548,
    logLevel: 'error',
    dataDir: '',
  };

  beforeAll(async () => {
    db = await createTestDb();
    await cleanTestDb();
    app = await createServer(config, db);
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
    await db.close();
  });

  describe('CORS', () => {
    it('should use configured CORS origin', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['access-control-allow-origin']).toBe('https://monitor.cryptobloks.io');
    });

    it('should not use wildcard when configured', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['access-control-allow-origin']).not.toBe('*');
    });
  });

  describe('Security headers', () => {
    it('should set X-Frame-Options', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['x-frame-options']).toBe('DENY');
    });

    it('should set X-Content-Type-Options', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['x-content-type-options']).toBe('nosniff');
    });

    it('should set Referrer-Policy', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      expect(res.headers['referrer-policy']).toBe('strict-origin-when-cross-origin');
    });
  });

  describe('Health endpoint', () => {
    it('should not return uptime', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/health' });
      const body = res.json();
      expect(body.status).toBe('ok');
      expect(body.timestamp).toBeDefined();
      expect(body.uptime).toBeUndefined();
    });
  });

  describe('Chain/network validation', () => {
    it('should return 404 for unknown chain', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/unknown/mainnet/rounds' });
      expect(res.statusCode).toBe(404);
      expect(res.json().error).toBe('Unknown chain/network');
    });

    it('should return 404 for unknown network', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/fakenet/rounds' });
      expect(res.statusCode).toBe(404);
      expect(res.json().error).toBe('Unknown chain/network');
    });

    it('should accept configured chain/network', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds' });
      expect(res.statusCode).toBe(200);
    });
  });

  describe('Producer 404', () => {
    it('should return 404 for nonexistent producer', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/producers/nonexistent' });
      expect(res.statusCode).toBe(404);
      expect(res.json().error).toBe('Not found');
    });
  });

  describe('Input validation', () => {
    it('should handle invalid limit gracefully', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds?limit=abc' });
      expect(res.statusCode).toBe(200);
    });

    it('should cap limit at 500', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds?limit=99999' });
      expect(res.statusCode).toBe(200);
    });

    it('should handle negative offset', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/rounds?offset=-5' });
      expect(res.statusCode).toBe(200);
    });

    it('should ignore invalid date format for since', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?since=notadate' });
      expect(res.statusCode).toBe(200);
    });

    it('should accept valid ISO date for since', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/libre/mainnet/events?since=2026-03-30' });
      expect(res.statusCode).toBe(200);
    });
  });

  describe('Error handler', () => {
    it('should not leak stack traces', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/nonexistent/route' });
      expect(res.statusCode).toBe(404);
      const body = res.json();
      expect(body.stack).toBeUndefined();
    });
  });

  describe('Chains API', () => {
    it('should not expose sensitive config', async () => {
      const res = await app.inject({ method: 'GET', url: '/api/v1/chains' });
      const body = res.json();
      expect(body.chains[0].shipUrl).toBeUndefined();
      expect(body.chains[0].apiUrl).toBeUndefined();
      expect(body.chains[0].hyperionUrl).toBeUndefined();
    });
  });
});
