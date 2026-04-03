import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createTestDb, cleanTestDb, getTestPgUrl } from '../setup.js';
import { Database } from '../../src/store/Database.js';

describe('Database', () => {
  let db: Database;

  beforeAll(async () => {
    db = await createTestDb();
  });

  beforeEach(async () => {
    await cleanTestDb();
  });

  afterAll(async () => {
    await db.close();
  });

  describe('monitor_state', () => {
    it('should return null for missing keys', async () => {
      expect(await db.getState('libre', 'mainnet', 'nonexistent')).toBeNull();
    });

    it('should set and get state', async () => {
      await db.setState('libre', 'mainnet', 'last_block', '12345');
      expect(await db.getState('libre', 'mainnet', 'last_block')).toBe('12345');
    });

    it('should upsert state', async () => {
      await db.setState('libre', 'mainnet', 'last_block', '100');
      await db.setState('libre', 'mainnet', 'last_block', '200');
      expect(await db.getState('libre', 'mainnet', 'last_block')).toBe('200');
    });

    it('should isolate state by chain/network', async () => {
      await db.setState('libre', 'mainnet', 'key', 'mainnet_val');
      await db.setState('libre', 'testnet', 'key', 'testnet_val');
      expect(await db.getState('libre', 'mainnet', 'key')).toBe('mainnet_val');
      expect(await db.getState('libre', 'testnet', 'key')).toBe('testnet_val');
    });
  });

  describe('rounds', () => {
    it('should insert and retrieve a round', async () => {
      const roundId = await db.insertRound({
        chain: 'libre', network: 'mainnet', round_number: 1,
        schedule_version: 5, timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:06.000', producers_scheduled: 21,
        producers_produced: 20, producers_missed: 1,
      });
      expect(roundId).toBeGreaterThan(0);
      const rounds = await db.getRecentRounds('libre', 'mainnet', 10);
      expect(rounds).toHaveLength(1);
      expect(rounds[0].producers_missed).toBe(1);
    });

    it('should return rounds in descending order', async () => {
      for (let i = 1; i <= 5; i++) {
        await db.insertRound({
          chain: 'libre', network: 'mainnet', round_number: i,
          schedule_version: 1, timestamp_start: `2026-03-30T00:0${i}:00.000`,
          timestamp_end: `2026-03-30T00:0${i}:30.000`, producers_scheduled: 21,
          producers_produced: 21, producers_missed: 0,
        });
      }
      const rounds = await db.getRecentRounds('libre', 'mainnet', 3);
      expect(rounds).toHaveLength(3);
      expect(rounds[0].round_number).toBe(5);
      expect(rounds[2].round_number).toBe(3);
    });

    it('should isolate rounds by chain/network', async () => {
      await db.insertRound({
        chain: 'libre', network: 'mainnet', round_number: 1,
        schedule_version: 1, timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:00.000', producers_scheduled: 21,
        producers_produced: 21, producers_missed: 0,
      });
      expect(await db.getRecentRounds('libre', 'mainnet')).toHaveLength(1);
      expect(await db.getRecentRounds('libre', 'testnet')).toHaveLength(0);
    });
  });

  describe('round_producers', () => {
    it('should insert and retrieve round producers', async () => {
      const roundId = await db.insertRound({
        chain: 'libre', network: 'mainnet', round_number: 1,
        schedule_version: 1, timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:00.000', producers_scheduled: 2,
        producers_produced: 1, producers_missed: 1,
      });
      await db.insertRoundProducer({
        round_id: roundId, producer: 'producer1', position: 0,
        blocks_expected: 12, blocks_produced: 12, blocks_missed: 0,
        first_block: 1000, last_block: 1011,
      });
      await db.insertRoundProducer({
        round_id: roundId, producer: 'producer2', position: 1,
        blocks_expected: 12, blocks_produced: 0, blocks_missed: 12,
        first_block: null, last_block: null,
      });
      const producers = await db.getRoundProducers(roundId);
      expect(producers).toHaveLength(2);
      expect(producers[0].blocks_produced).toBe(12);
      expect(producers[1].blocks_produced).toBe(0);
    });
  });

  describe('missed_block_events', () => {
    it('should insert and retrieve missed block events', async () => {
      await db.insertMissedBlockEvent({
        chain: 'libre', network: 'mainnet', producer: 'badproducer',
        round_id: null, blocks_missed: 12, block_number: 5000,
        timestamp: '2026-03-30T00:01:00.000',
      });
      const events = await db.getMissedBlockEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
      expect(events[0].producer).toBe('badproducer');
    });
  });

  describe('fork_events', () => {
    it('should insert and retrieve fork events', async () => {
      await db.insertForkEvent({
        chain: 'libre', network: 'mainnet', round_id: null,
        block_number: 3000, original_producer: 'bp_a',
        replacement_producer: 'bp_b', timestamp: '2026-03-30T00:00:30.000',
      });
      const events = await db.getForkEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
      expect(events[0].original_producer).toBe('bp_a');
    });
  });

  describe('producer stats', () => {
    async function seedData() {
      for (let i = 1; i <= 3; i++) {
        const roundId = await db.insertRound({
          chain: 'libre', network: 'mainnet', round_number: i,
          schedule_version: 1, timestamp_start: `2026-03-30T00:0${i}:00.000`,
          timestamp_end: `2026-03-30T00:0${i}:30.000`, producers_scheduled: 2,
          producers_produced: 2, producers_missed: 0,
        });
        await db.insertRoundProducer({
          round_id: roundId, producer: 'goodbp', position: 0,
          blocks_expected: 12, blocks_produced: 12, blocks_missed: 0,
          first_block: i * 100, last_block: i * 100 + 11,
        });
        await db.insertRoundProducer({
          round_id: roundId, producer: 'okbp', position: 1,
          blocks_expected: 12, blocks_produced: i === 2 ? 10 : 12,
          blocks_missed: i === 2 ? 2 : 0,
          first_block: i * 100 + 12, last_block: i * 100 + 23,
        });
      }
    }

    it('should compute all producer stats ranked by reliability', async () => {
      await seedData();
      await db.reconcileDay('libre', 'mainnet', '2026-03-30');
      const allStats = await db.getAllProducerStats('libre', 'mainnet', 30);
      expect(allStats).toHaveLength(2);
      expect(allStats[0].producer).toBe('goodbp');
      expect(parseFloat(allStats[0].reliability_pct)).toBe(100);
      expect(parseFloat(allStats[1].reliability_pct)).toBeLessThan(100);
    });

    it('should return undefined for unknown producer', async () => {
      const stats = await db.getProducerStats('libre', 'mainnet', 'nonexistent', 30);
      expect(stats).toBeUndefined();
    });
  });

  describe('latest round producers', () => {
    it('should return production status for latest round', async () => {
      const roundId = await db.insertRound({
        chain: 'libre', network: 'mainnet', round_number: 1,
        schedule_version: 1, timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:00.000', producers_scheduled: 2,
        producers_produced: 1, producers_missed: 1,
      });
      await db.insertRoundProducer({
        round_id: roundId, producer: 'up_bp', position: 0,
        blocks_expected: 12, blocks_produced: 12, blocks_missed: 0,
        first_block: 100, last_block: 111,
      });
      await db.insertRoundProducer({
        round_id: roundId, producer: 'down_bp', position: 1,
        blocks_expected: 12, blocks_produced: 0, blocks_missed: 12,
        first_block: null, last_block: null,
      });
      const details = await db.getLatestRoundProducerDetails('libre', 'mainnet');
      expect(details.get('up_bp')?.produced).toBe(12);
      expect(details.get('down_bp')?.produced).toBe(0);
    });
  });
});
