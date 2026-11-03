import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createTestDb, cleanTestDb, getTestPgUrl } from '../setup.js';
import { Database } from '../../src/store/Database.js';

const TEST_DAY = new Date(Date.now() - 86400000).toISOString().substring(0, 10);

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
        schedule_version: 5, timestamp_start: `${TEST_DAY}T00:00:00.000`,
        timestamp_end: `${TEST_DAY}T00:02:06.000`, producers_scheduled: 21,
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
          schedule_version: 1, timestamp_start: `${TEST_DAY}T00:0${i}:00.000`,
          timestamp_end: `${TEST_DAY}T00:0${i}:30.000`, producers_scheduled: 21,
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
        schedule_version: 1, timestamp_start: `${TEST_DAY}T00:00:00.000`,
        timestamp_end: `${TEST_DAY}T00:02:00.000`, producers_scheduled: 21,
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
        schedule_version: 1, timestamp_start: `${TEST_DAY}T00:00:00.000`,
        timestamp_end: `${TEST_DAY}T00:02:00.000`, producers_scheduled: 2,
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

    it('finds the first block of the earliest round by time, not round number', async () => {
      expect(await db.getEarliestRoundFirstBlock('wire', 'testnet')).toBeNull();
      const round = (n: number, start: string, firstBlocks: Array<number | null>) =>
        db.insertRound({
          chain: 'wire', network: 'testnet', round_number: n, schedule_version: 1,
          timestamp_start: start, timestamp_end: start, producers_scheduled: firstBlocks.length,
          producers_produced: 1, producers_missed: 0,
        }).then((id) => Promise.all(firstBlocks.map((fb, position) => db.insertRoundProducer({
          round_id: id, producer: `p${position}`, position, blocks_expected: 12,
          blocks_produced: fb === null ? 0 : 12, blocks_missed: fb === null ? 12 : 0,
          first_block: fb, last_block: fb === null ? null : fb + 11,
        }))));
      await round(1, `${TEST_DAY}T01:00:00.000`, [5000, 5012]);
      // Earlier in time, higher round number (numbering restarts per schedule activation).
      await round(9, `${TEST_DAY}T00:00:00.000`, [null, 2012, 2000]);
      expect(await db.getEarliestRoundFirstBlock('wire', 'testnet')).toBe(2000);
      expect(await db.getEarliestRoundFirstBlock('libre', 'testnet')).toBeNull();
    });
  });

  describe('rebuildDerivedStats', () => {
    // Rounds with alpha and bravo; bravo misses rounds 2-3 on day 1. Day 2 has one catchup
    // round before the live start (12:00) and a live round after it.
    const addRound = async (n: number, start: string, bravoProduced: number) => {
      const id = await db.insertRound({
        chain: 'wire', network: 'testnet', round_number: n, schedule_version: 1,
        timestamp_start: start, timestamp_end: start.replace(/:00\.000$/, ':59.500'), producers_scheduled: 2,
        producers_produced: bravoProduced > 0 ? 2 : 1, producers_missed: bravoProduced > 0 ? 0 : 1,
      });
      await db.insertRoundProducer({ round_id: id, producer: 'alpha', position: 0, blocks_expected: 12, blocks_produced: 12, blocks_missed: 0, first_block: n * 24, last_block: n * 24 + 11 });
      await db.insertRoundProducer({ round_id: id, producer: 'bravo', position: 1, blocks_expected: 12, blocks_produced: bravoProduced, blocks_missed: 12 - bravoProduced, first_block: null, last_block: null });
    };
    const query = (sql: string) => (db as any).pool.query(sql).then((r: any) => r.rows);

    it('derives daily stats and outage events for the range, leaving later rows alone', async () => {
      await addRound(1, '2026-10-20T10:00:00.000', 12);
      await addRound(2, '2026-10-20T10:01:00.000', 0);
      await addRound(3, '2026-10-20T10:02:00.000', 0);
      await addRound(4, '2026-10-20T10:03:00.000', 12);
      await addRound(5, '2026-10-21T11:00:00.000', 12);
      await addRound(6, '2026-10-21T12:00:00.000', 0); // live writer's
      // A stale day-1 row and a live outage event that must survive.
      await db.upsertRoundCountsDaily({ chain: 'wire', network: 'testnet', day: '2026-10-20', perfect: true });
      await db.insertOutageEvent({ chain: 'wire', network: 'testnet', producer: 'bravo', rounds_count: 1, start_round_number: 6, end_round_number: 6, schedule_version: 1, timestamp_start: '2026-10-21T12:00:00.000', timestamp_end: '2026-10-21T12:00:59.500' });

      await db.rebuildDerivedStats('wire', 'testnet', '2026-10-20T10:00:00.000', '2026-10-21T12:00:00.000');
      await db.rebuildDerivedStats('wire', 'testnet', '2026-10-20T10:00:00.000', '2026-10-21T12:00:00.000'); // idempotent

      expect(await query(`SELECT to_char(day,'YYYY-MM-DD') d, total_rounds, perfect_rounds, issue_rounds FROM round_counts_daily WHERE chain='wire' ORDER BY day`)).toEqual([
        { d: '2026-10-20', total_rounds: 4, perfect_rounds: 2, issue_rounds: 2 },
        { d: '2026-10-21', total_rounds: 2, perfect_rounds: 1, issue_rounds: 1 }, // reconciled: both rounds of the boundary day
      ]);
      expect(await query(`SELECT to_char(day,'YYYY-MM-DD') d, producer, rounds_scheduled, rounds_missed, blocks_missed FROM producer_stats_daily WHERE chain='wire' AND producer='bravo' ORDER BY day`)).toEqual([
        { d: '2026-10-20', producer: 'bravo', rounds_scheduled: 4, rounds_missed: 2, blocks_missed: 24 },
        { d: '2026-10-21', producer: 'bravo', rounds_scheduled: 2, rounds_missed: 1, blocks_missed: 12 },
      ]);
      expect(await query(`SELECT producer, rounds_count, start_round_number, end_round_number, timestamp_start, timestamp_end FROM outage_events WHERE chain='wire' ORDER BY timestamp_start`)).toEqual([
        { producer: 'bravo', rounds_count: 2, start_round_number: 2, end_round_number: 3, timestamp_start: '2026-10-20T10:01:00.000', timestamp_end: '2026-10-20T10:02:59.500' },
        { producer: 'bravo', rounds_count: 1, start_round_number: 6, end_round_number: 6, timestamp_start: '2026-10-21T12:00:00.000', timestamp_end: '2026-10-21T12:00:59.500' },
      ]);
    });
  });

  describe('missed_block_events', () => {
    async function seedRound() {
      return await db.insertRound({
        chain: 'libre', network: 'mainnet', round_number: 1,
        schedule_version: 1, timestamp_start: `${TEST_DAY}T00:00:00.000`,
        timestamp_end: `${TEST_DAY}T00:02:00.000`, producers_scheduled: 1,
        producers_produced: 0, producers_missed: 1,
      });
    }

    it('should insert and retrieve missed block events', async () => {
      const roundId = await seedRound();
      await db.insertMissedBlockEvent({
        chain: 'libre', network: 'mainnet', producer: 'badproducer',
        round_id: roundId, blocks_missed: 12, block_number: 5000,
        timestamp: `${TEST_DAY}T00:01:00.000`,
      });
      const events = await db.getMissedBlockEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
      expect(events[0].producer).toBe('badproducer');
    });

    it('should skip insert when round_id is null (orphan prevention)', async () => {
      await db.insertMissedBlockEvent({
        chain: 'libre', network: 'mainnet', producer: 'badproducer',
        round_id: null, blocks_missed: 12, block_number: 5000,
        timestamp: `${TEST_DAY}T00:01:00.000`,
      });
      const events = await db.getMissedBlockEvents('libre', 'mainnet');
      expect(events).toHaveLength(0);
    });

    it('should dedupe repeated inserts for the same (round, producer)', async () => {
      const roundId = await seedRound();
      const event = {
        chain: 'libre', network: 'mainnet', producer: 'badproducer',
        round_id: roundId, blocks_missed: 12, block_number: 5000,
        timestamp: `${TEST_DAY}T00:01:00.000`,
      };
      await db.insertMissedBlockEvent(event);
      await db.insertMissedBlockEvent(event);
      await db.insertMissedBlockEvent(event);
      const events = await db.getMissedBlockEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
    });
  });

  describe('fork_events', () => {
    it('should insert and retrieve fork events', async () => {
      await db.insertForkEvent({
        chain: 'libre', network: 'mainnet', round_id: null,
        block_number: 3000, original_producer: 'bp_a',
        replacement_producer: 'bp_b', timestamp: `${TEST_DAY}T00:00:30.000`,
      });
      const events = await db.getForkEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
      expect(events[0].original_producer).toBe('bp_a');
    });

    it('should dedupe repeated inserts for the same fork', async () => {
      const event = {
        chain: 'libre', network: 'mainnet', round_id: null,
        block_number: 4000, original_producer: 'bp_x',
        replacement_producer: 'bp_y', timestamp: `${TEST_DAY}T00:01:00.000`,
      };
      await db.insertForkEvent(event);
      await db.insertForkEvent(event);
      await db.insertForkEvent(event);
      const events = await db.getForkEvents('libre', 'mainnet');
      const matching = events.filter(e => e.block_number === 4000);
      expect(matching).toHaveLength(1);
    });
  });

  describe('producer stats', () => {
    async function seedData() {
      for (let i = 1; i <= 3; i++) {
        const roundId = await db.insertRound({
          chain: 'libre', network: 'mainnet', round_number: i,
          schedule_version: 1, timestamp_start: `${TEST_DAY}T00:0${i}:00.000`,
          timestamp_end: `${TEST_DAY}T00:0${i}:30.000`, producers_scheduled: 2,
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
      await db.reconcileDay('libre', 'mainnet', TEST_DAY);
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
        schedule_version: 1, timestamp_start: `${TEST_DAY}T00:00:00.000`,
        timestamp_end: `${TEST_DAY}T00:02:00.000`, producers_scheduled: 2,
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
