import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Database } from '../../src/store/Database.js';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('Database', () => {
  let db: Database;
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-test-'));
    db = new Database(tmpDir);
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  describe('schema', () => {
    it('should create the database file', () => {
      expect(fs.existsSync(path.join(tmpDir, 'core-monitor.db'))).toBe(true);
    });

    it('should be idempotent on re-open', () => {
      db.close();
      // Re-open same DB — migrations should not fail
      db = new Database(tmpDir);
      expect(db).toBeDefined();
    });
  });

  describe('monitor_state', () => {
    it('should return null for missing keys', () => {
      expect(db.getState('libre', 'mainnet', 'nonexistent')).toBeNull();
    });

    it('should set and get state', () => {
      db.setState('libre', 'mainnet', 'last_block', '12345');
      expect(db.getState('libre', 'mainnet', 'last_block')).toBe('12345');
    });

    it('should upsert state', () => {
      db.setState('libre', 'mainnet', 'last_block', '100');
      db.setState('libre', 'mainnet', 'last_block', '200');
      expect(db.getState('libre', 'mainnet', 'last_block')).toBe('200');
    });

    it('should isolate state by chain/network', () => {
      db.setState('libre', 'mainnet', 'key', 'mainnet_val');
      db.setState('libre', 'testnet', 'key', 'testnet_val');
      expect(db.getState('libre', 'mainnet', 'key')).toBe('mainnet_val');
      expect(db.getState('libre', 'testnet', 'key')).toBe('testnet_val');
    });
  });

  describe('rounds', () => {
    it('should insert and retrieve a round', () => {
      const roundId = db.insertRound({
        chain: 'libre',
        network: 'mainnet',
        round_number: 1,
        schedule_version: 5,
        timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:06.000',
        producers_scheduled: 21,
        producers_produced: 20,
        producers_missed: 1,
      });

      expect(roundId).toBeGreaterThan(0);

      const rounds = db.getRecentRounds('libre', 'mainnet', 10);
      expect(rounds).toHaveLength(1);
      expect(rounds[0].round_number).toBe(1);
      expect(rounds[0].producers_missed).toBe(1);
    });

    it('should return rounds in descending order', () => {
      for (let i = 1; i <= 5; i++) {
        db.insertRound({
          chain: 'libre',
          network: 'mainnet',
          round_number: i,
          schedule_version: 1,
          timestamp_start: `2026-03-30T00:0${i}:00.000`,
          timestamp_end: `2026-03-30T00:0${i}:30.000`,
          producers_scheduled: 21,
          producers_produced: 21,
          producers_missed: 0,
        });
      }

      const rounds = db.getRecentRounds('libre', 'mainnet', 3);
      expect(rounds).toHaveLength(3);
      expect(rounds[0].round_number).toBe(5);
      expect(rounds[2].round_number).toBe(3);
    });

    it('should isolate rounds by chain/network', () => {
      db.insertRound({
        chain: 'libre',
        network: 'mainnet',
        round_number: 1,
        schedule_version: 1,
        timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:00.000',
        producers_scheduled: 21,
        producers_produced: 21,
        producers_missed: 0,
      });

      expect(db.getRecentRounds('libre', 'mainnet')).toHaveLength(1);
      expect(db.getRecentRounds('libre', 'testnet')).toHaveLength(0);
    });
  });

  describe('round_producers', () => {
    it('should insert and retrieve round producers', () => {
      const roundId = db.insertRound({
        chain: 'libre',
        network: 'mainnet',
        round_number: 1,
        schedule_version: 1,
        timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:00.000',
        producers_scheduled: 2,
        producers_produced: 1,
        producers_missed: 1,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'producer1',
        position: 0,
        blocks_expected: 12,
        blocks_produced: 12,
        blocks_missed: 0,
        first_block: 1000,
        last_block: 1011,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'producer2',
        position: 1,
        blocks_expected: 12,
        blocks_produced: 0,
        blocks_missed: 12,
        first_block: null,
        last_block: null,
      });

      const producers = db.getRoundProducers(roundId);
      expect(producers).toHaveLength(2);
      expect(producers[0].producer).toBe('producer1');
      expect(producers[0].blocks_produced).toBe(12);
      expect(producers[1].producer).toBe('producer2');
      expect(producers[1].blocks_produced).toBe(0);
    });

    it('should cascade delete round_producers when round is deleted', () => {
      const roundId = db.insertRound({
        chain: 'libre',
        network: 'mainnet',
        round_number: 1,
        schedule_version: 1,
        timestamp_start: '2026-03-30T00:00:00.000',
        timestamp_end: '2026-03-30T00:02:00.000',
        producers_scheduled: 1,
        producers_produced: 1,
        producers_missed: 0,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'producer1',
        position: 0,
        blocks_expected: 12,
        blocks_produced: 12,
        blocks_missed: 0,
        first_block: 1000,
        last_block: 1011,
      });

      // Directly delete the round
      db.transaction(() => {
        (db as any).db.prepare('DELETE FROM rounds WHERE id = ?').run(roundId);
      });

      expect(db.getRoundProducers(roundId)).toHaveLength(0);
    });
  });

  describe('missed_block_events', () => {
    it('should insert and retrieve missed block events', () => {
      db.insertMissedBlockEvent({
        chain: 'libre',
        network: 'mainnet',
        producer: 'badproducer',
        round_id: null,
        blocks_missed: 12,
        block_number: 5000,
        timestamp: '2026-03-30T00:01:00.000',
      });

      const events = db.getMissedBlockEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
      expect(events[0].producer).toBe('badproducer');
      expect(events[0].blocks_missed).toBe(12);
    });
  });

  describe('fork_events', () => {
    it('should insert and retrieve fork events', () => {
      db.insertForkEvent({
        chain: 'libre',
        network: 'mainnet',
        round_id: null,
        block_number: 3000,
        original_producer: 'producer_a',
        replacement_producer: 'producer_b',
        timestamp: '2026-03-30T00:00:30.000',
      });

      const events = db.getForkEvents('libre', 'mainnet');
      expect(events).toHaveLength(1);
      expect(events[0].original_producer).toBe('producer_a');
      expect(events[0].replacement_producer).toBe('producer_b');
    });
  });

  describe('schedule_changes', () => {
    it('should insert and retrieve schedule changes', () => {
      db.insertScheduleChange({
        chain: 'libre',
        network: 'mainnet',
        schedule_version: 10,
        producers_added: '["new_bp"]',
        producers_removed: '["old_bp"]',
        producer_list: '["bp1","bp2","new_bp"]',
        block_number: 100000,
        timestamp: '2026-03-30T00:00:00.000',
      });

      const changes = db.getScheduleChanges('libre', 'mainnet');
      expect(changes).toHaveLength(1);
      expect(changes[0].schedule_version).toBe(10);
      expect(JSON.parse(changes[0].producers_added)).toEqual(['new_bp']);
    });
  });

  describe('producer stats', () => {
    function seedRoundsWithProducers() {
      for (let i = 1; i <= 3; i++) {
        const roundId = db.insertRound({
          chain: 'libre',
          network: 'mainnet',
          round_number: i,
          schedule_version: 1,
          timestamp_start: `2026-03-30T00:0${i}:00.000`,
          timestamp_end: `2026-03-30T00:0${i}:30.000`,
          producers_scheduled: 2,
          producers_produced: 2,
          producers_missed: 0,
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
          blocks_produced: i === 2 ? 10 : 12,
          blocks_missed: i === 2 ? 2 : 0,
          first_block: i * 100 + 12,
          last_block: i * 100 + 23,
        });
      }
    }

    it('should compute per-producer stats', () => {
      seedRoundsWithProducers();

      const stats = db.getProducerStats('libre', 'mainnet', 'goodbp', 30);
      expect(stats).toBeDefined();
      expect(stats.total_rounds).toBe(3);
      expect(stats.total_blocks_produced).toBe(36);
      expect(stats.reliability_pct).toBe(100);
    });

    it('should compute stats for producers with missed blocks', () => {
      seedRoundsWithProducers();

      const stats = db.getProducerStats('libre', 'mainnet', 'okbp', 30);
      expect(stats).toBeDefined();
      expect(stats.total_blocks_produced).toBe(34);
      expect(stats.total_blocks_missed).toBe(2);
      expect(stats.reliability_pct).toBeCloseTo(94.44, 1);
    });

    it('should return all producer stats ranked by reliability', () => {
      seedRoundsWithProducers();

      const allStats = db.getAllProducerStats('libre', 'mainnet', 30);
      expect(allStats).toHaveLength(2);
      expect(allStats[0].producer).toBe('goodbp');
      expect(allStats[1].producer).toBe('okbp');
    });

    it('should return null for unknown producer', () => {
      const stats = db.getProducerStats('libre', 'mainnet', 'nonexistent', 30);
      expect(stats).toBeUndefined();
    });
  });

  describe('transactions', () => {
    it('should support transactional inserts', () => {
      db.transaction(() => {
        const roundId = db.insertRound({
          chain: 'libre',
          network: 'mainnet',
          round_number: 1,
          schedule_version: 1,
          timestamp_start: '2026-03-30T00:00:00.000',
          timestamp_end: '2026-03-30T00:02:00.000',
          producers_scheduled: 1,
          producers_produced: 1,
          producers_missed: 0,
        });
        db.insertRoundProducer({
          round_id: roundId,
          producer: 'bp1',
          position: 0,
          blocks_expected: 12,
          blocks_produced: 12,
          blocks_missed: 0,
          first_block: 100,
          last_block: 111,
        });
      });

      expect(db.getRecentRounds('libre', 'mainnet')).toHaveLength(1);
    });
  });
});
