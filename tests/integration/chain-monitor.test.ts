import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Database } from '../../src/store/Database.js';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { RoundEvaluator, type BlockRecord } from '../../src/monitor/RoundEvaluator.js';
import { AlertManager } from '../../src/alerting/AlertManager.js';
import type { ChainConfig } from '../../src/config.js';
import type { AlertChannel, AlertMessage } from '../../src/alerting/types.js';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('Chain Monitor Pipeline Integration (slot-based)', () => {
  let db: Database;
  let schedule: ScheduleTracker;
  let evaluator: RoundEvaluator;
  let alertManager: AlertManager;
  let mockChannel: AlertChannel & { calls: AlertMessage[] };
  let tmpDir: string;

  const chainConfig: ChainConfig = {
    id: 'libre_mainnet',
    chain: 'libre',
    network: 'mainnet',
    shipUrl: 'ws://localhost:8088',
    apiUrl: 'http://localhost:8888',
    chainId: 'abc123',
    scheduleSize: 3,
    blocksPerBp: 4,
    blockTimeMs: 500,
  };

  const producers = ['alpha', 'bravo', 'charlie'];
  const EPOCH = Date.UTC(2000, 0, 1);
  const SLOTS_PER_ROUND = 12; // 3 * 4

  function slotToTimestamp(slot: number): string {
    return new Date(EPOCH + slot * 500).toISOString();
  }

  function makeBlock(blockNum: number, slot: number, producer: string): BlockRecord {
    return { block_num: blockNum, producer, timestamp: slotToTimestamp(slot), schedule_version: 1 };
  }

  function expectedProducer(slot: number): string {
    return producers[Math.floor((slot % SLOTS_PER_ROUND) / chainConfig.blocksPerBp)];
  }

  function generateRound(roundNum: number, startBlockNum: number, skip: string[] = []): BlockRecord[] {
    const blocks: BlockRecord[] = [];
    const startSlot = roundNum * SLOTS_PER_ROUND;
    for (let i = 0; i < SLOTS_PER_ROUND; i++) {
      const slot = startSlot + i;
      const producer = expectedProducer(slot);
      if (skip.includes(producer)) continue;
      blocks.push(makeBlock(startBlockNum + blocks.length, slot, producer));
    }
    return blocks;
  }

  function feedBlocks(blocks: BlockRecord[]) {
    let result = null;
    for (const b of blocks) {
      const r = evaluator.processBlock(b);
      if (r) result = r;
    }
    return result;
  }

  function createMockChannel(): AlertChannel & { calls: AlertMessage[] } {
    const calls: AlertMessage[] = [];
    return { name: 'mock', calls, send: vi.fn(async (msg: AlertMessage) => { calls.push(msg); }) };
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-pipe-'));
    db = new Database(tmpDir);
    schedule = new ScheduleTracker('libre', 'mainnet', db);
    evaluator = new RoundEvaluator(chainConfig, db, schedule);
    alertManager = new AlertManager(0);
    mockChannel = createMockChannel();
    alertManager.addChannel(mockChannel);
    schedule.updateSchedule(
      1,
      producers.map(p => ({ producer_name: p, block_signing_key: 'EOS000' })),
      1,
      '2000-01-01T00:00:00.000Z'
    );
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should persist a perfect round to DB', () => {
    feedBlocks(generateRound(100, 1));
    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    expect(result!.producersProduced).toBe(3);

    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(1);
    const rp = db.getRoundProducers(rounds[0].id);
    expect(rp).toHaveLength(3);
    for (const p of rp) {
      expect(p.blocks_produced).toBe(4);
      expect(p.blocks_produced).toBeLessThanOrEqual(p.blocks_expected);
    }
  });

  it('should detect missed round and allow alerting', async () => {
    feedBlocks(generateRound(100, 1, ['bravo']));
    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    expect(result!.producersMissed).toBe(1);

    await alertManager.missedRound({
      chain: 'libre', network: 'mainnet',
      producer: 'bravo', round: result!.roundNumber,
      scheduleVersion: result!.scheduleVersion,
      blocksMissed: 4, timestamp: result!.timestampEnd,
    });

    expect(mockChannel.calls).toHaveLength(1);
    expect(mockChannel.calls[0].severity).toBe('alert');
    expect(mockChannel.calls[0].title).toContain('bravo');

    const events = db.getMissedBlockEvents('libre', 'mainnet');
    expect(events.find((e: any) => e.producer === 'bravo')).toBeDefined();
  });

  it('should produce correct reliability stats', () => {
    // Round 100: perfect
    feedBlocks(generateRound(100, 1));
    // Round 101: bravo misses
    feedBlocks(generateRound(101, 100, ['bravo']));
    // Trigger eval of 101
    const slot = 102 * SLOTS_PER_ROUND;
    evaluator.processBlock(makeBlock(200, slot, expectedProducer(slot)));

    const stats = db.getAllProducerStats('libre', 'mainnet', 30);
    const alpha = stats.find((s: any) => s.producer === 'alpha');
    const bravo = stats.find((s: any) => s.producer === 'bravo');

    expect(alpha!.reliability_pct).toBe(100);
    expect(bravo!.reliability_pct).toBeLessThan(100);
    // Verify no producer exceeds 100%
    for (const s of stats) {
      expect(s.reliability_pct).toBeLessThanOrEqual(100);
    }
  });

  it('should handle multiple consecutive rounds', () => {
    const results = [];

    feedBlocks(generateRound(100, 1));
    const r = feedBlocks(generateRound(101, 100, ['bravo']));
    if (r) results.push(r);

    const slot = 102 * SLOTS_PER_ROUND;
    const r2 = evaluator.processBlock(makeBlock(200, slot, expectedProducer(slot)));
    if (r2) results.push(r2);

    expect(results).toHaveLength(2);
    expect(results[0].producersMissed).toBe(0);
    expect(results[1].producersMissed).toBe(1);

    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(2);
  });

  it('should persist and resume state', () => {
    feedBlocks(generateRound(100, 1));
    const slot = 101 * SLOTS_PER_ROUND;
    evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    const evaluator2 = new RoundEvaluator(chainConfig, db, schedule);
    expect(evaluator2.round).toBeGreaterThan(0);
  });
});
