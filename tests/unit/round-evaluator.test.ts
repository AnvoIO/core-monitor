import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { RoundEvaluator, type BlockRecord } from '../../src/monitor/RoundEvaluator.js';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { Database } from '../../src/store/Database.js';
import type { ChainConfig } from '../../src/config.js';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('RoundEvaluator (slot-based)', () => {
  let db: Database;
  let schedule: ScheduleTracker;
  let evaluator: RoundEvaluator;
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

  // Antelope epoch
  const EPOCH = Date.UTC(2000, 0, 1);
  // Slots per round = 3 * 4 = 12
  const SLOTS_PER_ROUND = 12;

  // Create a timestamp for a given slot number
  function slotToTimestamp(slot: number): string {
    return new Date(EPOCH + slot * 500).toISOString();
  }

  // Create a block at a given slot
  function makeBlock(blockNum: number, slot: number, producer: string): BlockRecord {
    return {
      block_num: blockNum,
      producer,
      timestamp: slotToTimestamp(slot),
      schedule_version: 1,
    };
  }

  // Get the expected producer for a slot
  function expectedProducer(slot: number): string {
    const posInRound = Math.floor((slot % SLOTS_PER_ROUND) / chainConfig.blocksPerBp);
    return producers[posInRound];
  }

  // Generate all blocks for a complete round
  function generateRound(roundNum: number, startBlockNum: number, skip: string[] = []): BlockRecord[] {
    const blocks: BlockRecord[] = [];
    const roundStartSlot = roundNum * SLOTS_PER_ROUND;

    for (let slotOffset = 0; slotOffset < SLOTS_PER_ROUND; slotOffset++) {
      const slot = roundStartSlot + slotOffset;
      const producer = expectedProducer(slot);
      if (skip.includes(producer)) continue; // simulate missed blocks
      blocks.push(makeBlock(startBlockNum + blocks.length, slot, producer));
    }
    return blocks;
  }

  function feedBlocks(blocks: BlockRecord[]) {
    let result = null;
    for (const block of blocks) {
      const r = evaluator.processBlock(block);
      if (r) result = r;
    }
    return result;
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-round-'));
    db = new Database(tmpDir);
    schedule = new ScheduleTracker('libre', 'mainnet', db);
    schedule.updateSchedule(
      1,
      producers.map((p) => ({ producer_name: p, block_signing_key: 'EOS000' })),
      1,
      '2000-01-01T00:00:00.000Z'
    );
    evaluator = new RoundEvaluator(chainConfig, db, schedule);
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should not emit during the first round', () => {
    const blocks = generateRound(100, 1);
    const result = feedBlocks(blocks);
    expect(result).toBeNull();
  });

  it('should emit when crossing into the next round', () => {
    const round1 = generateRound(100, 1);
    feedBlocks(round1);

    // First block of round 101 triggers evaluation of round 100
    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    expect(result!.roundNumber).toBe(100);
    expect(result!.producersProduced).toBe(3);
    expect(result!.producersMissed).toBe(0);
  });

  it('should correctly evaluate a perfect round', () => {
    const round1 = generateRound(100, 1);
    feedBlocks(round1);

    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    for (const pr of result!.producerResults) {
      expect(pr.blocksProduced).toBe(4);
      expect(pr.blocksMissed).toBe(0);
      expect(pr.blocksExpected).toBe(4);
    }
  });

  it('should never have blocksProduced > blocksExpected', () => {
    // Even if a producer somehow has extra blocks in the round data,
    // blocksProduced is capped at blocksPerBp
    const round1 = generateRound(100, 1);
    feedBlocks(round1);

    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    for (const pr of result!.producerResults) {
      expect(pr.blocksProduced).toBeLessThanOrEqual(pr.blocksExpected);
    }
  });

  it('should detect a producer that missed all blocks', () => {
    // bravo misses all their blocks
    const blocks = generateRound(100, 1, ['bravo']);
    feedBlocks(blocks);

    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    expect(result!.producersMissed).toBe(1);

    const bravoResult = result!.producerResults.find(p => p.producer === 'bravo');
    expect(bravoResult!.blocksProduced).toBe(0);
    expect(bravoResult!.blocksMissed).toBe(4);
  });

  it('should detect partial missed blocks', () => {
    const roundStartSlot = 100 * SLOTS_PER_ROUND;
    const blocks: BlockRecord[] = [];
    let blockNum = 1;

    // alpha: all 4 blocks
    for (let i = 0; i < 4; i++) {
      blocks.push(makeBlock(blockNum++, roundStartSlot + i, 'alpha'));
    }
    // bravo: only 2 of 4 blocks
    for (let i = 0; i < 2; i++) {
      blocks.push(makeBlock(blockNum++, roundStartSlot + 4 + i, 'bravo'));
    }
    // charlie: all 4 blocks
    for (let i = 0; i < 4; i++) {
      blocks.push(makeBlock(blockNum++, roundStartSlot + 8 + i, 'charlie'));
    }

    feedBlocks(blocks);

    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(blockNum, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    const bravoResult = result!.producerResults.find(p => p.producer === 'bravo');
    expect(bravoResult!.blocksProduced).toBe(2);
    expect(bravoResult!.blocksMissed).toBe(2);
  });

  it('should handle multiple offline producers', () => {
    // Both bravo and charlie miss
    const blocks = generateRound(100, 1, ['bravo', 'charlie']);
    feedBlocks(blocks);

    const slot = 101 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    expect(result).not.toBeNull();
    expect(result!.producersProduced).toBe(1);
    expect(result!.producersMissed).toBe(2);
  });

  it('should persist round results to database', () => {
    const blocks = generateRound(100, 1);
    feedBlocks(blocks);

    const slot = 101 * SLOTS_PER_ROUND;
    evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(1);
    expect(rounds[0].round_number).toBe(100);

    const rp = db.getRoundProducers(rounds[0].id);
    expect(rp).toHaveLength(3);
  });

  it('should persist missed block events', () => {
    const blocks = generateRound(100, 1, ['bravo']);
    feedBlocks(blocks);

    const slot = 101 * SLOTS_PER_ROUND;
    evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    const events = db.getMissedBlockEvents('libre', 'mainnet');
    const bravoEvent = events.find((e: any) => e.producer === 'bravo');
    expect(bravoEvent).toBeDefined();
    expect(bravoEvent!.blocks_missed).toBe(4);
  });

  it('should handle multiple consecutive rounds', () => {
    const results = [];

    // Round 100: perfect
    feedBlocks(generateRound(100, 1));
    // Round 101: bravo misses
    const r101 = generateRound(101, 100, ['bravo']);
    const r = feedBlocks(r101);
    if (r) results.push(r);

    // Round 102 start triggers eval of 101
    const slot = 102 * SLOTS_PER_ROUND;
    const r2 = evaluator.processBlock(makeBlock(200, slot, expectedProducer(slot)));
    if (r2) results.push(r2);

    expect(results).toHaveLength(2);
    expect(results[0].roundNumber).toBe(100);
    expect(results[0].producersMissed).toBe(0);
    expect(results[1].roundNumber).toBe(101);
    expect(results[1].producersMissed).toBe(1);
  });

  it('should track first complete round', () => {
    feedBlocks(generateRound(100, 1));
    const slot = 101 * SLOTS_PER_ROUND;
    evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    const first = db.getState('libre', 'mainnet', 'first_complete_round');
    expect(first).toBe('100');
  });

  it('should persist state for restart', () => {
    feedBlocks(generateRound(100, 1));
    const slot = 101 * SLOTS_PER_ROUND;
    evaluator.processBlock(makeBlock(100, slot, expectedProducer(slot)));

    const savedRound = db.getState('libre', 'mainnet', 'current_round');
    expect(savedRound).toBe('101');

    // New evaluator should restore
    const evaluator2 = new RoundEvaluator(chainConfig, db, schedule);
    expect(evaluator2.round).toBe(101);
  });

  it('should handle skipped rounds (long outage)', () => {
    // Round 100
    feedBlocks(generateRound(100, 1));

    // Jump to round 105 (rounds 101-104 had no blocks at all)
    const slot = 105 * SLOTS_PER_ROUND;
    const result = evaluator.processBlock(makeBlock(200, slot, expectedProducer(slot)));

    // Should evaluate round 100
    expect(result).not.toBeNull();
    expect(result!.roundNumber).toBe(100);

    // currentRound should jump to 105
    expect(evaluator.round).toBe(105);
  });
});
