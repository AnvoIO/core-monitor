import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { RoundEvaluator, timestampToSlot, type BlockRecord } from '../../src/monitor/RoundEvaluator.js';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { Database } from '../../src/store/Database.js';
import { createTestDb, cleanTestDb } from '../setup.js';
import type { ChainConfig } from '../../src/config.js';

describe('RoundEvaluator (slot-based)', () => {
  let db: Database;
  let schedule: ScheduleTracker;
  let evaluator: RoundEvaluator;

  const chainConfig: ChainConfig = {
    id: 'libre_mainnet', chain: 'libre', network: 'mainnet',
    shipUrl: 'ws://localhost:8088', apiUrl: 'http://localhost:8888',
    chainId: 'abc123', scheduleSize: 3, blocksPerBp: 4, blockTimeMs: 500,
    api: { port: 3000, host: '0.0.0.0', corsOrigin: '*' },
  } as any;

  const producers = ['alpha', 'bravo', 'charlie'];
  const EPOCH = Date.UTC(2000, 0, 1);
  const SLOTS_PER_ROUND = 12;

  function slotToTimestamp(slot: number): string {
    return new Date(EPOCH + slot * 500).toISOString();
  }

  function expectedProducer(slot: number): string {
    return producers[Math.floor((slot % SLOTS_PER_ROUND) / 4)];
  }

  function generateRound(roundNum: number, startBlockNum: number, skip: string[] = []): BlockRecord[] {
    const blocks: BlockRecord[] = [];
    const startSlot = roundNum * SLOTS_PER_ROUND;
    for (let i = 0; i < SLOTS_PER_ROUND; i++) {
      const slot = startSlot + i;
      const producer = expectedProducer(slot);
      if (skip.includes(producer)) continue;
      blocks.push({ block_num: startBlockNum + blocks.length, producer, timestamp: slotToTimestamp(slot), schedule_version: 1 });
    }
    return blocks;
  }

  async function feedBlocks(blocks: BlockRecord[]) {
    let result = null;
    for (const b of blocks) {
      const r = await evaluator.processBlock(b);
      if (r) result = r;
    }
    return result;
  }

  beforeAll(async () => {
    db = await createTestDb();
  });

  beforeEach(async () => {
    await cleanTestDb();
    schedule = new ScheduleTracker('libre', 'mainnet', db);
    await schedule.init();
    await schedule.updateSchedule(
      1, producers.map(p => ({ producer_name: p, block_signing_key: 'EOS000' })),
      1, '2000-01-01T00:00:00.000Z'
    );
    evaluator = new RoundEvaluator(chainConfig, db, schedule);
    await evaluator.init();
  });

  afterAll(async () => {
    await db.close();
  });

  it('should discard first partial round', async () => {
    await feedBlocks(generateRound(100, 1));
    const slot = 101 * SLOTS_PER_ROUND;
    const result = await evaluator.processBlock({ block_num: 100, producer: expectedProducer(slot), timestamp: slotToTimestamp(slot), schedule_version: 1 });
    expect(result).toBeNull(); // first round discarded
  });

  it('should evaluate second round correctly', async () => {
    await feedBlocks(generateRound(100, 1));
    // Trigger discard of round 100
    const slot101 = 101 * SLOTS_PER_ROUND;
    await evaluator.processBlock({ block_num: 100, producer: expectedProducer(slot101), timestamp: slotToTimestamp(slot101), schedule_version: 1 });
    // Feed round 101
    await feedBlocks(generateRound(101, 200).slice(1)); // skip first, already fed
    // Trigger eval of round 101
    const slot102 = 102 * SLOTS_PER_ROUND;
    const result = await evaluator.processBlock({ block_num: 300, producer: expectedProducer(slot102), timestamp: slotToTimestamp(slot102), schedule_version: 1 });
    expect(result).not.toBeNull();
    expect(result!.producersProduced).toBe(3);
    expect(result!.producersMissed).toBe(0);
  });

  it('should never have blocksProduced > blocksExpected', async () => {
    await feedBlocks(generateRound(100, 1));
    const slot101 = 101 * SLOTS_PER_ROUND;
    await evaluator.processBlock({ block_num: 100, producer: expectedProducer(slot101), timestamp: slotToTimestamp(slot101), schedule_version: 1 });
    await feedBlocks(generateRound(101, 200).slice(1));
    const slot102 = 102 * SLOTS_PER_ROUND;
    const result = await evaluator.processBlock({ block_num: 300, producer: expectedProducer(slot102), timestamp: slotToTimestamp(slot102), schedule_version: 1 });
    expect(result).not.toBeNull();
    for (const pr of result!.producerResults) {
      expect(pr.blocksProduced).toBeLessThanOrEqual(pr.blocksExpected);
    }
  });

  it('should detect missed producer', async () => {
    await feedBlocks(generateRound(100, 1, ['bravo']));
    const slot101 = 101 * SLOTS_PER_ROUND;
    await evaluator.processBlock({ block_num: 100, producer: expectedProducer(slot101), timestamp: slotToTimestamp(slot101), schedule_version: 1 });
    await feedBlocks(generateRound(101, 200, ['bravo']).slice(1));
    const slot102 = 102 * SLOTS_PER_ROUND;
    const result = await evaluator.processBlock({ block_num: 300, producer: expectedProducer(slot102), timestamp: slotToTimestamp(slot102), schedule_version: 1 });
    expect(result).not.toBeNull();
    expect(result!.producersMissed).toBe(1);
    const bravo = result!.producerResults.find(p => p.producer === 'bravo');
    expect(bravo!.blocksProduced).toBe(0);
    expect(bravo!.blocksMissed).toBe(4);
  });

  it('should persist rounds to database', async () => {
    await feedBlocks(generateRound(100, 1));
    const slot101 = 101 * SLOTS_PER_ROUND;
    await evaluator.processBlock({ block_num: 100, producer: expectedProducer(slot101), timestamp: slotToTimestamp(slot101), schedule_version: 1 });
    await feedBlocks(generateRound(101, 200).slice(1));
    const slot102 = 102 * SLOTS_PER_ROUND;
    await evaluator.processBlock({ block_num: 300, producer: expectedProducer(slot102), timestamp: slotToTimestamp(slot102), schedule_version: 1 });
    const rounds = await db.getRecentRounds('libre', 'mainnet');
    expect(rounds.length).toBeGreaterThanOrEqual(1);
  });

  it('should set schedule-relative round numbers', async () => {
    // Set activation at global round 95
    await evaluator.setScheduleActivation(slotToTimestamp(95 * SLOTS_PER_ROUND));
    await feedBlocks(generateRound(100, 1));
    const slot101 = 101 * SLOTS_PER_ROUND;
    await evaluator.processBlock({ block_num: 100, producer: expectedProducer(slot101), timestamp: slotToTimestamp(slot101), schedule_version: 1 });
    await feedBlocks(generateRound(101, 200).slice(1));
    const slot102 = 102 * SLOTS_PER_ROUND;
    const result = await evaluator.processBlock({ block_num: 300, producer: expectedProducer(slot102), timestamp: slotToTimestamp(slot102), schedule_version: 1 });
    expect(result).not.toBeNull();
    // display round = 101 - 95 = 6
    expect(result!.roundNumber).toBe(6);
  });
});
