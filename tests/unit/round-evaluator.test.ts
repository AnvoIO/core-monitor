import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { RoundEvaluator, type BlockRecord } from '../../src/monitor/RoundEvaluator.js';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { Database } from '../../src/store/Database.js';
import type { ChainConfig } from '../../src/config.js';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('RoundEvaluator', () => {
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
  let nextBlockNum = 1;
  let nextSecond = 0;

  function makeBlock(producer: string, scheduleVersion: number = 1): BlockRecord {
    const block: BlockRecord = {
      block_num: nextBlockNum++,
      producer,
      timestamp: `2026-03-30T00:${String(Math.floor(nextSecond / 60)).padStart(2, '0')}:${String(nextSecond % 60).padStart(2, '0')}.000`,
      schedule_version: scheduleVersion,
    };
    nextSecond++;
    return block;
  }

  // Feed N blocks from a producer, return any round result
  function feedProducer(producer: string, count: number, sv: number = 1) {
    let result = null;
    for (let i = 0; i < count; i++) {
      const r = evaluator.processBlock(makeBlock(producer, sv));
      if (r) result = r;
    }
    return result;
  }

  // Feed a full round (all producers, 4 blocks each), return any result
  function feedFullRound(sv: number = 1) {
    let result = null;
    for (const p of producers) {
      const r = feedProducer(p, 4, sv);
      if (r) result = r;
    }
    return result;
  }

  // Warm up: feed one full round (discarded as partial), then trigger
  // boundary with alpha's first block of next round.
  // After warmUp, the evaluator has 1 alpha block in the current round.
  function warmUp() {
    feedFullRound();
    // Trigger boundary — discards partial round
    evaluator.processBlock(makeBlock('alpha'));
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-round-'));
    db = new Database(tmpDir);
    schedule = new ScheduleTracker('libre', 'mainnet', db);
    schedule.updateSchedule(
      1,
      producers.map((p) => ({ producer_name: p, block_signing_key: 'EOS000' })),
      1,
      '2026-03-30T00:00:00.000'
    );
    evaluator = new RoundEvaluator(chainConfig, db, schedule);
    nextBlockNum = 1;
    nextSecond = 0;
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should not emit a result during the first round', () => {
    const result = feedFullRound();
    expect(result).toBeNull();
  });

  it('should discard the first partial round and not persist it', () => {
    feedFullRound();
    const result = evaluator.processBlock(makeBlock('alpha'));
    expect(result).toBeNull();
    expect(db.getRecentRounds('libre', 'mainnet')).toHaveLength(0);
  });

  it('should emit a result for the first complete round', () => {
    warmUp();
    // Feed remaining 3 alpha + full bravo + full charlie
    feedProducer('alpha', 3);
    feedProducer('bravo', 4);
    feedProducer('charlie', 4);
    // Trigger next boundary
    const result = evaluator.processBlock(makeBlock('alpha'));

    expect(result).not.toBeNull();
    expect(result!.producersProduced).toBe(3);
    expect(result!.producersMissed).toBe(0);
  });

  it('should correctly evaluate a perfect round', () => {
    warmUp();
    feedProducer('alpha', 3);
    feedProducer('bravo', 4);
    feedProducer('charlie', 4);
    const result = evaluator.processBlock(makeBlock('alpha'));

    expect(result).not.toBeNull();
    for (const pr of result!.producerResults) {
      expect(pr.blocksProduced).toBe(4);
      expect(pr.blocksMissed).toBe(0);
    }
  });

  it('should detect a producer that missed their entire slot', () => {
    warmUp();
    // alpha: 3 more (warmup has 1)
    feedProducer('alpha', 3);
    // bravo skipped
    feedProducer('charlie', 4);
    const result = evaluator.processBlock(makeBlock('alpha'));

    expect(result).not.toBeNull();
    const bravoResult = result!.producerResults.find((p) => p.producer === 'bravo');
    expect(bravoResult!.blocksProduced).toBe(0);
    expect(bravoResult!.blocksMissed).toBe(4);
    expect(result!.producersMissed).toBe(1);
  });

  it('should detect partial missed blocks', () => {
    warmUp();
    feedProducer('alpha', 3);
    feedProducer('bravo', 2); // only 2 of 4
    feedProducer('charlie', 4);
    const result = evaluator.processBlock(makeBlock('alpha'));

    expect(result).not.toBeNull();
    const bravoResult = result!.producerResults.find((p) => p.producer === 'bravo');
    expect(bravoResult!.blocksProduced).toBe(2);
    expect(bravoResult!.blocksMissed).toBe(2);
  });

  it('should persist round results to database', () => {
    warmUp();
    feedProducer('alpha', 3);
    feedProducer('bravo', 4);
    feedProducer('charlie', 4);
    evaluator.processBlock(makeBlock('alpha'));

    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(1);
    const roundProducers = db.getRoundProducers(rounds[0].id);
    expect(roundProducers).toHaveLength(3);
  });

  it('should persist missed block events to database', () => {
    warmUp();
    feedProducer('alpha', 3);
    // bravo misses
    feedProducer('charlie', 4);
    evaluator.processBlock(makeBlock('alpha'));

    const events = db.getMissedBlockEvents('libre', 'mainnet');
    const bravoEvent = events.find((e: any) => e.producer === 'bravo');
    expect(bravoEvent).toBeDefined();
    expect(bravoEvent!.blocks_missed).toBe(4);
  });

  it('should persist state and resume across instances', () => {
    warmUp();
    feedProducer('alpha', 3);
    feedProducer('bravo', 4);
    feedProducer('charlie', 4);
    evaluator.processBlock(makeBlock('alpha'));

    const evaluator2 = new RoundEvaluator(chainConfig, db, schedule);
    expect(evaluator2.round).toBeGreaterThan(0);
  });

  it('should handle multiple consecutive rounds', () => {
    warmUp();

    // Round 1: perfect (3 more alpha + bravo + charlie)
    feedProducer('alpha', 3);
    feedProducer('bravo', 4);
    feedProducer('charlie', 4);

    // Round 2 starts with alpha — triggers eval of round 1
    // Then bravo misses in round 2
    const r1 = feedProducer('alpha', 4);
    expect(r1).not.toBeNull();
    expect(r1!.producersMissed).toBe(0);

    feedProducer('charlie', 4);

    // Trigger eval of round 2
    const r2 = evaluator.processBlock(makeBlock('alpha'));
    expect(r2).not.toBeNull();
    expect(r2!.producersMissed).toBe(1);

    const dbRounds = db.getRecentRounds('libre', 'mainnet');
    expect(dbRounds).toHaveLength(2);
  });

  it('should discard partial round after schedule version change', () => {
    warmUp();

    // Start a round with schedule v2
    feedProducer('alpha', 3, 2);
    feedProducer('charlie', 4, 2);
    // Trigger boundary — should discard since it's partial in new schedule
    const result = evaluator.processBlock(makeBlock('alpha', 2));
    expect(result).toBeNull();

    // No v2 rounds in DB
    const rounds = db.getRecentRounds('libre', 'mainnet');
    const v2Rounds = rounds.filter((r: any) => r.schedule_version === 2);
    expect(v2Rounds).toHaveLength(0);
  });
});
