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

describe('Chain Monitor Pipeline Integration', () => {
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

  function makeBlock(num: number, producer: string, seconds: number): BlockRecord {
    return {
      block_num: num,
      producer,
      timestamp: `2026-03-30T00:${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}.000`,
      schedule_version: 1,
    };
  }

  function createMockChannel(): AlertChannel & { calls: AlertMessage[] } {
    const calls: AlertMessage[] = [];
    return {
      name: 'mock',
      calls,
      send: vi.fn(async (msg: AlertMessage) => {
        calls.push(msg);
      }),
    };
  }

  // Feed a complete round into the evaluator
  function feedFullRound(startBlock: number, startSecond: number, producerList: string[] = producers) {
    let blockNum = startBlock;
    let second = startSecond;
    let result = null;
    for (const producer of producerList) {
      for (let i = 0; i < 4; i++) {
        const r = evaluator.processBlock(makeBlock(blockNum++, producer, second++));
        if (r) result = r;
      }
    }
    return { result, nextBlock: blockNum, nextSecond: second };
  }

  // Warm up: feed one round (discarded as partial) + trigger boundary
  function warmUp(): { nextBlock: number; nextSecond: number } {
    const { nextBlock, nextSecond } = feedFullRound(1, 0);
    // Trigger boundary — partial round is discarded
    evaluator.processBlock(makeBlock(nextBlock, 'alpha', nextSecond));
    return { nextBlock: nextBlock + 1, nextSecond: nextSecond + 1 };
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
      producers.map((p) => ({ producer_name: p, block_signing_key: 'EOS000' })),
      1,
      '2026-03-30T00:00:00.000'
    );
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should process a full round with no issues and persist to DB', () => {
    const { nextBlock, nextSecond } = warmUp();

    // warmUp left 1 alpha block in the current round, feed 3 more
    let blockNum = nextBlock;
    let second = nextSecond;
    for (let i = 0; i < 3; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    }
    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'bravo', second++));
    }
    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'charlie', second++));
    }

    const result = evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));

    expect(result).not.toBeNull();
    expect(result!.producersProduced).toBe(3);
    expect(result!.producersMissed).toBe(0);

    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(1);

    const roundProducers = db.getRoundProducers(rounds[0].id);
    expect(roundProducers).toHaveLength(3);
    for (const rp of roundProducers) {
      expect(rp.blocks_produced).toBe(4);
    }
  });

  it('should detect missed round and trigger alert', async () => {
    const { nextBlock, nextSecond } = warmUp();

    let blockNum = nextBlock;
    let second = nextSecond;

    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    }
    // bravo completely missed
    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'charlie', second++));
    }

    const result = evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    expect(result).not.toBeNull();
    expect(result!.producersMissed).toBe(1);

    const bravoResult = result!.producerResults.find((p) => p.producer === 'bravo');
    expect(bravoResult).toBeDefined();
    expect(bravoResult!.blocksProduced).toBe(0);

    await alertManager.missedRound({
      chain: 'libre',
      network: 'mainnet',
      producer: 'bravo',
      round: result!.roundNumber,
      scheduleVersion: result!.scheduleVersion,
      blocksMissed: bravoResult!.blocksExpected,
      timestamp: result!.timestampEnd,
    });

    expect(mockChannel.calls).toHaveLength(1);
    expect(mockChannel.calls[0].severity).toBe('alert');
    expect(mockChannel.calls[0].title).toContain('bravo');

    const events = db.getMissedBlockEvents('libre', 'mainnet');
    const bravoEvent = events.find((e: any) => e.producer === 'bravo');
    expect(bravoEvent).toBeDefined();
    expect(bravoEvent.blocks_missed).toBe(4);
  });

  it('should detect partial misses and trigger warning', async () => {
    const { nextBlock, nextSecond } = warmUp();

    let blockNum = nextBlock;
    let second = nextSecond;

    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    }
    for (let i = 0; i < 2; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'bravo', second++));
    }
    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'charlie', second++));
    }

    const result = evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    expect(result).not.toBeNull();

    const bravoResult = result!.producerResults.find((p) => p.producer === 'bravo');
    expect(bravoResult!.blocksProduced).toBe(2);
    expect(bravoResult!.blocksMissed).toBe(2);

    await alertManager.missedBlocks({
      chain: 'libre',
      network: 'mainnet',
      producer: 'bravo',
      round: result!.roundNumber,
      scheduleVersion: result!.scheduleVersion,
      blocksProduced: bravoResult!.blocksProduced,
      blocksMissed: bravoResult!.blocksMissed,
      blocksExpected: bravoResult!.blocksExpected,
      timestamp: result!.timestampEnd,
    });

    expect(mockChannel.calls).toHaveLength(1);
    expect(mockChannel.calls[0].severity).toBe('warn');
    expect(mockChannel.calls[0].body).toContain('2 of 4 blocks missed');
  });

  it('should handle schedule change mid-stream', () => {
    const { nextBlock, nextSecond } = warmUp();

    let blockNum = nextBlock;
    let second = nextSecond;

    for (let i = 0; i < 4; i++) {
      evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    }

    const newProducers = ['alpha', 'bravo', 'delta'];
    const changed = schedule.updateSchedule(
      2,
      newProducers.map((p) => ({ producer_name: p, block_signing_key: 'EOS000' })),
      blockNum,
      `2026-03-30T00:00:${String(second).padStart(2, '0')}.000`
    );
    expect(changed).toBe(true);
    expect(schedule.version).toBe(2);
    expect(schedule.producers).toContain('delta');
    expect(schedule.producers).not.toContain('charlie');

    const changes = db.getScheduleChanges('libre', 'mainnet');
    expect(changes).toHaveLength(2);
    const latest = changes[0];
    expect(JSON.parse(latest.producers_added)).toEqual(['delta']);
    expect(JSON.parse(latest.producers_removed)).toEqual(['charlie']);
  });

  it('should persist state and resume correctly', () => {
    const { nextBlock, nextSecond } = warmUp();
    const { nextBlock: nb, nextSecond: ns } = feedFullRound(nextBlock, nextSecond);
    evaluator.processBlock(makeBlock(nb, 'alpha', ns));

    const lastBlock = db.getState('libre', 'mainnet', 'last_block');
    expect(parseInt(lastBlock!, 10)).toBeGreaterThan(0);

    const savedSchedule = db.getState('libre', 'mainnet', 'schedule');
    expect(savedSchedule).not.toBeNull();
    const parsed = JSON.parse(savedSchedule!);
    expect(parsed.version).toBe(1);
    expect(parsed.producers).toEqual(producers);

    const schedule2 = new ScheduleTracker('libre', 'mainnet', db);
    expect(schedule2.version).toBe(1);
    expect(schedule2.producers).toEqual(producers);

    const evaluator2 = new RoundEvaluator(chainConfig, db, schedule2);
    expect(evaluator2.round).toBeGreaterThan(0);
  });

  it('should handle multiple consecutive rounds with varying results', async () => {
    const { nextBlock, nextSecond } = warmUp();

    let blockNum = nextBlock;
    let second = nextSecond;
    const results = [];

    // Round A: perfect (3 more alpha since warmup has 1 + bravo + charlie)
    for (let i = 0; i < 3; i++) {
      const r = evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
      if (r) results.push(r);
    }
    for (const producer of ['bravo', 'charlie']) {
      for (let i = 0; i < 4; i++) {
        const r = evaluator.processBlock(makeBlock(blockNum++, producer, second++));
        if (r) results.push(r);
      }
    }

    // Round B: bravo misses (alpha 4 blocks triggers eval of A)
    for (let i = 0; i < 4; i++) {
      const r = evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
      if (r) results.push(r);
    }
    for (let i = 0; i < 4; i++) {
      const r = evaluator.processBlock(makeBlock(blockNum++, 'charlie', second++));
      if (r) results.push(r);
    }

    const r = evaluator.processBlock(makeBlock(blockNum++, 'alpha', second++));
    if (r) results.push(r);

    expect(results).toHaveLength(2);
    expect(results[0].producersMissed).toBe(0);
    expect(results[1].producersMissed).toBe(1);

    const rounds = db.getRecentRounds('libre', 'mainnet');
    expect(rounds).toHaveLength(2);

    const stats = db.getAllProducerStats('libre', 'mainnet', 30);
    expect(stats.length).toBeGreaterThanOrEqual(2);

    const alphaStats = stats.find((s: any) => s.producer === 'alpha');
    const bravoStats = stats.find((s: any) => s.producer === 'bravo');
    expect(alphaStats!.reliability_pct).toBe(100);
    expect(bravoStats!.reliability_pct).toBeLessThan(100);
  });
});
