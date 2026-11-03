import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createTestDb, cleanTestDb } from '../setup.js';
import { Database } from '../../src/store/Database.js';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { RoundEvaluator } from '../../src/monitor/RoundEvaluator.js';
import { SavannaCatchupSchedule, CATCHUP_SAVANNA_STATE_KEY } from '../../src/monitor/SavannaCatchupSchedule.js';
import { slotToTimestamp } from '../../src/ship/ShipClient.js';
import { WIRE_EPOCH_MS } from '../../src/utils/chainTime.js';
import type { ChainConfig } from '../../src/config.js';

const config = {
  id: 'wire_testnet', chain: 'wire', network: 'testnet',
  scheduleSize: 21, blocksPerBp: 12, blockTimeMs: 500,
  blockTimestampEpochMs: WIRE_EPOCH_MS, systemAccount: 'sysio',
} as unknown as ChainConfig;

const auth = (name: string) => ({ producer_name: name, block_signing_key: `KEY_${name}` });

/** Block n is at slot SLOT0 + n, with a strong QC on its parent. */
const SLOT0 = 1000;
function block(n: number, producer: string, diff: any = null) {
  const slot = SLOT0 + n;
  return {
    producer,
    timestamp: slotToTimestamp(slot, WIRE_EPOCH_MS),
    savanna: { slot, qcClaim: { blockNum: n - 1, isStrong: true }, proposerDiff: diff },
  };
}

// Genesis producer sysio proposes [alpha, bravo] at block 10 (slot 1010, turn 1008).
// It's pending from turn 1020 and active from turn 1032, i.e. block 32.
const DIFF = {
  version: 1, proposalSlot: SLOT0 + 10, removeIndexes: [0], insertIndexes: [{ index: 0, value: auth('alpha') }, { index: 1, value: auth('bravo') }],
};
const ACTIVATION_BLOCK = 32;

describe('SavannaCatchupSchedule', () => {
  let db: Database;

  const make = async () => {
    const schedule = new ScheduleTracker('wire', 'testnet', db, 'catchup_');
    await schedule.init();
    const evaluator = new RoundEvaluator(config, db, schedule, 'catchup_', { batchInserts: true });
    await evaluator.init();
    return { schedule, evaluator, savanna: new SavannaCatchupSchedule(config, db, schedule, evaluator) };
  };

  const producerAt = (n: number) => (n < ACTIVATION_BLOCK ? 'sysio' : ['alpha', 'bravo'][Math.floor(((SLOT0 + n) % 24) / 12)]);

  const feed = async (s: SavannaCatchupSchedule, from: number, to: number) => {
    for (let n = from; n <= to; n++) {
      expect(await s.process(block(n, producerAt(n), n === 10 ? DIFF : null), n)).toBe(true);
    }
  };

  beforeAll(async () => {
    db = await createTestDb();
  });
  beforeEach(async () => {
    await cleanTestDb();
  });
  afterAll(async () => {
    await db.close();
  });

  it('starts from the genesis producer and activates proposals on the chain\'s timetable', async () => {
    const { schedule, savanna } = await make();
    await feed(savanna, 2, 40); // block 1 (genesis) has no producer and is skipped by the writer
    expect(schedule.version).toBe(1);
    expect(schedule.producers).toEqual(['alpha', 'bravo']);
    expect(savanna.activeVersion).toBe(1);

    const changes = await db.getScheduleChanges('wire', 'testnet', 10);
    const byVersion = Object.fromEntries(changes.map((c: any) => [c.schedule_version, Number(c.block_number)]));
    expect(byVersion).toEqual({ 0: 2, 1: ACTIVATION_BLOCK });
  });

  it('sizes rounds by the active schedule, not SCHEDULE_SIZE', async () => {
    const { savanna, evaluator } = await make();
    for (let n = 2; n <= 90; n++) {
      expect(await savanna.process(block(n, producerAt(n), n === 10 ? DIFF : null), n)).toBe(true);
      await evaluator.processBlock({ block_num: n, producer: producerAt(n), timestamp: block(n, '').timestamp, schedule_version: savanna.activeVersion! });
    }
    const { rows } = await (db as any).pool.query(
      `SELECT schedule_version, timestamp_start, timestamp_end, producers_scheduled, producers_produced, producers_missed
         FROM rounds WHERE chain = 'wire' ORDER BY timestamp_start`
    );
    const ts = (n: number) => block(n, '').timestamp;
    // v0 (1 producer): 12-slot rounds. [1002-1007] joined mid-round and [1020-1031] spans the
    // activation, so only [1008-1019] counts. v1 (2 producers): 24-slot rounds; [1032-1055]
    // is the first after activation, so [1056-1079] counts.
    expect(rows).toEqual([
      { schedule_version: 0, timestamp_start: ts(8), timestamp_end: ts(19), producers_scheduled: 1, producers_produced: 1, producers_missed: 0 },
      { schedule_version: 1, timestamp_start: ts(56), timestamp_end: ts(79), producers_scheduled: 2, producers_produced: 2, producers_missed: 0 },
    ]);
  });

  it('refuses a fresh run that does not start at the first block', async () => {
    const { savanna } = await make();
    expect(await savanna.process(block(500, 'sysio'), 500)).toBe(false);
    expect(await db.getState('wire', 'testnet', CATCHUP_SAVANNA_STATE_KEY)).toBeNull();
  });

  it('resumes from a saved tracker without re-applying blocks it already saw', async () => {
    const first = await make();
    await feed(first.savanna, 2, 20); // proposal saved at block 10, now pending
    await first.savanna.persist(); // as at shutdown: tracker saved through block 20

    // The evaluator's last block lags behind the tracker's after a crash; the stream resumes
    // from the earlier of the two, and the tracker skips what it has seen.
    expect(await SavannaCatchupSchedule.resumeBlock(db, config, 16)).toBe(16);
    expect(await SavannaCatchupSchedule.resumeBlock(db, config, 30)).toBe(21);

    const second = await make();
    await feed(second.savanna, 16, 40);
    expect(second.schedule.version).toBe(1);
    const changes = await db.getScheduleChanges('wire', 'testnet', 10);
    expect(changes.find((c: any) => c.schedule_version === 1)?.block_number).toBe(ACTIVATION_BLOCK);
  });
});
