import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { Database } from '../../src/store/Database.js';
import { createTestDb, cleanTestDb } from '../setup.js';

describe('ScheduleTracker', () => {
  let db: Database;
  let tracker: ScheduleTracker;

  const makeProducers = (names: string[]) =>
    names.map(n => ({ producer_name: n, block_signing_key: 'EOS000' }));

  beforeAll(async () => { db = await createTestDb(); });
  beforeEach(async () => {
    await cleanTestDb();
    tracker = new ScheduleTracker('libre', 'mainnet', db);
    await tracker.init();
  });
  afterAll(async () => { await db.close(); });

  it('should start with no schedule', () => {
    expect(tracker.schedule).toBeNull();
    expect(tracker.version).toBe(0);
  });

  it('should accept initial schedule', async () => {
    const result = await tracker.updateSchedule(1, makeProducers(['bp1', 'bp2']), 1000, '2026-03-30T00:00:00.000');
    expect(result).toBeTruthy();
    expect(tracker.version).toBe(1);
    expect(tracker.producers).toEqual(['bp1', 'bp2']);
  });

  it('should reject same or older version', async () => {
    await tracker.updateSchedule(5, makeProducers(['bp1']), 1000, '2026-03-30T00:00:00.000');
    expect(await tracker.updateSchedule(5, makeProducers(['bp1']), 2000, '2026-03-30T00:01:00.000')).toBe(false);
    expect(await tracker.updateSchedule(3, makeProducers(['bp1']), 3000, '2026-03-30T00:02:00.000')).toBe(false);
  });

  it('should persist and restore', async () => {
    await tracker.updateSchedule(3, makeProducers(['a', 'b', 'c']), 5000, '2026-03-30T00:00:00.000');
    const tracker2 = new ScheduleTracker('libre', 'mainnet', db);
    await tracker2.init();
    expect(tracker2.version).toBe(3);
    expect(tracker2.producers).toEqual(['a', 'b', 'c']);
  });

  it('should record schedule changes in DB', async () => {
    await tracker.updateSchedule(1, makeProducers(['bp1', 'bp2']), 1000, '2026-03-30T00:00:00.000');
    await tracker.updateSchedule(2, makeProducers(['bp1', 'bp3']), 2000, '2026-03-30T00:01:00.000');
    const changes = await db.getScheduleChanges('libre', 'mainnet');
    expect(changes).toHaveLength(2);
    expect(JSON.parse(changes[0].producers_added)).toEqual(['bp3']);
    expect(JSON.parse(changes[0].producers_removed)).toEqual(['bp2']);
  });
});
