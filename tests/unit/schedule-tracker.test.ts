import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ScheduleTracker } from '../../src/monitor/ScheduleTracker.js';
import { Database } from '../../src/store/Database.js';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('ScheduleTracker', () => {
  let db: Database;
  let tracker: ScheduleTracker;
  let tmpDir: string;

  const makeProducers = (names: string[]) =>
    names.map((n) => ({ producer_name: n, block_signing_key: 'EOS000' }));

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-schedule-'));
    db = new Database(tmpDir);
    tracker = new ScheduleTracker('libre', 'mainnet', db);
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should start with no schedule', () => {
    expect(tracker.schedule).toBeNull();
    expect(tracker.version).toBe(0);
    expect(tracker.producers).toEqual([]);
  });

  it('should accept initial schedule', () => {
    const changed = tracker.updateSchedule(
      1,
      makeProducers(['bp1', 'bp2', 'bp3']),
      1000,
      '2026-03-30T00:00:00.000'
    );

    expect(changed).toBe(true);
    expect(tracker.version).toBe(1);
    expect(tracker.producers).toEqual(['bp1', 'bp2', 'bp3']);
  });

  it('should reject same or older schedule version', () => {
    tracker.updateSchedule(5, makeProducers(['bp1']), 1000, '2026-03-30T00:00:00.000');

    const same = tracker.updateSchedule(5, makeProducers(['bp1']), 2000, '2026-03-30T00:01:00.000');
    expect(same).toBe(false);

    const older = tracker.updateSchedule(3, makeProducers(['bp1']), 3000, '2026-03-30T00:02:00.000');
    expect(older).toBe(false);

    expect(tracker.version).toBe(5);
  });

  it('should accept newer schedule version', () => {
    tracker.updateSchedule(1, makeProducers(['bp1', 'bp2']), 1000, '2026-03-30T00:00:00.000');
    const changed = tracker.updateSchedule(2, makeProducers(['bp1', 'bp3']), 2000, '2026-03-30T00:01:00.000');

    expect(changed).toBe(true);
    expect(tracker.version).toBe(2);
    expect(tracker.producers).toEqual(['bp1', 'bp3']);
  });

  it('should record schedule change in database', () => {
    tracker.updateSchedule(
      1,
      makeProducers(['bp1', 'bp2']),
      1000,
      '2026-03-30T00:00:00.000'
    );
    tracker.updateSchedule(
      2,
      makeProducers(['bp1', 'bp3']),
      2000,
      '2026-03-30T00:01:00.000'
    );

    const changes = db.getScheduleChanges('libre', 'mainnet');
    expect(changes).toHaveLength(2);

    // Most recent first
    const latest = changes[0];
    expect(latest.schedule_version).toBe(2);
    expect(JSON.parse(latest.producers_added)).toEqual(['bp3']);
    expect(JSON.parse(latest.producers_removed)).toEqual(['bp2']);
  });

  it('should persist and restore schedule across instances', () => {
    tracker.updateSchedule(
      3,
      makeProducers(['a', 'b', 'c']),
      5000,
      '2026-03-30T00:00:00.000'
    );

    // Create a new tracker — should restore from state
    const tracker2 = new ScheduleTracker('libre', 'mainnet', db);
    expect(tracker2.version).toBe(3);
    expect(tracker2.producers).toEqual(['a', 'b', 'c']);
  });

  it('should report producer position correctly', () => {
    tracker.updateSchedule(
      1,
      makeProducers(['alpha', 'bravo', 'charlie']),
      1000,
      '2026-03-30T00:00:00.000'
    );

    expect(tracker.getProducerPosition('alpha')).toBe(0);
    expect(tracker.getProducerPosition('bravo')).toBe(1);
    expect(tracker.getProducerPosition('charlie')).toBe(2);
    expect(tracker.getProducerPosition('unknown')).toBe(-1);
  });

  it('should return expected producer for position', () => {
    tracker.updateSchedule(
      1,
      makeProducers(['alpha', 'bravo', 'charlie']),
      1000,
      '2026-03-30T00:00:00.000'
    );

    expect(tracker.getExpectedProducerForPosition(0)).toBe('alpha');
    expect(tracker.getExpectedProducerForPosition(1)).toBe('bravo');
    expect(tracker.getExpectedProducerForPosition(2)).toBe('charlie');
    expect(tracker.getExpectedProducerForPosition(3)).toBeNull();
    expect(tracker.getExpectedProducerForPosition(-1)).toBeNull();
  });
});
