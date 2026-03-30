import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Database } from '../../src/store/Database.js';
import { Summarizer } from '../../src/store/Summarizer.js';
import fs from 'fs';
import path from 'path';
import os from 'os';

describe('Summarizer', () => {
  let db: Database;
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'core-monitor-summarizer-'));
    db = new Database(tmpDir);

    // Seed rounds with producers
    for (let i = 0; i < 10; i++) {
      const roundId = db.insertRound({
        chain: 'libre',
        network: 'mainnet',
        round_number: i + 1,
        schedule_version: 1,
        timestamp_start: `2026-03-20T${String(i).padStart(2, '0')}:00:00.000`,
        timestamp_end: `2026-03-20T${String(i).padStart(2, '0')}:02:06.000`,
        producers_scheduled: 2,
        producers_produced: i < 8 ? 2 : 1,
        producers_missed: i < 8 ? 0 : 1,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'reliable_bp',
        position: 0,
        blocks_expected: 12,
        blocks_produced: 12,
        blocks_missed: 0,
        first_block: i * 24,
        last_block: i * 24 + 11,
      });

      db.insertRoundProducer({
        round_id: roundId,
        producer: 'flaky_bp',
        position: 1,
        blocks_expected: 12,
        blocks_produced: i < 8 ? 12 : 0,
        blocks_missed: i < 8 ? 0 : 12,
        first_block: i < 8 ? i * 24 + 12 : null,
        last_block: i < 8 ? i * 24 + 23 : null,
      });
    }

    // Seed a fork event
    db.insertForkEvent({
      chain: 'libre',
      network: 'mainnet',
      round_id: null,
      block_number: 50,
      original_producer: 'flaky_bp',
      replacement_producer: 'reliable_bp',
      timestamp: '2026-03-20T05:00:00.000',
    });
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should generate weekly summary with correct stats', () => {
    const summarizer = new Summarizer(tmpDir);
    summarizer.generateWeeklySummary('2026-03-14', '2026-03-21');
    summarizer.close();

    const summaries = db.getWeeklySummaries('libre', 'mainnet', 4);
    expect(summaries.length).toBeGreaterThanOrEqual(2);

    const reliable = summaries.find((s: any) => s.producer === 'reliable_bp');
    const flaky = summaries.find((s: any) => s.producer === 'flaky_bp');

    expect(reliable).toBeDefined();
    expect(reliable!.rounds_scheduled).toBe(10);
    expect(reliable!.blocks_produced).toBe(120);
    expect(reliable!.reliability_pct).toBe(100);

    expect(flaky).toBeDefined();
    expect(flaky!.rounds_scheduled).toBe(10);
    expect(flaky!.rounds_missed).toBe(2);
    expect(flaky!.blocks_produced).toBe(96);
    expect(flaky!.blocks_missed).toBe(24);
    expect(flaky!.reliability_pct).toBe(80);
    expect(flaky!.fork_count).toBe(1);
  });

  it('should generate monthly summary', () => {
    const summarizer = new Summarizer(tmpDir);
    summarizer.generateMonthlySummary('2026-03-01', '2026-03-31');
    summarizer.close();

    const summaries = db.getMonthlySummaries('libre', 'mainnet', 3);
    expect(summaries.length).toBeGreaterThanOrEqual(2);
  });

  it('should be idempotent (upsert on conflict)', () => {
    const summarizer = new Summarizer(tmpDir);
    summarizer.generateWeeklySummary('2026-03-14', '2026-03-21');
    // Run again — should update, not duplicate
    summarizer.generateWeeklySummary('2026-03-14', '2026-03-21');
    summarizer.close();

    const summaries = db.getWeeklySummaries('libre', 'mainnet', 4);
    const reliableEntries = summaries.filter((s: any) => s.producer === 'reliable_bp');
    expect(reliableEntries).toHaveLength(1);
  });
});
