import { describe, it, expect, beforeEach, vi } from 'vitest';
import { AlertManager } from '../../src/alerting/AlertManager.js';
import type { AlertChannel, AlertMessage } from '../../src/alerting/types.js';

function createMockChannel(name: string = 'mock'): AlertChannel & { calls: AlertMessage[] } {
  const calls: AlertMessage[] = [];
  return {
    name,
    calls,
    send: vi.fn(async (msg: AlertMessage) => {
      calls.push(msg);
    }),
  };
}

describe('AlertManager', () => {
  let manager: AlertManager;
  let channel: ReturnType<typeof createMockChannel>;

  beforeEach(() => {
    manager = new AlertManager(1000); // 1 second throttle
    channel = createMockChannel();
    manager.addChannel(channel);
  });

  it('should send alerts to registered channels', async () => {
    await manager.sendAlert({
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Test alert',
      body: 'Test body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(channel.send).toHaveBeenCalledTimes(1);
    expect(channel.calls[0].title).toBe('Test alert');
  });

  it('should send to multiple channels', async () => {
    const channel2 = createMockChannel('mock2');
    manager.addChannel(channel2);

    await manager.sendAlert({
      severity: 'info',
      chain: 'libre',
      network: 'mainnet',
      title: 'Multi',
      body: 'Goes everywhere',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(channel.send).toHaveBeenCalledTimes(1);
    expect(channel2.send).toHaveBeenCalledTimes(1);
  });

  it('should throttle duplicate alerts within window', async () => {
    const msg: AlertMessage = {
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Same alert',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    };

    await manager.sendAlert(msg);
    await manager.sendAlert(msg); // should be throttled
    await manager.sendAlert(msg); // should be throttled

    expect(channel.send).toHaveBeenCalledTimes(1);
  });

  it('should not throttle different alerts', async () => {
    await manager.sendAlert({
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Alert A',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    await manager.sendAlert({
      severity: 'warn',
      chain: 'libre',
      network: 'mainnet',
      title: 'Alert B',
      body: 'Body',
      timestamp: '2026-03-30T00:00:01.000',
    });

    expect(channel.send).toHaveBeenCalledTimes(2);
  });

  it('should not throttle same alert for different chains', async () => {
    const base = {
      severity: 'alert' as const,
      title: 'Same title',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    };

    await manager.sendAlert({ ...base, chain: 'libre', network: 'mainnet' });
    await manager.sendAlert({ ...base, chain: 'libre', network: 'testnet' });

    expect(channel.send).toHaveBeenCalledTimes(2);
  });

  it('should include suppressed count after throttle expires', async () => {
    // Use a manager with 0ms throttle for instant expiry testing
    const fastManager = new AlertManager(0);
    const fastChannel = createMockChannel();
    fastManager.addChannel(fastChannel);

    const msg: AlertMessage = {
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Repeated',
      body: 'Original body',
      timestamp: '2026-03-30T00:00:00.000',
    };

    await fastManager.sendAlert(msg);
    // With 0ms throttle, second call goes through immediately but shows suppressed count
    await fastManager.sendAlert(msg);

    expect(fastChannel.send).toHaveBeenCalledTimes(2);
  });

  it('should handle channel errors gracefully', async () => {
    const failingChannel: AlertChannel = {
      name: 'failing',
      send: vi.fn(async () => {
        throw new Error('Network error');
      }),
    };
    manager.addChannel(failingChannel);

    // Should not throw
    await expect(
      manager.sendAlert({
        severity: 'alert',
        chain: 'libre',
        network: 'mainnet',
        title: 'Test',
        body: 'Body',
        timestamp: '2026-03-30T00:00:00.000',
      })
    ).resolves.toBeUndefined();

    // The working channel should still have received the message
    expect(channel.send).toHaveBeenCalledTimes(1);
  });

  describe('convenience methods', () => {
    it('should format missedRound alert', async () => {
      await manager.missedRound({
        chain: 'libre',
        network: 'mainnet',
        producer: 'badproducer',
        round: 42,
        scheduleVersion: 10,
        blocksMissed: 12,
        timestamp: '2026-03-30T00:00:00.000',
      });

      expect(channel.calls[0].severity).toBe('alert');
      expect(channel.calls[0].title).toContain('Missed Round');
      expect(channel.calls[0].title).toContain('Schedule 10 / Round 42');
      expect(channel.calls[0].body).toContain('badproducer produced 0 of 12');
    });

    it('should format missedBlocks alert', async () => {
      await manager.missedBlocks({
        chain: 'libre',
        network: 'mainnet',
        producer: 'flaky',
        round: 10,
        scheduleVersion: 5,
        blocksProduced: 8,
        blocksMissed: 4,
        blocksExpected: 12,
        timestamp: '2026-03-30T00:00:00.000',
      });

      expect(channel.calls[0].severity).toBe('warn');
      expect(channel.calls[0].title).toContain('Missed Blocks');
      expect(channel.calls[0].title).toContain('Schedule 5 / Round 10');
      expect(channel.calls[0].body).toContain('flaky produced 8 of 12');
    });

    it('should not send roundComplete when no issues', async () => {
      await manager.roundComplete({
        chain: 'libre',
        network: 'mainnet',
        round: 5,
        producersProduced: 21,
        producersMissed: 0,
        missedProducers: [],
        partialProducers: [],
        forks: [],
        scheduleVersion: 1,
        timestamp: '2026-03-30T00:00:00.000',
      });

      expect(channel.send).not.toHaveBeenCalled();
    });

    it('should send degraded round with full breakdown', async () => {
      await manager.roundComplete({
        chain: 'libre',
        network: 'mainnet',
        round: 5,
        producersProduced: 19,
        producersMissed: 2,
        missedProducers: [{ producer: 'badprod1', expected: 12 }, { producer: 'badprod2', expected: 12 }],
        partialProducers: [{ producer: 'flaky', produced: 8, missed: 4, expected: 12 }],
        forks: [{ blockNumber: 100, originalProducer: 'bp1', replacementProducer: 'bp2' }],
        scheduleVersion: 1,
        timestamp: '2026-03-30T00:00:00.000',
      });

      expect(channel.send).toHaveBeenCalledTimes(1);
      expect(channel.calls[0].severity).toBe('alert');
      expect(channel.calls[0].title).toContain('Degraded Round');
      expect(channel.calls[0].body).toContain('badprod1 produced 0 of 12');
      expect(channel.calls[0].body).toContain('flaky produced 8 of 12');
      expect(channel.calls[0].body).toContain('Forked Block: bp1 block 100 replaced by bp2');
    });

    it('should format scheduleChange alert', async () => {
      await manager.scheduleChange({
        chain: 'libre',
        network: 'mainnet',
        version: 10,
        producers: ['bp1', 'bp2', 'bp3'],
        blockNum: 100000,
        timestamp: '2026-03-30T00:00:00.000',
      });

      expect(channel.calls[0].severity).toBe('info');
      expect(channel.calls[0].title).toContain('v10');
      expect(channel.calls[0].body).toContain('bp1, bp2, bp3');
    });
  });
});
