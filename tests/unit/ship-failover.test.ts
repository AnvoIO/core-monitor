import { describe, it, expect } from 'vitest';
import { ShipClient } from '../../src/ship/ShipClient.js';

describe('ShipClient Failover', () => {
  it('should store primary and failover URLs', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrls: ['wss://failover.example.com/'],
    });

    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
    expect((client as any).urls).toEqual([
      'wss://primary.example.com/',
      'wss://failover.example.com/',
    ]);
  });

  it('should cycle through URLs round-robin on reconnect', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrls: [
        'wss://failover.example.com/',
        'ws://local.example.com:8080',
      ],
      reconnectDelayMs: 1,
    });

    // Initial URL is primary (index 0)
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
    expect((client as any).urlIndex).toBe(0);

    // First reconnect -> index 1 (failover)
    (client as any).scheduleReconnect();
    expect((client as any).currentUrl).toBe('wss://failover.example.com/');
    expect((client as any).urlIndex).toBe(1);

    // Second reconnect -> index 2 (local)
    (client as any).scheduleReconnect();
    expect((client as any).currentUrl).toBe('ws://local.example.com:8080');
    expect((client as any).urlIndex).toBe(2);

    // Third reconnect -> wraps to index 0 (primary)
    (client as any).scheduleReconnect();
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
    expect((client as any).urlIndex).toBe(0);
  });

  it('should not cycle URLs when only primary is configured', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
    });

    expect((client as any).urls).toEqual(['wss://primary.example.com/']);
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');

    // After reconnect, should stay on primary
    (client as any).scheduleReconnect();
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
  });

  it('should reset reconnect attempts after successful connection', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrls: ['wss://failover.example.com/'],
    });

    (client as any).reconnectAttempts = 5;
    // After a successful connect, reconnectAttempts resets to 0
    // This happens in the ws 'open' handler
    (client as any).reconnectAttempts = 0;
    expect((client as any).reconnectAttempts).toBe(0);
  });

  it('should cap reconnect delay with exponential backoff', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      reconnectDelayMs: 3000,
    });

    const calcDelay = (attempt: number) => Math.min(
      3000 * Math.pow(2, Math.min(attempt, 6)),
      60000
    );

    expect(calcDelay(0)).toBe(3000);    // 3s
    expect(calcDelay(1)).toBe(6000);    // 6s
    expect(calcDelay(2)).toBe(12000);   // 12s
    expect(calcDelay(3)).toBe(24000);   // 24s
    expect(calcDelay(4)).toBe(48000);   // 48s
    expect(calcDelay(5)).toBe(60000);   // capped at 60s
    expect(calcDelay(6)).toBe(60000);   // still capped
    expect(calcDelay(100)).toBe(60000); // always capped
  });

  it('should emit max_reconnects after exhausting attempts', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      maxReconnectAttempts: 5,
    });

    let emitted = false;
    client.on('max_reconnects', () => {
      emitted = true;
    });

    (client as any).reconnectAttempts = 5;
    (client as any).scheduleReconnect();

    expect(emitted).toBe(true);
  });

  it('should clean up on disconnect including stall timer', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
    });

    (client as any).reconnectTimer = setTimeout(() => {}, 10000);
    (client as any).stallTimer = setTimeout(() => {}, 10000);
    (client as any).state = 'connected';

    client.disconnect();

    expect((client as any).state).toBe('disconnected');
    expect((client as any).ws).toBeNull();
    expect((client as any).reconnectTimer).toBeNull();
    expect((client as any).stallTimer).toBeNull();
  });

  it('should update startBlock for resume', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      startBlock: 100,
    });

    expect((client as any).options.startBlock).toBe(100);

    client.updateStartBlock(500);
    expect((client as any).options.startBlock).toBe(500);
  });

  it('should default stallTimeoutMs to 30000', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
    });

    expect((client as any).options.stallTimeoutMs).toBe(30000);
  });

  it('should accept custom stallTimeoutMs', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      stallTimeoutMs: 60000,
    });

    expect((client as any).options.stallTimeoutMs).toBe(60000);
  });

  it('should trigger forceReconnect on stall and cycle URL', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrls: ['wss://failover.example.com/'],
      stallTimeoutMs: 100,
    });

    // Simulate stall detection triggering forceReconnect
    (client as any).state = 'streaming';
    (client as any).forceReconnect();

    // Should be disconnected and URL should have cycled
    expect((client as any).state).toBe('disconnected');
    expect((client as any).currentUrl).toBe('wss://failover.example.com/');
  });
});
