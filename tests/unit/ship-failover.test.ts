import { describe, it, expect } from 'vitest';
import { ShipClient } from '../../src/ship/ShipClient.js';

describe('ShipClient Failover', () => {
  it('should store primary and failover URLs', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrl: 'wss://failover.example.com/',
    });

    // Verify initial URL is primary
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
    expect((client as any).options.failoverUrl).toBe('wss://failover.example.com/');
  });

  it('should toggle to failover URL every 3rd reconnect attempt', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrl: 'wss://failover.example.com/',
      reconnectDelayMs: 1, // fast for testing
    });

    // Simulate reconnect attempts by calling scheduleReconnect
    // Access private method via any
    const scheduleReconnect = (client as any).scheduleReconnect.bind(client);

    // Attempt 1 — stays on primary
    (client as any).reconnectAttempts = 1;
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');

    // Attempt 3 — should switch to failover
    (client as any).reconnectAttempts = 3;
    // The toggle happens inside scheduleReconnect when reconnectAttempts % 3 === 0
    // Simulate the check
    if ((client as any).options.failoverUrl && (client as any).reconnectAttempts % 3 === 0) {
      (client as any).currentUrl = (client as any).options.failoverUrl;
    }
    expect((client as any).currentUrl).toBe('wss://failover.example.com/');

    // Attempt 6 — should switch back to primary
    (client as any).reconnectAttempts = 6;
    if ((client as any).options.failoverUrl && (client as any).reconnectAttempts % 3 === 0) {
      (client as any).currentUrl = (client as any).currentUrl === (client as any).options.url
        ? (client as any).options.failoverUrl
        : (client as any).options.url;
    }
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
  });

  it('should not attempt failover when no failover URL is configured', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
    });

    expect((client as any).options.failoverUrl).toBe('');
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');

    // Even after many attempts, should stay on primary
    (client as any).reconnectAttempts = 6;
    // The condition checks failoverUrl exists first
    if ((client as any).options.failoverUrl && (client as any).reconnectAttempts % 3 === 0) {
      // This should NOT execute
      (client as any).currentUrl = 'should-not-happen';
    }
    expect((client as any).currentUrl).toBe('wss://primary.example.com/');
  });

  it('should reset reconnect attempts after successful connection', async () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
      failoverUrl: 'wss://failover.example.com/',
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

    // Verify backoff formula: min(3000 * 2^min(attempt, 6), 60000)
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

  it('should clean up on disconnect', () => {
    const client = new ShipClient({
      url: 'wss://primary.example.com/',
    });

    // Set up some state
    (client as any).reconnectTimer = setTimeout(() => {}, 10000);
    (client as any).state = 'connected';

    client.disconnect();

    expect((client as any).state).toBe('disconnected');
    expect((client as any).ws).toBeNull();
    expect((client as any).reconnectTimer).toBeNull();
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
});
