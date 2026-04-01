import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { TelegramAlert } from '../../src/alerting/TelegramAlert.js';

describe('TelegramAlert', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('should report unconfigured when apiKey is empty', () => {
    const tg = new TelegramAlert('', new Map());
    expect(tg.isConfigured).toBe(false);
  });

  it('should report unconfigured when no chat IDs', () => {
    const tg = new TelegramAlert('some-key', new Map());
    expect(tg.isConfigured).toBe(false);
  });

  it('should report configured when apiKey and chat IDs are set', () => {
    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100222' }],
    ]);
    const tg = new TelegramAlert('some-key', chatIds);
    expect(tg.isConfigured).toBe(true);
  });

  it('should not send when unconfigured', async () => {
    const mockFetch = vi.fn();
    globalThis.fetch = mockFetch;

    const tg = new TelegramAlert('', new Map());
    await tg.send({
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Test',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should send to status channel for info severity', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    globalThis.fetch = mockFetch;

    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100222' }],
    ]);
    const tg = new TelegramAlert('bot-key', chatIds);

    await tg.send({
      severity: 'info',
      chain: 'libre',
      network: 'mainnet',
      title: 'Info msg',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain('bot-key');
    const body = JSON.parse(options.body);
    expect(body.chat_id).toBe('-100111');
    expect(body.parse_mode).toBe('HTML');
    expect(body.text).toContain('Info msg');
  });

  it('should send to both status and alert channels for alert severity', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    globalThis.fetch = mockFetch;

    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100222' }],
    ]);
    const tg = new TelegramAlert('bot-key', chatIds);

    await tg.send({
      severity: 'alert',
      routing: 'both',
      chain: 'libre',
      network: 'mainnet',
      title: 'Critical!',
      body: 'Something broke',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).toHaveBeenCalledTimes(2);
    const chatIdsSent = mockFetch.mock.calls.map(
      ([, opts]: [string, any]) => JSON.parse(opts.body).chat_id
    );
    expect(chatIdsSent).toContain('-100111');
    expect(chatIdsSent).toContain('-100222');
  });

  it('should not double-send if status and alert channels are the same', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    globalThis.fetch = mockFetch;

    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100111' }],
    ]);
    const tg = new TelegramAlert('bot-key', chatIds);

    await tg.send({
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Alert',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('should skip if chain has no chat IDs configured', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    globalThis.fetch = mockFetch;

    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100222' }],
    ]);
    const tg = new TelegramAlert('bot-key', chatIds);

    await tg.send({
      severity: 'alert',
      chain: 'libre',
      network: 'testnet', // not configured
      title: 'Alert',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should handle API errors gracefully', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 429,
      text: async () => 'Rate limited',
    });
    globalThis.fetch = mockFetch;

    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100222' }],
    ]);
    const tg = new TelegramAlert('bot-key', chatIds);

    // Should not throw
    await expect(
      tg.send({
        severity: 'info',
        chain: 'libre',
        network: 'mainnet',
        title: 'Test',
        body: 'Body',
        timestamp: '2026-03-30T00:00:00.000',
      })
    ).resolves.toBeUndefined();
  });

  it('should handle network errors gracefully', async () => {
    const mockFetch = vi.fn().mockRejectedValue(new Error('ECONNREFUSED'));
    globalThis.fetch = mockFetch;

    const chatIds = new Map([
      ['libre_mainnet', { statusChatId: '-100111', alertChatId: '-100222' }],
    ]);
    const tg = new TelegramAlert('bot-key', chatIds);

    await expect(
      tg.send({
        severity: 'info',
        chain: 'libre',
        network: 'mainnet',
        title: 'Test',
        body: 'Body',
        timestamp: '2026-03-30T00:00:00.000',
      })
    ).resolves.toBeUndefined();
  });
});
