import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { SlackAlert } from '../../src/alerting/SlackAlert.js';

describe('SlackAlert', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('should report unconfigured when webhookUrl is empty', () => {
    const slack = new SlackAlert('');
    expect(slack.isConfigured).toBe(false);
  });

  it('should report configured when webhookUrl is set', () => {
    const slack = new SlackAlert('https://hooks.slack.com/services/xxx');
    expect(slack.isConfigured).toBe(true);
  });

  it('should not send when unconfigured', async () => {
    const mockFetch = vi.fn();
    globalThis.fetch = mockFetch;

    const slack = new SlackAlert('');
    await slack.send({
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'Test',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should send formatted message to webhook', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    globalThis.fetch = mockFetch;

    const slack = new SlackAlert('https://hooks.slack.com/services/xxx');
    await slack.send({
      severity: 'alert',
      chain: 'libre',
      network: 'mainnet',
      title: 'BP missed round',
      body: 'producer1 missed 12 blocks',
      timestamp: '2026-03-30T00:00:00.000',
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('https://hooks.slack.com/services/xxx');

    const body = JSON.parse(options.body);
    expect(body.attachments).toHaveLength(1);
    expect(body.attachments[0].title).toContain('BP missed round');
    expect(body.attachments[0].text).toContain('producer1');
    expect(body.attachments[0].color).toBe('#cc0000'); // alert color
    expect(body.attachments[0].footer).toContain('libre mainnet');
  });

  it('should use correct colors per severity', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    globalThis.fetch = mockFetch;

    const slack = new SlackAlert('https://hooks.slack.com/services/xxx');
    const base = {
      chain: 'libre',
      network: 'mainnet',
      title: 'Test',
      body: 'Body',
      timestamp: '2026-03-30T00:00:00.000',
    };

    await slack.send({ ...base, severity: 'info' });
    await slack.send({ ...base, severity: 'warn' });
    await slack.send({ ...base, severity: 'alert' });

    const colors = mockFetch.mock.calls.map(
      ([, opts]: [string, any]) => JSON.parse(opts.body).attachments[0].color
    );
    expect(colors).toEqual(['#36a64f', '#daa038', '#cc0000']);
  });

  it('should handle webhook errors gracefully', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'Internal Server Error',
    });
    globalThis.fetch = mockFetch;

    const slack = new SlackAlert('https://hooks.slack.com/services/xxx');

    await expect(
      slack.send({
        severity: 'alert',
        chain: 'libre',
        network: 'mainnet',
        title: 'Test',
        body: 'Body',
        timestamp: '2026-03-30T00:00:00.000',
      })
    ).resolves.toBeUndefined();
  });
});
