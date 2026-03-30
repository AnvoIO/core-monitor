import { logger } from '../utils/logger.js';
import type { AlertChannel, AlertMessage } from './types.js';

const log = logger.child({ module: 'SlackAlert' });

const SEVERITY_EMOJI: Record<string, string> = {
  info: ':white_check_mark:',
  warn: ':warning:',
  alert: ':rotating_light:',
};

const SEVERITY_COLOR: Record<string, string> = {
  info: '#36a64f',
  warn: '#daa038',
  alert: '#cc0000',
};

export class SlackAlert implements AlertChannel {
  name = 'slack';
  private webhookUrl: string;

  constructor(webhookUrl: string) {
    this.webhookUrl = webhookUrl;
  }

  get isConfigured(): boolean {
    return this.webhookUrl.length > 0;
  }

  async send(message: AlertMessage): Promise<void> {
    if (!this.isConfigured) return;

    const emoji = SEVERITY_EMOJI[message.severity] || '';
    const color = SEVERITY_COLOR[message.severity] || '#cccccc';

    const payload = {
      attachments: [
        {
          color,
          title: `${emoji} ${message.title}`,
          text: message.body,
          footer: `${message.chain} ${message.network}`,
          ts: Math.floor(new Date(message.timestamp).getTime() / 1000),
        },
      ],
    };

    try {
      const response = await fetch(this.webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const body = await response.text();
        log.error({ status: response.status, body }, 'Slack webhook error');
      }
    } catch (err) {
      log.error({ err }, 'Failed to send Slack message');
    }
  }
}
