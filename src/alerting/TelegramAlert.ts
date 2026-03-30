import { logger } from '../utils/logger.js';
import type { AlertChannel, AlertMessage } from './types.js';

const log = logger.child({ module: 'TelegramAlert' });

const SEVERITY_EMOJI: Record<string, string> = {
  info: '\u2705',    // green check
  warn: '\u26A0\uFE0F',  // warning
  alert: '\uD83D\uDEA8',  // alarm
};

export class TelegramAlert implements AlertChannel {
  name = 'telegram';
  private apiKey: string;
  private chatIds: Map<string, { statusChatId: string; alertChatId: string }>;

  constructor(
    apiKey: string,
    chatIds: Map<string, { statusChatId: string; alertChatId: string }>
  ) {
    this.apiKey = apiKey;
    this.chatIds = chatIds;
  }

  get isConfigured(): boolean {
    return this.apiKey.length > 0 && this.chatIds.size > 0;
  }

  async send(message: AlertMessage): Promise<void> {
    if (!this.isConfigured) return;

    const chainKey = `${message.chain}_${message.network}`;
    const ids = this.chatIds.get(chainKey);
    if (!ids) {
      log.warn({ chainKey }, 'No Telegram chat IDs configured for chain');
      return;
    }

    const emoji = SEVERITY_EMOJI[message.severity] || '';
    const text = `${emoji} <b>${message.title}</b>\n${message.body}`;

    // Always send to status channel
    await this.sendToChat(ids.statusChatId, text);

    // Critical alerts also go to alert channel
    if (message.severity === 'alert' && ids.alertChatId !== ids.statusChatId) {
      await this.sendToChat(ids.alertChatId, text);
    }
  }

  private async sendToChat(chatId: string, text: string): Promise<void> {
    const url = `https://api.telegram.org/bot${this.apiKey}/sendMessage`;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text,
          parse_mode: 'HTML',
          disable_web_page_preview: true,
        }),
      });

      if (!response.ok) {
        const body = await response.text();
        log.error({ chatId, status: response.status, body }, 'Telegram API error');
      }
    } catch (err) {
      log.error({ err, chatId }, 'Failed to send Telegram message');
    }
  }
}
