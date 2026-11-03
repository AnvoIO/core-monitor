import { logger } from '../utils/logger.js';
import type { AlertChannel, AlertMessage, AlertSendResult } from './types.js';
import { errorCause } from '../utils/errors.js';

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
  /** Chains already warned about for missing chat IDs (warn once, not on every alert). */
  private warnedMissing = new Set<string>();

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

  async send(message: AlertMessage): Promise<AlertSendResult> {
    if (!this.isConfigured) return 'skipped';

    const chainKey = `${message.chain}_${message.network}`;
    const ids = this.chatIds.get(chainKey);
    if (!ids) {
      if (!this.warnedMissing.has(chainKey)) {
        this.warnedMissing.add(chainKey);
        log.warn({ chainKey }, 'No Telegram chat IDs configured for chain; its alerts are not sent');
      }
      return 'skipped';
    }

    const emoji = SEVERITY_EMOJI[message.severity] || '';
    const indentedBody = message.body.split('\n').map(line => ` ${line}`).join('\n');
    const text = `${emoji} <b>${message.title}</b>\n${indentedBody}`;

    const routing = message.routing || 'status';

    let ok = true;
    if (routing === 'status' || routing === 'both') {
      ok = (await this.sendToChat(ids.statusChatId, text)) && ok;
    }
    if ((routing === 'alert' || routing === 'both') && ids.alertChatId !== ids.statusChatId) {
      ok = (await this.sendToChat(ids.alertChatId, text)) && ok;
    }
    return ok ? 'sent' : 'failed';
  }

  /** True if Telegram accepted the message. */
  private async sendToChat(chatId: string, text: string): Promise<boolean> {
    // Note: Telegram Bot API requires the token in the URL path — this is by design.
    // Ensure this URL is never logged.
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
        return false;
      }
      return true;
    } catch (err) {
      log.error({ err, cause: errorCause(err), chatId }, 'Failed to send Telegram message');
      return false;
    }
  }
}
