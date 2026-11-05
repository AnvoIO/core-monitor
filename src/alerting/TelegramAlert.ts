import { logger } from '../utils/logger.js';
import type { AlertChannel, AlertMessage, AlertSendResult } from './types.js';
import { errorCause } from '../utils/errors.js';

const log = logger.child({ module: 'TelegramAlert' });

/** Waits before the 2nd and 3rd attempts at a send that failed transiently. */
const DEFAULT_RETRY_DELAYS_MS = [2_000, 5_000];
/** Longest wait honoured from a 429's retry_after. */
const MAX_RETRY_AFTER_MS = 30_000;
const REQUEST_TIMEOUT_MS = 10_000;

export interface TelegramAlertOptions {
  /** Delay before each retry; its length is the number of retries. */
  retryDelaysMs?: number[];
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

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
  private retryDelaysMs: number[];

  constructor(
    apiKey: string,
    chatIds: Map<string, { statusChatId: string; alertChatId: string }>,
    options: TelegramAlertOptions = {}
  ) {
    this.apiKey = apiKey;
    this.chatIds = chatIds;
    this.retryDelaysMs = options.retryDelaysMs ?? DEFAULT_RETRY_DELAYS_MS;
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

  /**
   * True if Telegram accepted the message. Network errors (DNS failures such as EAI_AGAIN,
   * resets, timeouts), 429 and 5xx responses are retried; other 4xx responses are not.
   */
  private async sendToChat(chatId: string, text: string): Promise<boolean> {
    for (let attempt = 0; ; attempt++) {
      const result = await this.attemptSend(chatId, text);
      if (result.ok) {
        if (attempt > 0) log.info({ chatId, attempts: attempt + 1 }, 'Telegram message sent after retry');
        return true;
      }
      if (!result.retryable || attempt >= this.retryDelaysMs.length) return false;
      const delay = result.retryAfterMs ?? this.retryDelaysMs[attempt];
      log.warn({ chatId, attempt: attempt + 1, delayMs: delay }, 'Retrying Telegram message');
      await sleep(delay);
    }
  }

  private async attemptSend(
    chatId: string,
    text: string
  ): Promise<{ ok: boolean; retryable: boolean; retryAfterMs?: number }> {
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
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });

      if (!response.ok) {
        const body = await response.text();
        log.error({ chatId, status: response.status, body }, 'Telegram API error');
        const retryable = response.status === 429 || response.status >= 500;
        return { ok: false, retryable, retryAfterMs: retryAfterMs(response.status, body) };
      }
      return { ok: true, retryable: false };
    } catch (err) {
      log.error({ err, cause: errorCause(err), chatId }, 'Failed to send Telegram message');
      return { ok: false, retryable: true };
    }
  }
}

/** The wait a 429 response asks for (parameters.retry_after, in seconds), capped. */
function retryAfterMs(status: number, body: string): number | undefined {
  if (status !== 429) return undefined;
  try {
    const seconds = (JSON.parse(body) as { parameters?: { retry_after?: unknown } }).parameters
      ?.retry_after;
    if (typeof seconds === 'number' && seconds >= 0) return Math.min(seconds * 1000, MAX_RETRY_AFTER_MS);
  } catch {
    // Not JSON: use the default delay.
  }
  return undefined;
}
