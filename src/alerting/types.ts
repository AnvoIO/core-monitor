export type AlertSeverity = 'info' | 'warn' | 'alert';
export type AlertRouting = 'status' | 'alert' | 'both';

export interface AlertMessage {
  severity: AlertSeverity;
  routing?: AlertRouting;
  chain: string;
  network: string;
  title: string;
  body: string;
  timestamp: string;
}

/** sent: delivered. failed: delivery attempted and failed. skipped: nothing to send to. */
export type AlertSendResult = 'sent' | 'failed' | 'skipped';

export interface AlertChannel {
  name: string;
  send(message: AlertMessage): Promise<AlertSendResult>;
}
