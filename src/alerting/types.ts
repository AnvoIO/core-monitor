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

export interface AlertChannel {
  name: string;
  send(message: AlertMessage): Promise<void>;
}
