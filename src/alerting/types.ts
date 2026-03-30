export type AlertSeverity = 'info' | 'warn' | 'alert';

export interface AlertMessage {
  severity: AlertSeverity;
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
