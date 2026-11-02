import dotenv from 'dotenv';
import { logger } from './utils/logger.js';
import type { ShipAccessCredentials } from './ship/types.js';

dotenv.config();

export interface ChainConfig {
  id: string;
  chain: string;
  network: string;
  shipUrl: string;
  shipFailoverUrls: string[];
  shipAccess?: ShipAccessCredentials;
  apiUrl: string;
  hyperionUrl?: string;
  chainId: string;
  scheduleSize: number;
  blocksPerBp: number;
  blockTimeMs: number;
  telegram?: {
    statusChatId: string;
    alertChatId: string;
  };
}

export interface AppConfig {
  chains: ChainConfig[];
  telegram: {
    apiKey: string;
    enabled: boolean;
  };
  slack: {
    webhookUrl: string;
    enabled: boolean;
  };
  api: {
    port: number;
    host: string;
    corsOrigin: string;
  };
  postgresUrl: string;
  retentionDays: number;
  logLevel: string;
  dataDir: string;
}

function requireEnv(key: string): string {
  const val = process.env[key];
  if (!val) throw new Error(`Missing required env var: ${key}`);
  return val;
}

function optionalEnv(key: string, fallback: string = ''): string {
  return process.env[key] || fallback;
}

function parseShipAccess(): ShipAccessCredentials | undefined {
  const clientId = optionalEnv('SHIP_CF_ACCESS_CLIENT_ID').trim();
  const clientSecret = optionalEnv('SHIP_CF_ACCESS_CLIENT_SECRET').trim();
  if (clientId && clientSecret) return { clientId, clientSecret };
  if (clientId || clientSecret) {
    logger.warn(
      'Only one of SHIP_CF_ACCESS_CLIENT_ID / SHIP_CF_ACCESS_CLIENT_SECRET is set; sending no Access headers'
    );
  }
  return undefined;
}

function parseChainConfig(id: string): ChainConfig {
  const prefix = id.toUpperCase().replace(/[^A-Z0-9]/g, '_');
  const [chain, network] = id.split('_');

  const shipFailoverUrls = [
    optionalEnv(`${prefix}_SHIP_FAILOVER_URL`),
    optionalEnv(`${prefix}_CATCHUP_SHIP_URL`),
  ].filter(Boolean);

  return {
    id,
    chain,
    network,
    shipUrl: requireEnv(`${prefix}_SHIP_URL`),
    shipFailoverUrls,
    shipAccess: parseShipAccess(),
    apiUrl: requireEnv(`${prefix}_API_URL`),
    hyperionUrl: optionalEnv(`${prefix}_HYPERION_URL`) || undefined,
    chainId: requireEnv(`${prefix}_CHAIN_ID`),
    scheduleSize: parseInt(optionalEnv(`${prefix}_SCHEDULE_SIZE`, '21'), 10),
    blocksPerBp: parseInt(optionalEnv(`${prefix}_BLOCKS_PER_BP`, '12'), 10),
    blockTimeMs: parseInt(optionalEnv(`${prefix}_BLOCK_TIME_MS`, '500'), 10),
    telegram: optionalEnv(`TELEGRAM_${prefix}_STATUS_CHAT_ID`)
      ? {
          statusChatId: requireEnv(`TELEGRAM_${prefix}_STATUS_CHAT_ID`),
          alertChatId: requireEnv(`TELEGRAM_${prefix}_ALERT_CHAT_ID`),
        }
      : undefined,
  };
}

export function loadConfig(): AppConfig {
  const chainIds = requireEnv('CHAINS').split(',').map(s => s.trim());

  return {
    chains: chainIds.map(parseChainConfig),
    telegram: {
      apiKey: optionalEnv('TELEGRAM_API_KEY'),
      enabled: optionalEnv('TELEGRAM_ENABLED', 'false').toLowerCase() === 'true',
    },
    slack: {
      webhookUrl: optionalEnv('SLACK_WEBHOOK_URL'),
      enabled: optionalEnv('SLACK_ENABLED', 'false').toLowerCase() === 'true',
    },
    api: {
      port: parseInt(optionalEnv('API_PORT', '3000'), 10),
      host: optionalEnv('API_HOST', '0.0.0.0'),
      corsOrigin: optionalEnv('API_CORS_ORIGIN', '*'),
    },
    postgresUrl: requireEnv('POSTGRES_URL'),
    retentionDays: parseInt(optionalEnv('RETENTION_DAYS', '548'), 10),
    logLevel: optionalEnv('LOG_LEVEL', 'info'),
    dataDir: optionalEnv('DATA_DIR', './data'),
  };
}
