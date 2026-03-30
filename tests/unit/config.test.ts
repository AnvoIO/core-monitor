import { describe, it, expect, beforeEach, afterEach } from 'vitest';

// We test loadConfig by manipulating process.env directly
// since config.ts reads from process.env via dotenv

describe('Config', () => {
  const originalEnv = { ...process.env };

  function setMinimalEnv() {
    process.env.CHAINS = 'libre_mainnet';
    process.env.LIBRE_MAINNET_SHIP_URL = 'wss://ship.example.com/';
    process.env.LIBRE_MAINNET_API_URL = 'https://api.example.com';
    process.env.LIBRE_MAINNET_CHAIN_ID = 'abc123';
  }

  beforeEach(() => {
    // Clear all env vars that config reads
    delete process.env.CHAINS;
    delete process.env.LIBRE_MAINNET_SHIP_URL;
    delete process.env.LIBRE_MAINNET_SHIP_FAILOVER_URL;
    delete process.env.LIBRE_MAINNET_API_URL;
    delete process.env.LIBRE_MAINNET_CHAIN_ID;
    delete process.env.LIBRE_MAINNET_SCHEDULE_SIZE;
    delete process.env.LIBRE_MAINNET_BLOCKS_PER_BP;
    delete process.env.LIBRE_MAINNET_BLOCK_TIME_MS;
    delete process.env.LIBRE_TESTNET_SHIP_URL;
    delete process.env.LIBRE_TESTNET_API_URL;
    delete process.env.LIBRE_TESTNET_CHAIN_ID;
    delete process.env.TELEGRAM_API_KEY;
    delete process.env.TELEGRAM_LIBRE_MAINNET_STATUS_CHAT_ID;
    delete process.env.TELEGRAM_LIBRE_MAINNET_ALERT_CHAT_ID;
    delete process.env.SLACK_WEBHOOK_URL;
    delete process.env.API_PORT;
    delete process.env.API_HOST;
    delete process.env.RETENTION_DAYS;
    delete process.env.LOG_LEVEL;
    delete process.env.DATA_DIR;
  });

  afterEach(() => {
    // Restore original env
    for (const key of Object.keys(process.env)) {
      if (!(key in originalEnv)) {
        delete process.env[key];
      }
    }
    Object.assign(process.env, originalEnv);
  });

  it('should throw if CHAINS is missing', async () => {
    // Re-import to get fresh module
    const { loadConfig } = await import('../../src/config.js');
    expect(() => loadConfig()).toThrow('Missing required env var: CHAINS');
  });

  it('should throw if required chain vars are missing', async () => {
    process.env.CHAINS = 'libre_mainnet';
    const { loadConfig } = await import('../../src/config.js');
    expect(() => loadConfig()).toThrow('Missing required env var: LIBRE_MAINNET_SHIP_URL');
  });

  it('should parse a single chain config correctly', async () => {
    setMinimalEnv();
    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.chains).toHaveLength(1);
    expect(config.chains[0].id).toBe('libre_mainnet');
    expect(config.chains[0].chain).toBe('libre');
    expect(config.chains[0].network).toBe('mainnet');
    expect(config.chains[0].shipUrl).toBe('wss://ship.example.com/');
    expect(config.chains[0].apiUrl).toBe('https://api.example.com');
    expect(config.chains[0].chainId).toBe('abc123');
  });

  it('should use defaults for optional chain fields', async () => {
    setMinimalEnv();
    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.chains[0].scheduleSize).toBe(21);
    expect(config.chains[0].blocksPerBp).toBe(12);
    expect(config.chains[0].blockTimeMs).toBe(500);
    expect(config.chains[0].shipFailoverUrl).toBeUndefined();
    expect(config.chains[0].telegram).toBeUndefined();
  });

  it('should override defaults when env vars are set', async () => {
    setMinimalEnv();
    process.env.LIBRE_MAINNET_SCHEDULE_SIZE = '42';
    process.env.LIBRE_MAINNET_BLOCKS_PER_BP = '6';
    process.env.LIBRE_MAINNET_BLOCK_TIME_MS = '250';
    process.env.LIBRE_MAINNET_SHIP_FAILOVER_URL = 'wss://failover.example.com/';

    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.chains[0].scheduleSize).toBe(42);
    expect(config.chains[0].blocksPerBp).toBe(6);
    expect(config.chains[0].blockTimeMs).toBe(250);
    expect(config.chains[0].shipFailoverUrl).toBe('wss://failover.example.com/');
  });

  it('should parse Telegram config when chat IDs are set', async () => {
    setMinimalEnv();
    process.env.TELEGRAM_LIBRE_MAINNET_STATUS_CHAT_ID = '-100111';
    process.env.TELEGRAM_LIBRE_MAINNET_ALERT_CHAT_ID = '-100222';

    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.chains[0].telegram).toBeDefined();
    expect(config.chains[0].telegram!.statusChatId).toBe('-100111');
    expect(config.chains[0].telegram!.alertChatId).toBe('-100222');
  });

  it('should parse multiple chains', async () => {
    setMinimalEnv();
    process.env.CHAINS = 'libre_mainnet,libre_testnet';
    process.env.LIBRE_TESTNET_SHIP_URL = 'wss://testnet-ship.example.com/';
    process.env.LIBRE_TESTNET_API_URL = 'https://testnet-api.example.com';
    process.env.LIBRE_TESTNET_CHAIN_ID = 'def456';

    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.chains).toHaveLength(2);
    expect(config.chains[0].id).toBe('libre_mainnet');
    expect(config.chains[1].id).toBe('libre_testnet');
    expect(config.chains[1].chain).toBe('libre');
    expect(config.chains[1].network).toBe('testnet');
  });

  it('should use defaults for app-level config', async () => {
    setMinimalEnv();
    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.api.port).toBe(3000);
    expect(config.api.host).toBe('0.0.0.0');
    expect(config.retentionDays).toBe(548);
    expect(config.logLevel).toBe('info');
    expect(config.dataDir).toBe('./data');
  });

  it('should override app-level defaults from env', async () => {
    setMinimalEnv();
    process.env.API_PORT = '8080';
    process.env.API_HOST = '127.0.0.1';
    process.env.RETENTION_DAYS = '365';
    process.env.LOG_LEVEL = 'debug';
    process.env.DATA_DIR = '/custom/data';

    const { loadConfig } = await import('../../src/config.js');
    const config = loadConfig();

    expect(config.api.port).toBe(8080);
    expect(config.api.host).toBe('127.0.0.1');
    expect(config.retentionDays).toBe(365);
    expect(config.logLevel).toBe('debug');
    expect(config.dataDir).toBe('/custom/data');
  });
});
