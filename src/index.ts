import { loadConfig } from './config.js';
import { Database } from './store/Database.js';
import { Retention } from './store/Retention.js';
import { Summarizer } from './store/Summarizer.js';
import { ChainMonitor } from './monitor/ChainMonitor.js';
import { AlertManager } from './alerting/AlertManager.js';
import { TelegramAlert } from './alerting/TelegramAlert.js';
import { SlackAlert } from './alerting/SlackAlert.js';
import { startServer } from './api/server.js';
import { logger } from './utils/logger.js';
import cron from 'node-cron';
import fs from 'fs';

const log = logger.child({ module: 'main' });

async function main(): Promise<void> {
  log.info('AnvoIO Core Monitor starting — Block Producer monitoring for Anvo Core and Legacy Antelope/EOSIO chains');

  // Load configuration
  const config = loadConfig();
  log.info({ chains: config.chains.map((c) => c.id) }, 'Configuration loaded');

  // Ensure data directory exists
  if (!fs.existsSync(config.dataDir)) {
    fs.mkdirSync(config.dataDir, { recursive: true });
  }

  // Initialize database
  const db = new Database(config.dataDir);

  // Initialize alerting
  const alertManager = new AlertManager();

  // Telegram
  if (config.telegram.enabled && config.telegram.apiKey) {
    const chatIds = new Map<string, { statusChatId: string; alertChatId: string }>();
    for (const chain of config.chains) {
      if (chain.telegram) {
        chatIds.set(chain.id, chain.telegram);
      }
    }
    const telegram = new TelegramAlert(config.telegram.apiKey, chatIds);
    if (telegram.isConfigured) {
      alertManager.addChannel(telegram);
      log.info('Telegram alerting enabled');
    }
  } else {
    log.info('Telegram alerting disabled');
  }

  // Slack
  if (config.slack.enabled && config.slack.webhookUrl) {
    const slack = new SlackAlert(config.slack.webhookUrl);
    if (slack.isConfigured) {
      alertManager.addChannel(slack);
      log.info('Slack alerting enabled');
    }
  } else {
    log.info('Slack alerting disabled');
  }

  // Start API server
  const server = await startServer(config, db);

  // Start chain monitors
  const monitors: ChainMonitor[] = [];

  for (const chainConfig of config.chains) {
    const monitor = new ChainMonitor(chainConfig, db);

    // Wire up alerting events
    monitor.on('missed_round', (params) => alertManager.missedRound(params));
    monitor.on('missed_blocks', (params) => alertManager.missedBlocks(params));
    monitor.on('round_complete', (params) => alertManager.roundComplete(params));
    monitor.on('schedule_change', (params) => alertManager.scheduleChange(params));
    monitor.on('producer_action', (params) => alertManager.producerAction(params));

    monitor.on('fatal', (err) => {
      log.error({ err, chain: chainConfig.id }, 'Chain monitor fatal error');
    });

    try {
      await monitor.start();
      monitors.push(monitor);
      log.info({ chain: chainConfig.id }, 'Chain monitor started');
    } catch (err) {
      log.error({ err, chain: chainConfig.id }, 'Failed to start chain monitor');
    }
  }

  // Schedule retention purge — daily at 00:00 UTC
  cron.schedule('0 0 * * *', () => {
    log.info('Running scheduled retention purge');
    const retention = new Retention(config.dataDir, config.retentionDays);
    retention.runAll();
    retention.close();
  });

  // Schedule weekly summary — Sunday at 23:59 UTC
  cron.schedule('59 23 * * 0', () => {
    log.info('Generating weekly summary');
    const summarizer = new Summarizer(config.dataDir);
    const now = new Date();
    const weekEnd = now.toISOString().split('T')[0];
    const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split('T')[0];
    summarizer.generateWeeklySummary(weekStart, weekEnd);
    summarizer.close();
  });

  // Schedule monthly summary — last day of month at 23:59 UTC
  cron.schedule('59 23 28-31 * *', () => {
    const now = new Date();
    const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    // Only run if tomorrow is the 1st (i.e., today is the last day of the month)
    if (tomorrow.getDate() !== 1) return;

    log.info('Generating monthly summary');
    const summarizer = new Summarizer(config.dataDir);
    const monthEnd = now.toISOString().split('T')[0];
    const monthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
    summarizer.generateMonthlySummary(monthStart, monthEnd);
    summarizer.close();
  });

  // Graceful shutdown
  const shutdown = async () => {
    log.info('Shutting down');
    for (const monitor of monitors) {
      monitor.stop();
    }
    await server.close();
    db.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  log.info('AnvoIO Core Monitor running');
}

main().catch((err) => {
  log.error({ err }, 'Fatal error');
  process.exit(1);
});
