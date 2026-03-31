/**
 * Writer — SHiP block ingester, round evaluator, and alerting.
 *
 * Streams blocks from SHiP, evaluates rounds, writes to PostgreSQL,
 * and sends alerts via Telegram/Slack.
 */

import { loadConfig } from './config.js';
import { Database } from './store/Database.js';
import { Retention } from './store/Retention.js';
import { Summarizer } from './store/Summarizer.js';
import { ChainMonitor } from './monitor/ChainMonitor.js';
import { AlertManager } from './alerting/AlertManager.js';
import { TelegramAlert } from './alerting/TelegramAlert.js';
import { SlackAlert } from './alerting/SlackAlert.js';
import { logger } from './utils/logger.js';
import cron from 'node-cron';

const log = logger.child({ module: 'writer' });

async function main(): Promise<void> {
  log.info('AnvoIO Core Monitor — Writer starting');

  const config = loadConfig();
  log.info({ chains: config.chains.map((c) => c.id) }, 'Configuration loaded');

  const db = new Database(config.postgresUrl);
  await db.init();

  // Alerting
  const alertManager = new AlertManager();

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

  if (config.slack.enabled && config.slack.webhookUrl) {
    const slack = new SlackAlert(config.slack.webhookUrl);
    if (slack.isConfigured) {
      alertManager.addChannel(slack);
      log.info('Slack alerting enabled');
    }
  } else {
    log.info('Slack alerting disabled');
  }

  // Chain monitors
  const monitors: ChainMonitor[] = [];

  for (const chainConfig of config.chains) {
    const monitor = new ChainMonitor(chainConfig, db);

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

  // Retention purge — daily at 00:00 UTC
  cron.schedule('0 0 * * *', async () => {
    log.info('Running scheduled retention purge');
    const retention = new Retention(config.postgresUrl, config.retentionDays);
    await retention.runAll();
    await retention.close();
  });

  // Weekly summary — Sunday at 23:59 UTC
  cron.schedule('59 23 * * 0', async () => {
    log.info('Generating weekly summary');
    const summarizer = new Summarizer(config.postgresUrl);
    const now = new Date();
    const weekEnd = now.toISOString().split('T')[0];
    const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split('T')[0];
    await summarizer.generateWeeklySummary(weekStart, weekEnd);
    await summarizer.close();
  });

  // Monthly summary — last day of month at 23:59 UTC
  cron.schedule('59 23 28-31 * *', async () => {
    const now = new Date();
    const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    if (tomorrow.getDate() !== 1) return;

    log.info('Generating monthly summary');
    const summarizer = new Summarizer(config.postgresUrl);
    const monthEnd = now.toISOString().split('T')[0];
    const monthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
    await summarizer.generateMonthlySummary(monthStart, monthEnd);
    await summarizer.close();
  });

  // Graceful shutdown
  const shutdown = async () => {
    log.info('Writer shutting down');
    for (const monitor of monitors) {
      monitor.stop();
    }
    await db.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  log.info('AnvoIO Core Monitor — Writer running');
}

main().catch((err) => {
  const msg = err?.message?.replace(/postgresql:\/\/[^@]*@/g, 'postgresql://***@') || 'Unknown error';
  log.error({ err: msg }, 'Fatal error');
  process.exit(1);
});
