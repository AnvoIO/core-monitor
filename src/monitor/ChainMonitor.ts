import { EventEmitter } from 'events';
import { Name, PublicKey, Serializer } from '@wharfkit/antelope';
import { ShipClient } from '../ship/ShipClient.js';
import { ScheduleTracker } from './ScheduleTracker.js';
import { RoundEvaluator, type RoundResult, type BlockRecord } from './RoundEvaluator.js';
import { Database } from '../store/Database.js';
import { chainLogger } from '../utils/logger.js';
import type { ChainConfig } from '../config.js';
import type { ShipResult, ShipGetStatusResult } from '../ship/types.js';
import * as metrics from '../metrics/metrics.js';
import {
  SavannaProposerTracker,
  type ProposerPolicy,
  type SavannaTrackerState,
} from './SavannaProposerTracker.js';

/** monitor_state key holding the Savanna tracker snapshot. */
const SAVANNA_STATE_KEY = 'savanna_tracker';
/** Persist the Savanna tracker at least this often (blocks), besides every policy event. */
const SAVANNA_PERSIST_INTERVAL = 120;

export class ChainMonitor extends EventEmitter {
  private config: ChainConfig;
  private db: Database;
  private ship!: ShipClient;
  private schedule: ScheduleTracker;
  private roundEvaluator: RoundEvaluator;
  private log: ReturnType<typeof chainLogger>;
  private blocksProcessed: number = 0;
  private lastLogTime: number = 0;
  private lastBlockId: string = '';
  private lastBlockProducer: string = '';
  private pendingSchedule: { version: number; producers: Array<{ producer_name: string; block_signing_key: string }> } | null = null;
  private processingBlock: Promise<void> = Promise.resolve();
  private running = false;
  /** Producer schedule tracker for Savanna chains (created on the first Savanna block). */
  private savanna: SavannaProposerTracker | null = null;

  constructor(config: ChainConfig, db: Database) {
    super();
    this.config = config;
    this.db = db;
    this.log = chainLogger(config.chain, config.network);
    this.schedule = new ScheduleTracker(config.chain, config.network, db);
    this.roundEvaluator = new RoundEvaluator(config, db, this.schedule);
  }

  async start(): Promise<void> {
    this.log.info({ shipUrl: this.config.shipUrl }, 'Starting chain monitor');

    // Initialize components (async state restore)
    await this.schedule.init();
    await this.roundEvaluator.init();

    // Bootstrap schedule if needed
    if (!this.schedule.schedule) {
      await this.fetchSchedule();
    }

    // Determine start block
    const lastBlock = await this.db.getState(this.config.chain, this.config.network, 'last_block');
    const startBlock = lastBlock ? parseInt(lastBlock, 10) + 1 : 0;

    const { chain, network } = this.config;
    metrics.initChainSeries(chain, network);
    this.initProducerMetrics();

    this.ship = new ShipClient({
      url: this.config.shipUrl,
      failoverUrls: this.config.shipFailoverUrls,
      access: this.config.shipAccess,
      blockTimestampEpochMs: this.config.blockTimestampEpochMs,
      startBlock,
      fetchBlock: true,
      fetchTraces: true,
      fetchDeltas: true,
    });

    this.ship.on('status', (status: ShipGetStatusResult) => {
      this.log.info(
        { head: status.head.block_num, chainId: status.chain_id },
        'Connected to chain'
      );
      if (status.chain_id !== this.config.chainId) {
        this.log.error(
          { expected: this.config.chainId, got: status.chain_id },
          'Chain ID mismatch — wrong endpoint?'
        );
        this.ship.disconnect();
      }
    });

    this.ship.on('block', (result: ShipResult) => {
      // Serialize block processing — wait for previous block to finish
      // before starting the next to prevent duplicate round evaluations
      this.processingBlock = this.processingBlock.then(() =>
        this.processBlock(result)
      ).catch(err => {
        this.log.error({ err }, 'Error processing block');
      });
    });

    this.ship.on('disconnect', () => metrics.shipDisconnects.inc({ chain, network }));
    this.ship.on('reconnect_attempt', () => metrics.shipReconnectAttempts.inc({ chain, network }));
    this.ship.on('endpoint_change', ({ from, to }: { from: number; to: number }) => {
      if (from === 0 && to > 0) metrics.shipFailovers.inc({ chain, network });
    });
    metrics.registerChainSource(chain, network, () => {
      const state = this.ship.connectionState;
      return {
        running: this.running,
        connected: state === 'connected' || state === 'streaming',
        endpointIndex: this.ship.endpointIndex,
        url: this.ship.endpointUrl,
      };
    });

    this.ship.on('max_reconnects', () => {
      this.running = false;
      this.log.error('Max reconnection attempts reached, monitor stopping');
      this.emit('fatal', new Error('Max reconnection attempts reached'));
    });

    this.running = true;
    await this.ship.connect();
  }

  /** Create this schedule's producer counter series at 0, so their first increment is visible. */
  private initProducerMetrics(): void {
    metrics.initProducerSeries(this.config.chain, this.config.network, this.schedule.producers);
  }

  private async fetchSchedule(): Promise<void> {
    try {
      this.log.info('Fetching producer schedule via RPC');
      const response = await fetch(`${this.config.apiUrl}/v1/chain/get_producer_schedule`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });

      if (!response.ok) {
        this.log.warn({ status: response.status }, 'Failed to fetch schedule via RPC');
        return;
      }

      const data = await response.json() as any;
      const active = data.active;

      if (active && active.producers && active.producers.length > 0) {
        const version = active.version;
        const producers = active.producers.map((p: any) => ({
          producer_name: p.producer_name,
          block_signing_key: p.block_signing_key || p.authority?.[1]?.keys?.[0]?.key || 'unknown',
        }));

        let activationTimestamp: string | null = null;
        if (this.config.hyperionUrl) {
          activationTimestamp = await this.fetchScheduleActivation(version);
        }

        const timestamp = activationTimestamp || new Date().toISOString();

        await this.schedule.updateSchedule(version, producers, 0, timestamp);

        if (activationTimestamp) {
          await this.roundEvaluator.setScheduleActivation(activationTimestamp);
          this.log.info(
            { version, producers: producers.length, activationTimestamp },
            'Schedule bootstrapped from RPC + Hyperion'
          );
        } else {
          this.log.info(
            { version, producers: producers.length },
            'Schedule bootstrapped from RPC (no activation timestamp)'
          );
        }
      }
    } catch (err) {
      this.log.warn({ err }, 'Failed to bootstrap schedule — will detect from SHiP blocks');
    }
  }

  private async fetchScheduleActivation(version: number): Promise<string | null> {
    try {
      const url = `${this.config.hyperionUrl}/history/get_schedule?version=${version}`;
      this.log.info({ url }, 'Fetching schedule activation from Hyperion');

      const response = await fetch(url);
      if (!response.ok) {
        this.log.warn({ status: response.status }, 'Hyperion schedule lookup failed');
        return null;
      }

      const data = await response.json() as any;
      if (data.timestamp) {
        this.log.info(
          { version, activatedAt: data.timestamp },
          'Schedule activation timestamp from Hyperion'
        );
        return data.timestamp;
      }
    } catch (err) {
      this.log.warn({ err }, 'Failed to fetch schedule activation from Hyperion');
    }
    return null;
  }

  stop(): void {
    this.log.info('Stopping chain monitor');
    this.running = false;
    if (this.ship) this.ship.disconnect();
  }

  private scheduleVerified: boolean = false;

  private async processBlock(result: ShipResult): Promise<void> {
    if (!result.this_block || !result.block) return;

    const blockNum = result.this_block.block_num;
    const blockId = result.this_block.block_id;
    const block = result.block;

    // On first block, verify our schedule matches the chain's active schedule.
    // If we missed transitions during downtime, re-bootstrap from RPC.
    if (!this.scheduleVerified && !block.savanna) {
      this.scheduleVerified = true;
      if (block.schedule_version > this.schedule.version) {
        this.log.warn(
          { stored: this.schedule.version, chain: block.schedule_version },
          'Schedule version mismatch on startup — re-bootstrapping from RPC'
        );
        await this.fetchSchedule();
        // Clear stale pending schedule that may reference the now-active version
        this.pendingSchedule = null;
        await this.db.setState(this.config.chain, this.config.network, 'pending_schedule', '');
        this.initProducerMetrics();
      }
    }

    // Fork detection
    if (this.lastBlockId && result.prev_block) {
      const prevBlockId = result.prev_block.block_id;
      if (prevBlockId !== this.lastBlockId) {
        const forkBlockNum = result.prev_block.block_num;
        this.log.warn(
          {
            forkAt: forkBlockNum,
            expectedPrev: this.lastBlockId.substring(0, 16),
            actualPrev: prevBlockId.substring(0, 16),
            currentBlock: blockNum,
            originalProducer: this.lastBlockProducer,
            newProducer: block.producer,
          },
          'Fork detected — block replaced'
        );

        await this.db.insertForkEvent({
          chain: this.config.chain,
          network: this.config.network,
          round_id: null,
          block_number: forkBlockNum,
          original_producer: this.lastBlockProducer,
          replacement_producer: block.producer,
          timestamp: block.timestamp,
        });

        this.roundEvaluator.recordFork(forkBlockNum, this.lastBlockProducer, block.producer);
        metrics.forks.inc({ chain: this.config.chain, network: this.config.network });

        this.emit('fork', {
          chain: this.config.chain,
          network: this.config.network,
          blockNumber: forkBlockNum,
          originalProducer: this.lastBlockProducer,
          replacementProducer: block.producer,
          timestamp: block.timestamp,
        });
      }
    }

    this.lastBlockId = blockId;
    this.lastBlockProducer = block.producer;

    if (block.savanna) {
      await this.processSavannaSchedule(block, blockNum);
    }

    // Schedule change detection
    // new_producers from block header (legacy) or header extensions (WTMSIG)
    // represents a PROPOSED schedule, not yet active. Store it as pending.
    if (!block.savanna && block.new_producers && block.new_producers.version > this.schedule.version) {
      this.pendingSchedule = block.new_producers;
      const pendingProducers = block.new_producers.producers.map((p) => p.producer_name);
      const currentProducers = this.schedule.producers;
      const currentKeys = this.schedule.producerKeys;
      const added = pendingProducers.filter((p: string) => !currentProducers.includes(p));
      const removed = currentProducers.filter(p => !pendingProducers.includes(p));

      // Detect key-only changes: same producer, different signing key
      // Normalize to legacy format so RPC (EOS...) and SHiP (PUB_K1_...) keys compare equal
      const keyUpdates: string[] = [];
      for (const p of block.new_producers.producers) {
        const oldKey = currentKeys[p.producer_name];
        if (oldKey && !added.includes(p.producer_name)) {
          let newKey: string;
          try { newKey = PublicKey.from(p.block_signing_key).toLegacyString(); } catch { newKey = p.block_signing_key; }
          let normOld: string;
          try { normOld = PublicKey.from(oldKey).toLegacyString(); } catch { normOld = oldKey; }
          if (normOld !== newKey) {
            keyUpdates.push(p.producer_name);
          }
        }
      }

      await this.db.setState(this.config.chain, this.config.network, 'pending_schedule', JSON.stringify({
        version: block.new_producers.version,
        producers: pendingProducers,
        added,
        removed,
        keyUpdates,
        blockNum,
      }));

      this.log.info(
        { version: block.new_producers.version, blockNum, added, removed, keyUpdates: keyUpdates.length > 0 ? keyUpdates : undefined },
        'Pending schedule detected (not yet active)'
      );
    }

    // The schedule_version in the block header tells us which schedule is
    // actually ACTIVE. When it increments, apply the pending schedule.
    if (!block.savanna && block.schedule_version > this.schedule.version && this.pendingSchedule) {
      const result = await this.schedule.updateSchedule(
        this.pendingSchedule.version,
        this.pendingSchedule.producers,
        blockNum,
        block.timestamp
      );
      if (result) {
        await this.roundEvaluator.setScheduleActivation(block.timestamp);
        this.initProducerMetrics();

        this.emit('schedule_change', {
          chain: this.config.chain,
          network: this.config.network,
          version: this.pendingSchedule.version,
          producers: this.pendingSchedule.producers.map((p) => p.producer_name),
          added: result.added,
          removed: result.removed,
          keyUpdates: result.keyUpdates,
          blockNum,
          timestamp: block.timestamp,
        });
      }
      this.pendingSchedule = null;
      await this.db.setState(this.config.chain, this.config.network, 'pending_schedule', '');
    }

    // Producer registration events
    const sys = this.config.systemAccount;
    for (const trace of result.traces) {
      for (const actionTrace of trace.action_traces) {
        const account = String(actionTrace.act.account);
        const name = String(actionTrace.act.name);
        if (account === sys && (name === 'regproducer' || name === 'unregprod' || name === 'kickbp')) {
          let producerName = '';

          if (name === 'kickbp') {
            // kickbp action data is a single name (producer to kick)
            // Decode from raw bytes: the data is an 8-byte encoded Antelope name
            try {
              const rawData = actionTrace.act.data;
              const dataBytes = rawData?.array ?? rawData;
              if (dataBytes && dataBytes.length >= 8) {
                const decoded = Name.from(Serializer.decode({ data: dataBytes, type: 'name' }));
                producerName = String(decoded);
              }
            } catch {
              // Fallback: check inline actions for unregprod
              for (const inlineTrace of trace.action_traces) {
                if (String(inlineTrace.act.account) === sys && String(inlineTrace.act.name) === 'unregprod') {
                  producerName = String(inlineTrace.act.authorization?.[0]?.actor || '');
                  break;
                }
              }
            }
          } else {
            producerName = String(actionTrace.act.authorization?.[0]?.actor || '')
              || String(actionTrace.act.data?.producer || '');
          }

          if (!producerName || producerName === sys) continue;

          await this.db.insertProducerEvent({
            chain: this.config.chain,
            network: this.config.network,
            producer: producerName,
            action: name,
            block_number: blockNum,
            timestamp: block.timestamp,
          });

          this.emit('producer_action', {
            chain: this.config.chain,
            network: this.config.network,
            action: name,
            producer: producerName,
            data: actionTrace.act.data,
            blockNum,
            timestamp: block.timestamp,
          });

          // kickbp implies unregprod — log both events
          if (name === 'kickbp') {
            await this.db.insertProducerEvent({
              chain: this.config.chain,
              network: this.config.network,
              producer: producerName,
              action: 'unregprod',
              block_number: blockNum,
              timestamp: block.timestamp,
            });

            this.emit('producer_action', {
              chain: this.config.chain,
              network: this.config.network,
              action: 'unregprod',
              producer: producerName,
              data: actionTrace.act.data,
              blockNum,
              timestamp: block.timestamp,
            });
          }
        }
      }
    }

    // Feed to round evaluator
    const blockRecord: BlockRecord = {
      block_num: blockNum,
      producer: block.producer,
      timestamp: block.timestamp,
      schedule_version: this.savanna ? this.savanna.active.version : block.schedule_version,
    };

    const roundResult = await this.roundEvaluator.processBlock(blockRecord);

    if (roundResult) {
      this.emitRoundResult(roundResult);
    }

    this.updatePositionMetrics(result, blockNum, block.timestamp);

    // Progress logging
    this.blocksProcessed++;
    const now = Date.now();
    if (now - this.lastLogTime >= 10000) {
      const headBlock = result.head.block_num;
      const behind = headBlock - blockNum;
      this.log.info(
        {
          blockNum,
          headBlock,
          behind,
          producer: block.producer,
          round: this.roundEvaluator.round,
          scheduleVersion: block.schedule_version,
          blocksProcessed: this.blocksProcessed,
        },
        behind > 10 ? 'Catching up' : 'Streaming'
      );
      this.lastLogTime = now;
      this.blocksProcessed = 0;
    }
  }

  /**
   * Savanna chains: follow proposer policies from block headers. A proposal is stored as the
   * pending schedule; an activation is applied like a legacy schedule_version change.
   */
  private async processSavannaSchedule(block: NonNullable<ShipResult['block']>, blockNum: number): Promise<void> {
    if (!this.savanna) {
      this.savanna = await this.loadSavannaTracker();
      // No activation time for the starting schedule (there's no Hyperion lookup for Savanna):
      // count rounds from here instead of from slot 0.
      if (!this.roundEvaluator.hasScheduleActivation) {
        await this.roundEvaluator.setScheduleActivation(block.timestamp);
      }
    }
    const tracker = this.savanna;
    const { chain, network } = this.config;

    const step = tracker.next({ blockNum, ...block.savanna! });

    if (step.proposed) {
      const names = step.proposed.producers.map((p) => p.producer_name);
      const current = this.schedule.producers;
      const added = names.filter((p) => !current.includes(p));
      const removed = current.filter((p) => !names.includes(p));
      await this.db.setState(chain, network, 'pending_schedule', JSON.stringify({
        version: step.proposed.version,
        producers: names,
        added,
        removed,
        keyUpdates: [],
        blockNum,
      }));
      this.log.info({ version: step.proposed.version, blockNum, added, removed }, 'Pending schedule detected (not yet active)');
    }

    if (step.activated) {
      const policy = step.activated;
      const result = await this.schedule.updateSchedule(policy.version, policy.producers, blockNum, block.timestamp);
      if (result) {
        await this.roundEvaluator.setScheduleActivation(block.timestamp);
        this.initProducerMetrics();
        this.emit('schedule_change', {
          chain,
          network,
          version: policy.version,
          producers: policy.producers.map((p) => p.producer_name),
          added: result.added,
          removed: result.removed,
          keyUpdates: result.keyUpdates,
          blockNum,
          timestamp: block.timestamp,
        });
      }
      if (!tracker.pending && !tracker.proposed) {
        await this.db.setState(chain, network, 'pending_schedule', '');
      }
    }

    if (step.proposed || step.activated || blockNum % SAVANNA_PERSIST_INTERVAL === 0) {
      await this.db.setState(chain, network, SAVANNA_STATE_KEY, JSON.stringify(tracker.snapshot()));
    }
  }

  /** Restore the Savanna tracker from state, else start it from the chain's current schedule (RPC). */
  private async loadSavannaTracker(): Promise<SavannaProposerTracker> {
    const saved = await this.db.getState(this.config.chain, this.config.network, SAVANNA_STATE_KEY);
    if (saved) {
      try {
        const state = JSON.parse(saved) as SavannaTrackerState;
        this.log.info({ active: state.active.version, pending: state.pending?.version, proposed: state.proposed?.version }, 'Restored Savanna schedule tracker');
        return new SavannaProposerTracker(state);
      } catch (err) {
        this.log.warn({ err }, 'Saved Savanna tracker unreadable — bootstrapping from RPC');
      }
    }

    const toPolicy = (s: any): ProposerPolicy | null =>
      s && Array.isArray(s.producers) && s.producers.length > 0
        ? {
            version: Number(s.version),
            proposalSlot: 0,
            producers: s.producers.map((p: any) => ({
              producer_name: String(p.producer_name),
              block_signing_key: String(p.block_signing_key || p.authority?.[1]?.keys?.[0]?.key || 'unknown'),
            })),
          }
        : null;

    try {
      const response = await fetch(`${this.config.apiUrl}/v1/chain/get_producer_schedule`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      if (response.ok) {
        const data = (await response.json()) as any;
        const active = toPolicy(data.active);
        if (active) {
          this.log.info({ active: active.version, pending: data.pending?.version, proposed: data.proposed?.version }, 'Savanna schedule tracker bootstrapped from RPC');
          return SavannaProposerTracker.fromPolicies(active, toPolicy(data.pending), toPolicy(data.proposed));
        }
      }
      this.log.warn({ status: response.status }, 'Savanna schedule bootstrap from RPC failed — using stored schedule');
    } catch (err) {
      this.log.warn({ err }, 'Savanna schedule bootstrap from RPC failed — using stored schedule');
    }

    const keys = this.schedule.producerKeys;
    return SavannaProposerTracker.fromPolicies({
      version: this.schedule.version,
      proposalSlot: 0,
      producers: this.schedule.producers.map((name) => ({ producer_name: name, block_signing_key: keys[name] ?? 'unknown' })),
    });
  }

  private updatePositionMetrics(result: ShipResult, blockNum: number, timestamp: string): void {
    const labels = { chain: this.config.chain, network: this.config.network };
    metrics.headBlock.set(labels, result.head.block_num);
    metrics.processedBlock.set(labels, blockNum);
    metrics.blocksBehind.set(labels, Math.max(0, result.head.block_num - blockNum));
    if (result.last_irreversible) metrics.libBlock.set(labels, result.last_irreversible.block_num);
    const seconds = metrics.blockTimeSeconds(timestamp);
    if (seconds !== null) metrics.lastBlockTimestamp.set(labels, seconds);
  }

  private recordRoundMetrics(result: RoundResult): void {
    const { chain, network } = this.config;
    metrics.roundsEvaluated.inc({ chain, network });
    for (const p of result.producerResults) {
      const labels = { chain, network, producer: p.producer };
      if (p.blocksProduced > 0) metrics.blocksProduced.inc(labels, p.blocksProduced);
      if (p.blocksMissed > 0) metrics.blocksMissed.inc(labels, p.blocksMissed);
      if (p.blocksProduced === 0 && p.blocksExpected > 0) metrics.roundsMissed.inc(labels);
    }
  }

  private emitRoundResult(result: RoundResult): void {
    this.recordRoundMetrics(result);
    const missed = result.producerResults.filter((p) => p.blocksProduced === 0);
    const partial = result.producerResults.filter(
      (p) => p.blocksProduced > 0 && p.blocksMissed > 0
    );

    if (missed.length > 0) {
      for (const m of missed) {
        this.emit('missed_round', {
          chain: this.config.chain,
          network: this.config.network,
          producer: m.producer,
          round: result.roundNumber,
          scheduleVersion: result.scheduleVersion,
          blocksMissed: m.blocksExpected,
          timestamp: result.timestampEnd,
        });
      }
    }

    if (partial.length > 0) {
      for (const p of partial) {
        this.emit('missed_blocks', {
          chain: this.config.chain,
          network: this.config.network,
          producer: p.producer,
          round: result.roundNumber,
          scheduleVersion: result.scheduleVersion,
          blocksProduced: p.blocksProduced,
          blocksMissed: p.blocksMissed,
          blocksExpected: p.blocksExpected,
          timestamp: result.timestampEnd,
        });
      }
    }

    this.emit('round_complete', {
      chain: this.config.chain,
      network: this.config.network,
      round: result.roundNumber,
      producersProduced: result.producersProduced,
      producersMissed: result.producersMissed,
      partialProducers: partial.map(p => ({ producer: p.producer, produced: p.blocksProduced, missed: p.blocksMissed, expected: p.blocksExpected })),
      missedProducers: missed.map(m => ({ producer: m.producer, expected: m.blocksExpected })),
      forks: result.forks,
      scheduleVersion: result.scheduleVersion,
      timestamp: result.timestampEnd,
    });
  }
}
