/**
 * Schedule tracking for the catchup writer on Savanna chains (e.g. Wire).
 *
 * The catchup writer starts at the chain's first block, so the schedule tracker starts from
 * the genesis producer and applies every proposer policy diff from there. That avoids the
 * live writer's RPC bootstrap (which only knows the current schedule) and gives each schedule
 * a real activation point, so round numbers line up with the chain's history.
 */

import { logger } from '../utils/logger.js';
import { Database } from '../store/Database.js';
import { ScheduleTracker } from './ScheduleTracker.js';
import { RoundEvaluator } from './RoundEvaluator.js';
import { SavannaProposerTracker, type SavannaHeader, type SavannaTrackerState } from './SavannaProposerTracker.js';
import type { ChainConfig } from '../config.js';

const log = logger.child({ module: 'catchup-savanna' });

/** monitor_state key for the catchup writer's tracker snapshot. */
export const CATCHUP_SAVANNA_STATE_KEY = 'catchup_savanna_tracker';
/** Persist the tracker at least this often (blocks), besides every proposal and activation. */
const PERSIST_INTERVAL = 1000;

export interface SavannaCatchupBlock {
  producer: string;
  timestamp: string;
  savanna: Omit<SavannaHeader, 'blockNum'>;
}

export class SavannaCatchupSchedule {
  private tracker: SavannaProposerTracker | null = null;
  /** Last block the restored tracker had already processed; blocks up to it are replays. */
  private resumedThrough = 0;

  constructor(
    private config: ChainConfig,
    private db: Database,
    private schedule: ScheduleTracker,
    private evaluator: RoundEvaluator
  ) {
    evaluator.useScheduleSizedRounds();
  }

  /** Version of the active schedule, or null before the first block. */
  get activeVersion(): number | null {
    return this.tracker ? this.tracker.active.version : null;
  }

  /**
   * Follow the schedule through one block. Returns false if the block can't be processed:
   * a fresh run has to start at the chain's first block, since no other starting schedule
   * is known.
   */
  async process(block: SavannaCatchupBlock, blockNum: number): Promise<boolean> {
    const { chain, network } = this.config;

    if (!this.tracker) {
      const saved = await this.db.getState(chain, network, CATCHUP_SAVANNA_STATE_KEY);
      if (saved) {
        const state = JSON.parse(saved) as SavannaTrackerState;
        this.tracker = new SavannaProposerTracker(state);
        this.resumedThrough = state.prevBlockNum ?? 0;
        log.info({ chain: this.config.id, active: state.active.version, through: this.resumedThrough },
          'Restored Savanna schedule tracker');
      } else if (blockNum <= 2) {
        // Genesis: block 2's producer (block 1 has none) is the only producer until the first
        // policy diff takes effect.
        const genesis = { producer_name: block.producer, block_signing_key: 'unknown' };
        this.tracker = SavannaProposerTracker.fromPolicies({ version: 0, proposalSlot: 0, producers: [genesis] });
        await this.schedule.updateSchedule(0, [genesis], blockNum, block.timestamp);
        await this.evaluator.setScheduleActivation(block.timestamp);
        log.info({ chain: this.config.id, blockNum, producer: block.producer }, 'Savanna schedule starts from genesis producer');
      } else {
        log.error({ chain: this.config.id, blockNum },
          'Savanna catchup must start at the chain\'s first block (set CATCHUP_START_BLOCK=1) or resume from a saved tracker');
        return false;
      }
    }

    // After a restart the stream resumes at the evaluator's last block, which may be before
    // the tracker's; those blocks are already in the tracker's state.
    if (blockNum <= this.resumedThrough) return true;

    const tracker = this.tracker;
    const step = tracker.next({ blockNum, ...block.savanna });

    if (step.proposed) {
      log.info({ chain: this.config.id, version: step.proposed.version, blockNum, timestamp: block.timestamp },
        'Savanna schedule proposed');
    }

    if (step.activated) {
      const policy = step.activated;
      const changed = await this.schedule.updateSchedule(policy.version, policy.producers, blockNum, block.timestamp);
      if (changed) {
        await this.evaluator.setScheduleActivation(block.timestamp);
        log.info({ chain: this.config.id, version: policy.version, blockNum }, 'Schedule activated');
      }
    }

    if (step.proposed || step.activated || blockNum % PERSIST_INTERVAL === 0) {
      await this.persist();
    }
    return true;
  }

  /**
   * Block to resume a stream from so the tracker sees every block after its saved state:
   * the earlier of the evaluator's resume block and the block after the tracker's last one.
   */
  static async resumeBlock(db: Database, config: ChainConfig, evaluatorResume: number): Promise<number> {
    const saved = await db.getState(config.chain, config.network, CATCHUP_SAVANNA_STATE_KEY);
    if (!saved) return evaluatorResume;
    const prev = (JSON.parse(saved) as SavannaTrackerState).prevBlockNum;
    return prev === null ? evaluatorResume : Math.min(evaluatorResume, prev + 1);
  }

  async persist(): Promise<void> {
    if (!this.tracker) return;
    await this.db.setState(this.config.chain, this.config.network, CATCHUP_SAVANNA_STATE_KEY,
      JSON.stringify(this.tracker.snapshot()));
  }
}
