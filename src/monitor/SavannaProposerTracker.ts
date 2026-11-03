/**
 * Tracks the active producer schedule ("proposer policy") of a Savanna chain from block headers.
 *
 * Savanna headers carry no schedule_version. A new schedule arrives as a diff
 * (new_proposer_policy_diff) against the latest proposed policy, and the chain promotes it
 * on its own timetable. This class mirrors Wire v1.0.0 (libraries/chain/block_header_state.cpp,
 * block_header_state_utils.hpp, finality_core.cpp):
 *
 * - A "round" here is one producer's turn: 12 slots (config::producer_repetitions), not the
 *   whole schedule.
 * - For the first block of a round, the parent's state decides the active policy:
 *   the latest proposed policy if it was proposed before the prior round started and is
 *   final; else the pending policy if it is final; else the current active policy.
 * - On the first block of a round, a proposed policy moves to pending if pending is empty.
 * - A header diff applies to the parent's latest proposed policy (proposed, else pending,
 *   else active).
 * - Finality is two-chain: a block whose qc_claim is strong on block S makes final the block
 *   that S's own qc_claim names.
 */

export interface ProposerAuthority {
  producer_name: string;
  block_signing_key: string;
}

export interface ProposerPolicy {
  version: number;
  /** Slot of the block that proposed this policy (0 when unknown, e.g. bootstrapped from RPC). */
  proposalSlot: number;
  producers: ProposerAuthority[];
}

export interface ProposerPolicyDiff {
  version: number;
  proposalSlot: number;
  removeIndexes: number[];
  insertIndexes: Array<{ index: number; value: ProposerAuthority }>;
}

export interface SavannaHeader {
  blockNum: number;
  slot: number;
  qcClaim: { blockNum: number; isStrong: boolean };
  proposerDiff?: ProposerPolicyDiff | null;
}

export interface SavannaTrackerState {
  active: ProposerPolicy;
  pending: ProposerPolicy | null;
  proposed: ProposerPolicy | null;
  /** Last final block number after the last processed block; null when not yet known. */
  lastFinalBlockNum: number | null;
  lastFinalSlot: number | null;
  /** Last processed block. */
  prevBlockNum: number | null;
  prevSlot: number | null;
  /** Recent blocks: block number -> [slot, qc claim target]. Needed to resolve finality. */
  recent: Array<[number, number, number]>;
}

export interface SavannaStep {
  /** Policy that became active at this block, if the active policy changed. */
  activated: ProposerPolicy | null;
  /** Policy proposed by this block's header, if any. */
  proposed: ProposerPolicy | null;
}

export const PRODUCER_REPETITIONS = 12;
const RECENT_LIMIT = 2048;
/** Per-block states kept so a fork can resume from the replacement block's parent. */
const HISTORY_LIMIT = 64;

type CoreState = Omit<SavannaTrackerState, 'recent'>;

export function roundStartSlot(slot: number): number {
  return slot - (slot % PRODUCER_REPETITIONS);
}

function inSameRound(next: number, curr: number): boolean {
  return next < roundStartSlot(curr) + PRODUCER_REPETITIONS;
}

function priorRoundStartSlot(slot: number): number | null {
  if (slot < PRODUCER_REPETITIONS) return null;
  return roundStartSlot(slot) - PRODUCER_REPETITIONS;
}

function firstBlockOfRound(curr: number, parent: number): boolean {
  return parent < roundStartSlot(curr);
}

/** Producer scheduled for a slot under a producer list. */
export function scheduledProducer(producers: readonly ProposerAuthority[], slot: number): string {
  const index = Math.floor((slot % (producers.length * PRODUCER_REPETITIONS)) / PRODUCER_REPETITIONS);
  return producers[index].producer_name;
}

/** fc::ordered_diff::apply_diff: remove by original index, then insert at final index. */
export function applyPolicyDiff(base: ProposerPolicy, diff: ProposerPolicyDiff): ProposerPolicy {
  const producers = [...base.producers];
  diff.removeIndexes.forEach((index, i) => {
    if (i > 0 && index <= diff.removeIndexes[i - 1]) throw new Error('remove_indexes not increasing');
    const at = index - i;
    if (at < 0 || at >= producers.length) throw new Error(`remove index ${index} out of range`);
    producers.splice(at, 1);
  });
  diff.insertIndexes.forEach(({ index, value }, i) => {
    if (i > 0 && index <= diff.insertIndexes[i - 1].index) throw new Error('insert_indexes not increasing');
    if (index > producers.length) throw new Error(`insert index ${index} out of range`);
    producers.splice(index, 0, value);
  });
  return { version: diff.version, proposalSlot: diff.proposalSlot, producers };
}

export class SavannaProposerTracker {
  private s: SavannaTrackerState;
  private recent = new Map<number, { slot: number; claim: number }>();
  /** State after each recent block, keyed by block number. */
  private history = new Map<number, CoreState>();

  constructor(state: SavannaTrackerState) {
    this.s = { ...state };
    for (const [num, slot, claim] of state.recent) this.recent.set(num, { slot, claim });
  }

  /** Start from a known active policy (and any in-flight ones), e.g. from RPC. */
  static fromPolicies(
    active: ProposerPolicy,
    pending: ProposerPolicy | null = null,
    proposed: ProposerPolicy | null = null
  ): SavannaProposerTracker {
    return new SavannaProposerTracker({
      active,
      pending,
      proposed,
      lastFinalBlockNum: null,
      lastFinalSlot: null,
      prevBlockNum: null,
      prevSlot: null,
      recent: [],
    });
  }

  get active(): ProposerPolicy {
    return this.s.active;
  }

  get pending(): ProposerPolicy | null {
    return this.s.pending;
  }

  get proposed(): ProposerPolicy | null {
    return this.s.proposed;
  }

  get lastFinalBlockNum(): number | null {
    return this.s.lastFinalBlockNum;
  }

  /** Serializable state, for persisting across restarts. */
  snapshot(): SavannaTrackerState {
    return {
      ...this.s,
      recent: [...this.recent.entries()].slice(-64).map(([num, r]) => [num, r.slot, r.claim]),
    };
  }

  /**
   * Whether a policy proposed at `proposalSlot` is final as of the parent block. Unknown
   * finality (just started) counts as final: Savanna finalizes within a second or two.
   */
  private isFinal(proposalSlot: number): boolean {
    return this.s.lastFinalSlot === null || proposalSlot <= this.s.lastFinalSlot;
  }

  /** Active policy for a block at `slot`, decided from the parent's state. */
  private activeFor(slot: number): ProposerPolicy {
    const { active, pending, proposed, prevSlot } = this.s;
    if (prevSlot === null || inSameRound(slot, prevSlot)) return active;
    if (!proposed && !pending) return active;

    const prior = priorRoundStartSlot(prevSlot);
    if (proposed && prior !== null && proposed.proposalSlot < prior && this.isFinal(proposed.proposalSlot)) {
      return proposed;
    }
    if (pending && this.isFinal(pending.proposalSlot)) return pending;
    return active;
  }

  private saveHistory(blockNum: number): void {
    const { recent: _recent, ...core } = this.s;
    this.history.set(blockNum, core);
    while (this.history.size > HISTORY_LIMIT) {
      this.history.delete(this.history.keys().next().value as number);
    }
  }

  /**
   * On a fork the next header doesn't follow the last one. Resume from the state after its
   * parent, if that is still in history; otherwise carry on from the current state.
   * Returns true if state was rewound.
   */
  private rewindForFork(blockNum: number): boolean {
    if (this.s.prevBlockNum === null || blockNum === this.s.prevBlockNum + 1) return false;
    const parent = this.history.get(blockNum - 1);
    if (!parent) return false;
    for (const num of [...this.history.keys()]) if (num >= blockNum) this.history.delete(num);
    for (const num of [...this.recent.keys()]) if (num >= blockNum) this.recent.delete(num);
    this.s = { ...parent, recent: [] };
    return true;
  }

  /** Process the next block header (in chain order; forks rewind to the parent's state). */
  next(header: SavannaHeader): SavannaStep {
    this.rewindForFork(header.blockNum);
    const parentLastProposed = this.s.proposed ?? this.s.pending ?? this.s.active;
    let activated: ProposerPolicy | null = null;

    // 1. Promote proposed/pending to active (decided from the parent's state).
    const newActive = this.activeFor(header.slot);
    if (newActive !== this.s.active) {
      activated = newActive;
      if (this.s.proposed && newActive === this.s.proposed) {
        this.s.proposed = null;
        this.s.pending = null;
      } else if (this.s.pending && newActive === this.s.pending) {
        this.s.pending = null;
      }
      this.s.active = newActive;
    }

    // 2. First block of a round: proposed moves into an empty pending slot.
    if (
      this.s.prevSlot !== null &&
      firstBlockOfRound(header.slot, this.s.prevSlot) &&
      this.s.proposed &&
      !this.s.pending
    ) {
      this.s.pending = this.s.proposed;
      this.s.proposed = null;
    }

    // 3. A diff in this header proposes a new policy, relative to the parent's latest proposed.
    let proposed: ProposerPolicy | null = null;
    if (header.proposerDiff) {
      proposed = applyPolicyDiff(parentLastProposed, header.proposerDiff);
      this.s.proposed = proposed;
    }

    // 4. Finality after this block (two-chain rule).
    this.recent.set(header.blockNum, { slot: header.slot, claim: header.qcClaim.blockNum });
    if (header.qcClaim.isStrong) {
      const claimed = this.recent.get(header.qcClaim.blockNum);
      if (claimed) {
        const finalNum = claimed.claim;
        const finalBlock = this.recent.get(finalNum);
        if (finalBlock && (this.s.lastFinalBlockNum === null || finalNum > this.s.lastFinalBlockNum)) {
          this.s.lastFinalBlockNum = finalNum;
          this.s.lastFinalSlot = finalBlock.slot;
        }
      }
    }
    while (this.recent.size > RECENT_LIMIT) {
      this.recent.delete(this.recent.keys().next().value as number);
    }

    this.s.prevBlockNum = header.blockNum;
    this.s.prevSlot = header.slot;
    this.saveHistory(header.blockNum);
    return { activated, proposed };
  }
}
