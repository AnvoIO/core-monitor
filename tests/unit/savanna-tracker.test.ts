import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { ABI } from '@wharfkit/antelope';
import {
  SavannaProposerTracker,
  applyPolicyDiff,
  scheduledProducer,
  type ProposerPolicy,
  type SavannaHeader,
} from '../../src/monitor/SavannaProposerTracker.js';
import { ShipClient, isSavannaAbi, slotToTimestamp } from '../../src/ship/ShipClient.js';
import { WIRE_EPOCH_MS } from '../../src/utils/chainTime.js';
import { timestampToSlot } from '../../src/monitor/RoundEvaluator.js';

const auth = (name: string) => ({ producer_name: name, block_signing_key: `KEY_${name}` });
const policy = (version: number, names: string[], proposalSlot = 0): ProposerPolicy => ({
  version,
  proposalSlot,
  producers: names.map(auth),
});

/** Header for block n at slot s, claiming a strong QC on the previous block. */
const header = (n: number, slot: number, extra: Partial<SavannaHeader> = {}): SavannaHeader => ({
  blockNum: n,
  slot,
  qcClaim: { blockNum: n - 1, isStrong: true },
  proposerDiff: null,
  ...extra,
});

describe('applyPolicyDiff', () => {
  it('removes by original index, then inserts at final index', () => {
    const base = policy(1, ['a', 'b', 'c', 'd']);
    const next = applyPolicyDiff(base, {
      version: 2,
      proposalSlot: 10,
      removeIndexes: [1, 3],
      insertIndexes: [{ index: 0, value: auth('x') }, { index: 3, value: auth('y') }],
    });
    expect(next.producers.map((p) => p.producer_name)).toEqual(['x', 'a', 'c', 'y']);
    expect(next.version).toBe(2);
    expect(next.proposalSlot).toBe(10);
  });

  it('rejects out-of-range or unordered indexes', () => {
    const base = policy(1, ['a', 'b']);
    expect(() => applyPolicyDiff(base, { version: 2, proposalSlot: 0, removeIndexes: [2], insertIndexes: [] })).toThrow();
    expect(() => applyPolicyDiff(base, { version: 2, proposalSlot: 0, removeIndexes: [1, 0], insertIndexes: [] })).toThrow();
  });
});

describe('SavannaProposerTracker', () => {
  const v1 = ['a', 'b', 'c'];
  const diffTo = (version: number, proposalSlot: number) => ({
    version,
    proposalSlot,
    removeIndexes: [2],
    insertIndexes: [{ index: 0, value: auth('z') }],
  });

  /** Feed consecutive blocks from slot `from` to `to` inclusive; returns activation block slots. */
  const run = (t: SavannaProposerTracker, from: number, to: number, opts: { diffAt?: number; version?: number; strong?: (slot: number) => boolean } = {}) => {
    const activations: number[] = [];
    for (let slot = from; slot <= to; slot++) {
      const h = header(slot, slot, {
        proposerDiff: slot === opts.diffAt ? diffTo(opts.version ?? 2, slot) : null,
        qcClaim: { blockNum: slot - 1, isStrong: opts.strong ? opts.strong(slot) : true },
      });
      if (t.next(h).activated) activations.push(slot);
    }
    return activations;
  };

  it('activates a proposal at the first block of the second producer turn after it', () => {
    // Matches block 2397251 -> 2397273 on Wire testnet: proposed at slot ...902 (turn 900),
    // active from slot ...924 (turn 924).
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    const activations = run(t, 1000, 1060, { diffAt: 1010 });
    expect(activations).toEqual([1032]);
    expect(t.active.version).toBe(2);
    expect(t.active.producers.map((p) => p.producer_name)).toEqual(['z', 'a', 'b']);
    expect(t.pending).toBeNull();
    expect(t.proposed).toBeNull();
  });

  it('moves the proposal to pending at the next turn', () => {
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    run(t, 1000, 1012, { diffAt: 1010 });
    expect(t.proposed?.version).toBe(2);
    run(t, 1013, 1020);
    expect(t.pending?.version).toBe(2);
    expect(t.proposed).toBeNull();
    expect(t.active.version).toBe(1);
  });

  it('waits for finality before activating', () => {
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    // Establish finality, then stall it (weak QCs) from slot 1005 to 1070.
    const activations = run(t, 1000, 1100, { diffAt: 1010, strong: (s) => s < 1005 || s > 1070 });
    // Finality resumes at 1071; the proposal (slot 1010) is final once block 1010 is final,
    // i.e. after two strong links: next turn boundary after that is 1080.
    expect(activations).toEqual([1080]);
  });

  it('uses two-chain finality: a strong QC on block B finalizes the block B claims, not B', () => {
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    // Only block 1011 carries a strong QC (on 1010, the proposing block) after 1004. Under
    // two-chain finality that makes 1009 final, so the proposal at 1010 never becomes final.
    const activations = run(t, 1000, 1100, { diffAt: 1010, strong: (s) => s < 1005 || s === 1011 });
    expect(activations).toEqual([]);
    expect(t.lastFinalBlockNum).toBe(1009);
  });

  it('applies a diff to the latest proposed policy, not the active one', () => {
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    run(t, 1000, 1015, { diffAt: 1010 });
    // Second proposal while v2 is pending: base is v2.
    t.next(header(1016, 1016, { proposerDiff: { version: 3, proposalSlot: 1016, removeIndexes: [0], insertIndexes: [] } }));
    expect(t.proposed?.producers.map((p) => p.producer_name)).toEqual(['a', 'b']);
  });

  it('discards a proposal from a block that was forked out', () => {
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    run(t, 1000, 1010);
    t.next(header(1011, 1011, { proposerDiff: diffTo(2, 1011) }));
    expect(t.proposed?.version).toBe(2);
    // Replacement block 1011 (next slot) without the diff.
    t.next(header(1011, 1012));
    expect(t.proposed).toBeNull();
    for (let n = 1012; n <= 1060; n++) t.next(header(n, n + 1));
    expect(t.active.version).toBe(1);
    expect(t.pending).toBeNull();
  });

  it('survives a snapshot round-trip mid-proposal', () => {
    const t = SavannaProposerTracker.fromPolicies(policy(1, v1));
    run(t, 1000, 1015, { diffAt: 1010 });
    const restored = new SavannaProposerTracker(JSON.parse(JSON.stringify(t.snapshot())));
    expect(run(restored, 1016, 1060)).toEqual([1032]);
  });

  it('schedules producers by slot in 12-slot turns', () => {
    const p = policy(1, ['a', 'b', 'c']).producers;
    expect(scheduledProducer(p, 0)).toBe('a');
    expect(scheduledProducer(p, 11)).toBe('a');
    expect(scheduledProducer(p, 12)).toBe('b');
    expect(scheduledProducer(p, 36)).toBe('a');
  });
});

describe('Savanna SHiP headers (real Wire testnet blocks)', () => {
  const fixture = JSON.parse(
    readFileSync(new URL('../fixtures/wire-testnet-ship.json', import.meta.url), 'utf8')
  ) as { abi: any; blocks: Record<string, { id: string; hex: string }> };

  const client = new ShipClient({ url: 'ws://127.0.0.1:1', blockTimestampEpochMs: WIRE_EPOCH_MS });
  (client as any).abi = ABI.from(fixture.abi);
  const decode = (num: number) =>
    (client as any).decodeSavannaHeader({ array: Uint8Array.from(Buffer.from(fixture.blocks[num].hex, 'hex')) });

  it('recognises the Savanna ABI', () => {
    expect(isSavannaAbi(fixture.abi)).toBe(true);
    expect(isSavannaAbi({ structs: [{ name: 'block_header', fields: [{ name: 'schedule_version' }] }] })).toBe(false);
  });

  it('decodes producer, Wire-epoch timestamp and QC claim', () => {
    const h = decode(2397250);
    expect(h.producer).toBe('defproducerg');
    expect(h.timestamp).toBe('2026-11-02T17:12:30.500');
    expect(h.savanna.qcClaim).toEqual({ blockNum: 2397249, isStrong: true });
    expect(h.savanna.proposerDiff).toBeNull();
    expect(timestampToSlot(h.timestamp, WIRE_EPOCH_MS)).toBe(h.savanna.slot);
  });

  it('decodes the proposer policy diff of block 2397251', () => {
    const h = decode(2397251);
    expect(h.timestamp).toBe('2026-11-02T17:12:31.000');
    expect(h.savanna.proposerDiff).toEqual({
      version: 47,
      proposalSlot: h.savanna.slot,
      removeIndexes: [20],
      insertIndexes: [
        {
          index: 0,
          value: { producer_name: 'defproducera', block_signing_key: 'PUB_K1_5i2THRhGzWnz5qn4XMhqiSP3MhFhbR7EPkUFpQBG4k92p5MGNf' },
        },
      ],
    });
  });

  it('still delivers a block whose traces use a signature type the decoder lacks', async () => {
    const { Serializer } = await import('@wharfkit/antelope');
    const traces = Uint8Array.from(Buffer.from((fixture as any).traces['2466269'], 'hex'));
    // The fixture really is undecodable (Wire signature curve unknown to wharfkit)...
    expect(() => Serializer.decode({ data: traces, type: 'transaction_trace[]', abi: (client as any).abi })).toThrow(
      /Unknown curve type/
    );
    // ...and the client still emits the block, with no traces.
    const emitted: any[] = [];
    client.on('block', (r) => emitted.push(r));
    (client as any).handleBlockResult({
      head: { block_num: 2466300, block_id: 'h' },
      last_irreversible: { block_num: 2466298, block_id: 'l' },
      this_block: { block_num: 2466269, block_id: 'b' },
      prev_block: { block_num: 2466268, block_id: 'p' },
      block: null,
      traces: { array: traces },
      deltas: null,
    });
    expect(emitted).toHaveLength(1);
    expect(emitted[0].this_block.block_num).toBe(2466269);
    expect(emitted[0].traces).toEqual([]);
  });

  it('formats slots as UTC timestamps without a zone suffix', () => {
    expect(slotToTimestamp(0, WIRE_EPOCH_MS)).toBe('2025-01-01T00:00:00.000');
    expect(slotToTimestamp(3, Date.UTC(2000, 0, 1))).toBe('2000-01-01T00:00:01.500');
  });
});
