import { describe, it, expect } from 'vitest';
import { ShipClient } from '../../src/ship/ShipClient.js';
import type { ShipResult, ShipGetStatusResult } from '../../src/ship/types.js';

/**
 * E2E test against the live Libre testnet SHiP endpoint.
 *
 * This test connects to the real SHiP WebSocket, receives blocks,
 * and verifies deserialization works correctly.
 *
 * Requires network access to api.testnet.libre.pdx.cryptobloks.io.
 * Skip in CI with: SKIP_E2E=1 npm test
 */
const SKIP_E2E = process.env.SKIP_E2E === '1' || process.env.CI === 'true';
const describeE2E = SKIP_E2E ? describe.skip : describe;

describeE2E('SHiP E2E - Libre Testnet', () => {
  it('should connect to SHiP and receive status', async () => {
    const client = new ShipClient({
      url: 'wss://api.testnet.libre.pdx.cryptobloks.io/',
      fetchBlock: true,
      fetchTraces: false,
      fetchDeltas: false,
    });

    const status = await new Promise<ShipGetStatusResult>((resolve, reject) => {
      const timeout = setTimeout(() => {
        client.disconnect();
        reject(new Error('Timed out waiting for SHiP status'));
      }, 15000);

      client.on('status', (s: ShipGetStatusResult) => {
        clearTimeout(timeout);
        resolve(s);
      });

      client.connect().catch(reject);
    });

    expect(status.head.block_num).toBeGreaterThan(0);
    expect(status.chain_id).toBeDefined();
    expect(status.chain_id.length).toBe(64);

    client.disconnect();
  });

  it('should receive and deserialize blocks', async () => {
    const client = new ShipClient({
      url: 'wss://api.testnet.libre.pdx.cryptobloks.io/',
      fetchBlock: true,
      fetchTraces: false,
      fetchDeltas: false,
    });

    const blocks: ShipResult[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        client.disconnect();
        reject(new Error('Timed out waiting for blocks'));
      }, 30000);

      client.on('block', (result: ShipResult) => {
        blocks.push(result);
        if (blocks.length >= 5) {
          clearTimeout(timeout);
          client.disconnect();
          resolve();
        }
      });

      client.connect().catch(reject);
    });

    await done;

    expect(blocks.length).toBeGreaterThanOrEqual(5);

    for (const b of blocks) {
      expect(b.this_block).not.toBeNull();
      expect(b.this_block!.block_num).toBeGreaterThan(0);
      expect(b.block).not.toBeNull();
      expect(b.block!.producer).toBeTruthy();
      expect(b.block!.timestamp).toBeTruthy();
      expect(b.block!.schedule_version).toBeGreaterThan(0);
    }

    // Verify blocks are sequential
    for (let i = 1; i < blocks.length; i++) {
      expect(blocks[i].this_block!.block_num).toBe(
        blocks[i - 1].this_block!.block_num + 1
      );
    }
  });

  it('should receive blocks with deltas when requested', async () => {
    const client = new ShipClient({
      url: 'wss://api.testnet.libre.pdx.cryptobloks.io/',
      fetchBlock: true,
      fetchTraces: false,
      fetchDeltas: true,
    });

    const block = await new Promise<ShipResult>((resolve, reject) => {
      const timeout = setTimeout(() => {
        client.disconnect();
        reject(new Error('Timed out waiting for block with deltas'));
      }, 20000);

      client.on('block', (result: ShipResult) => {
        if (result.deltas.length > 0) {
          clearTimeout(timeout);
          client.disconnect();
          resolve(result);
        }
      });

      client.connect().catch(reject);
    });

    expect(block.deltas.length).toBeGreaterThan(0);

    for (const delta of block.deltas) {
      expect(delta.name).toBeTruthy();
      expect(Array.isArray(delta.rows)).toBe(true);
    }
  });

  it('should connect to failover endpoint', async () => {
    const client = new ShipClient({
      url: 'wss://api.testnet.libre.iad.cryptobloks.io/',
      fetchBlock: true,
      fetchTraces: false,
      fetchDeltas: false,
    });

    const status = await new Promise<ShipGetStatusResult>((resolve, reject) => {
      const timeout = setTimeout(() => {
        client.disconnect();
        reject(new Error('Timed out waiting for failover SHiP status'));
      }, 15000);

      client.on('status', (s: ShipGetStatusResult) => {
        clearTimeout(timeout);
        resolve(s);
      });

      client.connect().catch(reject);
    });

    expect(status.head.block_num).toBeGreaterThan(0);
    client.disconnect();
  });
});
