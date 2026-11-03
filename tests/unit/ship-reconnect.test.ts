import { afterEach, describe, expect, it } from 'vitest';
import { ABI, Serializer } from '@wharfkit/antelope';
import { WebSocket, WebSocketServer } from 'ws';
import type { AddressInfo } from 'net';
import { ShipClient, isPublicWssUrl } from '../../src/ship/ShipClient.js';
import type { ShipResult } from '../../src/ship/types.js';

const ABI_JSON = {
  version: 'eosio::abi/1.1',
  structs: [
    { name: 'get_status_request_v0', base: '', fields: [] },
    { name: 'block_position', base: '', fields: [
      { name: 'block_num', type: 'uint32' },
      { name: 'block_id', type: 'checksum256' },
    ] },
    { name: 'get_status_result_v0', base: '', fields: [
      { name: 'head', type: 'block_position' },
      { name: 'last_irreversible', type: 'block_position' },
      { name: 'trace_begin_block', type: 'uint32' },
      { name: 'trace_end_block', type: 'uint32' },
      { name: 'chain_state_begin_block', type: 'uint32' },
      { name: 'chain_state_end_block', type: 'uint32' },
      { name: 'chain_id', type: 'checksum256' },
    ] },
    { name: 'get_blocks_request_v0', base: '', fields: [
      { name: 'start_block_num', type: 'uint32' },
      { name: 'end_block_num', type: 'uint32' },
      { name: 'max_messages_in_flight', type: 'uint32' },
      { name: 'have_positions', type: 'block_position[]' },
      { name: 'irreversible_only', type: 'bool' },
      { name: 'fetch_block', type: 'bool' },
      { name: 'fetch_traces', type: 'bool' },
      { name: 'fetch_deltas', type: 'bool' },
    ] },
    { name: 'get_blocks_ack_request_v0', base: '', fields: [
      { name: 'num_messages', type: 'uint32' },
    ] },
    { name: 'get_blocks_result_v0', base: '', fields: [
      { name: 'head', type: 'block_position' },
      { name: 'last_irreversible', type: 'block_position' },
      { name: 'this_block', type: 'block_position?' },
      { name: 'prev_block', type: 'block_position?' },
      { name: 'block', type: 'bytes?' },
      { name: 'traces', type: 'bytes?' },
      { name: 'deltas', type: 'bytes?' },
    ] },
  ],
  variants: [
    { name: 'request', types: ['get_status_request_v0', 'get_blocks_request_v0', 'get_blocks_ack_request_v0'] },
    { name: 'result', types: ['get_status_result_v0', 'get_blocks_result_v0'] },
  ],
};
const abi = ABI.from(ABI_JSON as any);

const idOf = (num: number, branch = 'a') =>
  branch.repeat(8) + num.toString(16).padStart(56, '0');
const position = (num: number, branch = 'a') => ({ block_num: num, block_id: idOf(num, branch) });

interface BlockRequest {
  start: number;
  have: Array<{ block_num: number; block_id: string }>;
  inFlight: number;
}

/** Real WebSocket endpoint speaking enough of the SHiP protocol, including have_positions. */
class FakeShip {
  server!: WebSocketServer;
  port = 0;
  requests: BlockRequest[] = [];
  upgradeHeaders: Array<Record<string, unknown>> = [];
  private sockets = new Set<WebSocket>();
  private timers = new Set<NodeJS.Timeout>();
  /** Blocks each socket may still be sent: max_messages_in_flight, topped up by acks. */
  private credits = new Map<WebSocket, number>();

  /** branchOf lets a test give one block number different ids on different nodes. */
  constructor(
    public head: number,
    public opts: { firstBlock?: number; branchOf?: (num: number) => string; advance?: boolean } = {}
  ) {}

  async start(port = 0): Promise<this> {
    this.server = await new Promise<WebSocketServer>((resolve) => {
      const s = new WebSocketServer({ port }, () => resolve(s));
    });
    this.port = (this.server.address() as AddressInfo).port;
    this.server.on('connection', (socket, req) => {
      this.upgradeHeaders.push(req.headers);
      this.sockets.add(socket);
      socket.on('close', () => this.sockets.delete(socket));
      socket.send(JSON.stringify(ABI_JSON));
      socket.on('message', (data: Buffer) => this.onRequest(socket, data));
    });
    return this;
  }

  get url(): string {
    return `ws://127.0.0.1:${this.port}`;
  }

  private branch(num: number): string {
    return this.opts.branchOf ? this.opts.branchOf(num) : 'a';
  }

  private blockIdAvailable(num: number): string | null {
    return num <= this.head && num >= (this.opts.firstBlock ?? 1) ? idOf(num, this.branch(num)) : null;
  }

  private onRequest(socket: WebSocket, data: Buffer): void {
    const [type, request] = Serializer.decode({
      data: new Uint8Array(data),
      type: 'request',
      abi,
    }) as unknown as [string, any];

    if (type === 'get_status_request_v0') {
      const head = position(this.head, this.branch(this.head));
      socket.send(Serializer.encode({
        type: 'result',
        abi,
        object: ['get_status_result_v0', {
          head,
          last_irreversible: head,
          trace_begin_block: 2,
          trace_end_block: this.head + 1,
          chain_state_begin_block: 1,
          chain_state_end_block: this.head + 1,
          chain_id: 'c'.repeat(64),
        }],
      }).array);
    } else if (type === 'get_blocks_request_v0') {
      const have = (request.have_positions as any[]).map((p) => ({
        block_num: Number(p.block_num),
        block_id: String(p.block_id),
      }));
      let start = Number(request.start_block_num);
      this.requests.push({ start, have, inFlight: Number(request.max_messages_in_flight) });
      this.credits.set(socket, Number(request.max_messages_in_flight));

      // Same rule as nodeos: rewind to any position the node lacks or disagrees with.
      for (const p of have) {
        if (p.block_num >= start) continue;
        if (this.blockIdAvailable(p.block_num) !== p.block_id) start = p.block_num;
      }
      this.stream(socket, start);
    } else if (type === 'get_blocks_ack_request_v0') {
      this.credits.set(socket, (this.credits.get(socket) ?? 0) + Number(request.num_messages));
    }
  }

  private stream(socket: WebSocket, from: number): void {
    let next = from;
    const timer = setInterval(() => {
      if (socket.readyState !== WebSocket.OPEN) {
        clearInterval(timer);
        this.timers.delete(timer);
        return;
      }
      if (this.opts.advance) this.head++;
      while (next <= this.head && (this.credits.get(socket) ?? 0) > 0) {
        this.credits.set(socket, this.credits.get(socket)! - 1);
        const headPos = position(this.head, this.branch(this.head));
        socket.send(Serializer.encode({
          type: 'result',
          abi,
          object: ['get_blocks_result_v0', {
            head: headPos,
            last_irreversible: headPos,
            this_block: position(next, this.branch(next)),
            prev_block: position(next - 1, this.branch(next - 1)),
            block: null,
            traces: null,
            deltas: null,
          }],
        }).array);
        next++;
      }
    }, 5);
    this.timers.add(timer);
  }

  async stop(): Promise<void> {
    for (const t of this.timers) clearInterval(t);
    this.timers.clear();
    for (const s of this.sockets) s.terminate();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

async function until(check: () => boolean, label: string, timeoutMs = 4000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for: ${label}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

async function closedPort(): Promise<number> {
  const ship = await new FakeShip(1).start();
  const port = ship.port;
  await ship.stop();
  return port;
}

describe('ShipClient reconnect over real sockets', () => {
  const clients: ShipClient[] = [];
  const ships: FakeShip[] = [];

  const startShip = async (head: number, opts?: ConstructorParameters<typeof FakeShip>[1], port = 0) => {
    const ship = await new FakeShip(head, opts).start(port);
    ships.push(ship);
    return ship;
  };

  const startClient = (options: ConstructorParameters<typeof ShipClient>[0]) => {
    const client = new ShipClient({ reconnectDelayMs: 5, failbackIntervalMs: 20, ...options });
    const blocks: Array<{ num: number; id: string; prev: string | null }> = [];
    client.on('block', (r: ShipResult) => {
      blocks.push({ num: r.this_block!.block_num, id: r.this_block!.block_id, prev: r.prev_block?.block_id ?? null });
    });
    clients.push(client);
    return { client, blocks };
  };

  const currentUrl = (client: ShipClient): string => (client as any).currentUrl;

  afterEach(async () => {
    for (const c of clients.splice(0)) c.disconnect();
    for (const s of ships.splice(0)) await s.stop().catch(() => {});
  });

  it('reaches the failover when the primary refuses connections', async () => {
    const failover = await startShip(105);
    const primary = `ws://127.0.0.1:${await closedPort()}`;
    const { client, blocks } = startClient({ url: primary, failoverUrls: [failover.url], startBlock: 100 });

    await client.connect().catch(() => {});
    await until(() => blocks.length === 6, 'blocks from failover');

    expect(currentUrl(client)).toBe(failover.url);
    expect(blocks.map((b) => b.num)).toEqual([100, 101, 102, 103, 104, 105]);
  });

  it('keeps alternating endpoints when a reconnect attempt fails', async () => {
    // Regression: a failed reconnect rotated twice, so with two URLs it retried one forever.
    const primary = await startShip(110);
    const deadFailover = `ws://127.0.0.1:${await closedPort()}`;
    const { client, blocks } = startClient({ url: primary.url, failoverUrls: [deadFailover], startBlock: 100 });

    await client.connect();
    await until(() => blocks.length === 11, 'blocks from primary');

    const port = primary.port;
    await primary.stop();
    await until(() => (client as any).reconnectAttempts >= 3, 'failed attempts');

    const restarted = await startShip(115, undefined, port);
    await until(() => restarted.requests.length > 0, 'reconnect to the endpoint that came back');
    await until(() => blocks.at(-1)?.num === 115, 'blocks after recovery');
  });

  it('counts one attempt per failed reconnect', async () => {
    const port = await closedPort();
    const attempts: number[] = [];
    const { client } = startClient({ url: `ws://127.0.0.1:${port}`, reconnectDelayMs: 25 });
    await client.connect().catch(() => {});

    // First timer-driven attempt fails at ~25ms, the next at ~75ms.
    await new Promise((r) => setTimeout(r, 50));
    attempts.push((client as any).reconnectAttempts);

    expect(attempts).toEqual([2]);
  });

  it('resumes after the last delivered block, not the original start block', async () => {
    const ship = await startShip(110);
    const { client, blocks } = startClient({ url: ship.url, startBlock: 100 });

    await client.connect();
    await until(() => blocks.length === 11, 'initial blocks');

    for (const socket of ship.server.clients) socket.terminate();
    await until(() => ship.requests.length === 2, 'reconnect request');

    expect(ship.requests[1].start).toBe(111);
    expect(ship.requests[1].have.at(-1)).toEqual(position(110));
    expect(blocks.map((b) => b.num)).toEqual(Array.from({ length: 11 }, (_, i) => 100 + i));
  });

  it('asks for the configured in-flight window, defaulting to 10', async () => {
    const ship = await startShip(5);
    const small = startClient({ url: ship.url, startBlock: 1 });
    const large = startClient({ url: ship.url, startBlock: 1, maxMessagesInFlight: 1000 });
    await small.client.connect();
    await large.client.connect();
    await until(() => ship.requests.length === 2, 'both block requests');
    expect(ship.requests.map((r) => r.inFlight).sort((a, b) => a - b)).toEqual([10, 1000]);
  });

  it('with manualAck, sends no more than the window until the consumer acknowledges', async () => {
    const ship = await startShip(50);
    const { client, blocks } = startClient({ url: ship.url, startBlock: 1, maxMessagesInFlight: 3, manualAck: true });
    await client.connect();
    await until(() => blocks.length === 3, 'first window');
    await new Promise((r) => setTimeout(r, 50));
    expect(blocks.length).toBe(3);
    client.acknowledge();
    client.acknowledge();
    await until(() => blocks.length === 5, 'two more blocks');
    await new Promise((r) => setTimeout(r, 50));
    expect(blocks.map((b) => b.num)).toEqual([1, 2, 3, 4, 5]);
  });

  it('fails over and back with every block delivered exactly once', async () => {
    const primary = await startShip(110, { advance: true });
    const failover = await startShip(110, { advance: true });
    const { client, blocks } = startClient({ url: primary.url, failoverUrls: [failover.url], startBlock: 100 });

    await client.connect();
    await until(() => blocks.length > 5, 'streaming from primary');

    const port = primary.port;
    await primary.stop();
    await until(() => currentUrl(client) === failover.url && failover.requests.length > 0, 'move to failover');
    const onFailover = blocks.length;
    await until(() => blocks.length > onFailover + 5, 'streaming from failover');

    const restarted = await startShip(failover.head, { advance: true }, port);
    await until(() => restarted.requests.length > 0, 'return to primary');
    expect(currentUrl(client)).toBe(primary.url);
    const onPrimary = blocks.length;
    await until(() => blocks.length > onPrimary + 5, 'streaming from primary again');

    const nums = blocks.map((b) => b.num);
    expect(nums).toEqual(Array.from({ length: nums.length }, (_, i) => 100 + i));
    for (let i = 1; i < blocks.length; i++) {
      expect(blocks[i].prev).toBe(blocks[i - 1].id);
    }
  });

  it('reports disconnects, reconnect attempts and endpoint changes, and passes LIB through', async () => {
    const primary = await startShip(110, { advance: true });
    const failover = await startShip(110, { advance: true });
    const { client, blocks } = startClient({ url: primary.url, failoverUrls: [failover.url], startBlock: 100 });
    const events: string[] = [];
    let lib: number | undefined;
    client.on('disconnect', ({ reason }: { reason: string }) => events.push(`disconnect:${reason}`));
    client.on('reconnect_attempt', () => events.push('reconnect_attempt'));
    client.on('endpoint_change', ({ from, to }: { from: number; to: number }) => events.push(`endpoint:${from}->${to}`));
    client.on('block', (r: ShipResult) => { lib = r.last_irreversible?.block_num; });

    await client.connect();
    await until(() => blocks.length > 5, 'streaming from primary');
    expect(client.endpointIndex).toBe(0);
    expect(lib).toBeGreaterThanOrEqual(110);

    const port = primary.port;
    await primary.stop();
    await until(() => client.endpointIndex === 1 && client.connectionState === 'streaming', 'on failover');
    expect(client.endpointUrl).toBe(failover.url);

    await startShip(failover.head, { advance: true }, port);
    await until(() => client.endpointIndex === 0 && client.connectionState === 'streaming', 'back on primary');

    expect(events[0]).toBe('disconnect:close');
    expect(events).toContain('reconnect_attempt');
    expect(events).toContain('endpoint:0->1');
    expect(events.at(-1)).toBe('endpoint:1->0');
  });

  it('stays on the failover while the primary is behind the stream', async () => {
    const failover = await startShip(1000);
    const lagging = await startShip(500);
    const primaryUrl = lagging.url;
    const { client, blocks } = startClient({ url: primaryUrl, failoverUrls: [failover.url], startBlock: 990 });
    // Begin on the failover, as if the primary had dropped.
    (client as any).urlIndex = 1;
    (client as any).currentUrl = failover.url;

    await client.connect();
    await until(() => blocks.length === 11, 'blocks from failover');
    await new Promise((r) => setTimeout(r, 120));

    expect(currentUrl(client)).toBe(failover.url);
    expect(lagging.requests).toHaveLength(0);

    lagging.head = 1000;
    await until(() => currentUrl(client) === primaryUrl, 'failback after primary caught up');
  });

  it('drops blocks re-sent with the same id by a node that was behind', async () => {
    const ship = await startShip(110);
    const { client, blocks } = startClient({ url: ship.url, startBlock: 100 });

    await client.connect();
    await until(() => blocks.length === 11, 'initial blocks');

    // Node restarts behind us: it lacks 106-110, so it rewinds and later re-sends them.
    const port = ship.port;
    await ship.stop();
    const behind = await startShip(105, undefined, port);
    await until(() => behind.requests.length === 1, 'reconnect');
    behind.head = 112;
    await until(() => blocks.length === 13, 'new blocks');

    expect(blocks.map((b) => b.num)).toEqual(Array.from({ length: 13 }, (_, i) => 100 + i));
  });

  it('delivers replacement blocks when the new node is on a different fork', async () => {
    const ship = await startShip(110);
    const { client, blocks } = startClient({ url: ship.url, startBlock: 100 });

    await client.connect();
    await until(() => blocks.length === 11, 'initial blocks');

    const port = ship.port;
    await ship.stop();
    const forked = await startShip(112, { branchOf: (n) => (n >= 108 ? 'b' : 'a') }, port);
    await until(() => forked.requests.length === 1, 'reconnect');
    await until(() => blocks.at(-1)?.num === 112, 'replacement branch');

    const replay = blocks.slice(11);
    expect(replay.map((b) => b.num)).toEqual([108, 109, 110, 111, 112]);
    expect(replay[0].id).toBe(idOf(108, 'b'));

    // A later reconnect reports the branch we are now on, not the abandoned one.
    for (const socket of forked.server.clients) socket.terminate();
    await until(() => forked.requests.length === 2, 'second reconnect');
    expect(forked.requests[1].start).toBe(113);
    expect(forked.requests[1].have.at(-1)).toEqual(position(112, 'b'));
    expect(forked.requests[1].have.find((p) => p.block_num === 108)).toEqual(position(108, 'b'));
  });

  it('never sends access headers to a LAN endpoint', async () => {
    const ship = await startShip(101);
    const { client, blocks } = startClient({
      url: ship.url,
      startBlock: 100,
      access: { clientId: 'id-value', clientSecret: 'secret-value' },
    });

    await client.connect();
    await until(() => blocks.length === 2, 'blocks');

    expect(ship.upgradeHeaders[0]).not.toHaveProperty('cf-access-client-id');
    expect(ship.upgradeHeaders[0]).not.toHaveProperty('cf-access-client-secret');
  });
});

describe('ShipClient access headers', () => {
  const access = { clientId: 'id-value', clientSecret: 'secret-value' };
  const optionsFor = (url: string, withAccess = true) =>
    (new ShipClient({ url, access: withAccess ? access : undefined }) as any).socketOptions(url);

  it('sends both headers to a public wss host', () => {
    expect(optionsFor('wss://ship.libre.pdx.cryptobloks.io').headers).toEqual({
      'CF-Access-Client-Id': 'id-value',
      'CF-Access-Client-Secret': 'secret-value',
    });
  });

  it('sends nothing to LAN or plain ws URLs', () => {
    expect(optionsFor('ws://10.10.10.182:8080').headers).toBeUndefined();
    expect(optionsFor('wss://10.10.10.182:8080').headers).toBeUndefined();
    expect(optionsFor('ws://ship.libre.pdx.cryptobloks.io').headers).toBeUndefined();
  });

  it('sends nothing when no token is configured', () => {
    expect(optionsFor('wss://ship.libre.pdx.cryptobloks.io', false).headers).toBeUndefined();
  });

  it('never follows redirects', () => {
    expect(optionsFor('wss://ship.libre.pdx.cryptobloks.io').followRedirects).toBe(false);
  });

  it.each([
    ['wss://ship.libre.pdx.cryptobloks.io', true],
    ['wss://ship.example.com:8443/path', true],
    ['wss://8.8.8.8:8080', true],
    ['ws://ship.libre.pdx.cryptobloks.io', false],
    ['https://ship.libre.pdx.cryptobloks.io', false],
    ['wss://10.10.10.182:8080', false],
    ['wss://172.20.0.4', false],
    ['wss://172.32.0.1', true],
    ['wss://192.168.1.1', false],
    ['wss://127.0.0.1', false],
    ['wss://169.254.1.1', false],
    ['wss://100.64.0.1', false],
    ['wss://[::1]:8080', false],
    ['wss://[fd00::1]', false],
    ['wss://[fe80::1]', false],
    ['wss://[::ffff:10.0.0.1]', false],
    ['wss://localhost:8080', false],
    ['wss://ship-node', false],
    ['wss://ship.local', false],
    ['wss://ship.internal', false],
    ['not a url', false],
  ])('classifies %s as public=%s', (url, expected) => {
    expect(isPublicWssUrl(url)).toBe(expected);
  });
});
