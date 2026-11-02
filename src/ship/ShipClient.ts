import WebSocket from 'ws';
import { Serializer, ABI } from '@wharfkit/antelope';
import { EventEmitter } from 'events';
import { BlockList, isIP } from 'net';
import { logger } from '../utils/logger.js';
import type {
  ShipAccessCredentials,
  ShipClientOptions,
  ShipClientState,
  ShipResult,
  ShipGetStatusResult,
  ShipBlockHeader,
  ShipTransactionTrace,
  ShipTableDelta,
} from './types.js';

// The SHiP protocol sends the ABI as the first WebSocket message.
// We parse it on connect and use it for all subsequent serialization.
// The static ABI definition below is kept as a reference but is no longer used at runtime.
const _SHIP_ABI_REFERENCE = {
  version: 'eosio::abi/1.1',
  types: [],
  structs: [
    {
      name: 'get_status_request_v0',
      base: '',
      fields: [],
    },
    {
      name: 'get_blocks_request_v0',
      base: '',
      fields: [
        { name: 'start_block_num', type: 'uint32' },
        { name: 'end_block_num', type: 'uint32' },
        { name: 'max_messages_in_flight', type: 'uint32' },
        { name: 'have_positions', type: 'block_position[]' },
        { name: 'irreversible_only', type: 'bool' },
        { name: 'fetch_block', type: 'bool' },
        { name: 'fetch_traces', type: 'bool' },
        { name: 'fetch_deltas', type: 'bool' },
      ],
    },
    {
      name: 'get_blocks_ack_request_v0',
      base: '',
      fields: [{ name: 'num_messages', type: 'uint32' }],
    },
    {
      name: 'block_position',
      base: '',
      fields: [
        { name: 'block_num', type: 'uint32' },
        { name: 'block_id', type: 'checksum256' },
      ],
    },
    {
      name: 'get_status_result_v0',
      base: '',
      fields: [
        { name: 'head', type: 'block_position' },
        { name: 'last_irreversible', type: 'block_position' },
        { name: 'trace_begin_block', type: 'uint32' },
        { name: 'trace_end_block', type: 'uint32' },
        { name: 'chain_state_begin_block', type: 'uint32' },
        { name: 'chain_state_end_block', type: 'uint32' },
        { name: 'chain_id', type: 'checksum256' },
      ],
    },
    {
      name: 'get_blocks_result_v0',
      base: '',
      fields: [
        { name: 'head', type: 'block_position' },
        { name: 'last_irreversible', type: 'block_position' },
        { name: 'this_block', type: 'block_position?' },
        { name: 'prev_block', type: 'block_position?' },
        { name: 'block', type: 'bytes?' },
        { name: 'traces', type: 'bytes?' },
        { name: 'deltas', type: 'bytes?' },
      ],
    },
    {
      name: 'row',
      base: '',
      fields: [
        { name: 'present', type: 'bool' },
        { name: 'data', type: 'bytes' },
      ],
    },
    {
      name: 'table_delta_v0',
      base: '',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'rows', type: 'row[]' },
      ],
    },
    {
      name: 'action_trace_v0',
      base: '',
      fields: [
        { name: 'action_ordinal', type: 'varuint32' },
        { name: 'creator_action_ordinal', type: 'varuint32' },
        { name: 'receipt', type: 'action_receipt?' },
        { name: 'receiver', type: 'name' },
        { name: 'act', type: 'action' },
        { name: 'context_free', type: 'bool' },
        { name: 'elapsed', type: 'int64' },
        { name: 'console', type: 'string' },
        { name: 'account_ram_deltas', type: 'account_delta[]' },
        { name: 'except', type: 'string?' },
        { name: 'error_code', type: 'uint64?' },
      ],
    },
    {
      name: 'action_trace_v1',
      base: '',
      fields: [
        { name: 'action_ordinal', type: 'varuint32' },
        { name: 'creator_action_ordinal', type: 'varuint32' },
        { name: 'receipt', type: 'action_receipt?' },
        { name: 'receiver', type: 'name' },
        { name: 'act', type: 'action' },
        { name: 'context_free', type: 'bool' },
        { name: 'elapsed', type: 'int64' },
        { name: 'console', type: 'string' },
        { name: 'account_ram_deltas', type: 'account_delta[]' },
        { name: 'except', type: 'string?' },
        { name: 'error_code', type: 'uint64?' },
        { name: 'return_value', type: 'bytes' },
      ],
    },
    {
      name: 'action',
      base: '',
      fields: [
        { name: 'account', type: 'name' },
        { name: 'name', type: 'name' },
        { name: 'authorization', type: 'permission_level[]' },
        { name: 'data', type: 'bytes' },
      ],
    },
    {
      name: 'permission_level',
      base: '',
      fields: [
        { name: 'actor', type: 'name' },
        { name: 'permission', type: 'name' },
      ],
    },
    {
      name: 'account_delta',
      base: '',
      fields: [
        { name: 'account', type: 'name' },
        { name: 'delta', type: 'int64' },
      ],
    },
    {
      name: 'action_receipt_v0',
      base: '',
      fields: [
        { name: 'receiver', type: 'name' },
        { name: 'act_digest', type: 'checksum256' },
        { name: 'global_sequence', type: 'uint64' },
        { name: 'recv_sequence', type: 'uint64' },
        { name: 'auth_sequence', type: 'pair_name_uint64[]' },
        { name: 'code_sequence', type: 'varuint32' },
        { name: 'abi_sequence', type: 'varuint32' },
      ],
    },
    {
      name: 'pair_name_uint64',
      base: '',
      fields: [
        { name: 'first', type: 'name' },
        { name: 'second', type: 'uint64' },
      ],
    },
    {
      name: 'transaction_trace_v0',
      base: '',
      fields: [
        { name: 'id', type: 'checksum256' },
        { name: 'status', type: 'uint8' },
        { name: 'cpu_usage_us', type: 'uint32' },
        { name: 'net_usage_words', type: 'varuint32' },
        { name: 'elapsed', type: 'int64' },
        { name: 'net_usage', type: 'uint64' },
        { name: 'scheduled', type: 'bool' },
        { name: 'action_traces', type: 'action_trace_v0[]' },
        { name: 'account_ram_delta', type: 'account_delta?' },
        { name: 'except', type: 'string?' },
        { name: 'error_code', type: 'uint64?' },
        { name: 'failed_dtrx_trace', type: 'transaction_trace_v0?' },
      ],
    },
    {
      name: 'signed_block_header',
      base: '',
      fields: [
        { name: 'timestamp', type: 'block_timestamp_type' },
        { name: 'producer', type: 'name' },
        { name: 'confirmed', type: 'uint16' },
        { name: 'previous', type: 'checksum256' },
        { name: 'transaction_mroot', type: 'checksum256' },
        { name: 'action_mroot', type: 'checksum256' },
        { name: 'schedule_version', type: 'uint32' },
        { name: 'new_producers', type: 'producer_schedule?' },
        { name: 'header_extensions', type: 'extension[]' },
        { name: 'producer_signature', type: 'signature' },
      ],
    },
    {
      name: 'signed_block',
      base: 'signed_block_header',
      fields: [
        { name: 'transactions', type: 'transaction_receipt[]' },
        { name: 'block_extensions', type: 'extension[]' },
      ],
    },
    {
      name: 'producer_schedule',
      base: '',
      fields: [
        { name: 'version', type: 'uint32' },
        { name: 'producers', type: 'producer_key[]' },
      ],
    },
    {
      name: 'producer_key',
      base: '',
      fields: [
        { name: 'producer_name', type: 'name' },
        { name: 'block_signing_key', type: 'public_key' },
      ],
    },
    {
      name: 'extension',
      base: '',
      fields: [
        { name: 'type', type: 'uint16' },
        { name: 'data', type: 'bytes' },
      ],
    },
    {
      name: 'transaction_receipt_header',
      base: '',
      fields: [
        { name: 'status', type: 'uint8' },
        { name: 'cpu_usage_us', type: 'uint32' },
        { name: 'net_usage_words', type: 'varuint32' },
      ],
    },
    {
      name: 'transaction_receipt',
      base: 'transaction_receipt_header',
      fields: [{ name: 'trx', type: 'transaction_variant' }],
    },
  ],
  actions: [],
  tables: [],
  variants: [
    {
      name: 'request',
      types: [
        'get_status_request_v0',
        'get_blocks_request_v0',
        'get_blocks_ack_request_v0',
      ],
    },
    {
      name: 'result',
      types: ['get_status_result_v0', 'get_blocks_result_v0'],
    },
    {
      name: 'action_receipt',
      types: ['action_receipt_v0'],
    },
    {
      name: 'action_trace',
      types: ['action_trace_v0', 'action_trace_v1'],
    },
    {
      name: 'transaction_variant',
      types: ['checksum256', 'packed_transaction'],
    },
  ],
};

const log = logger.child({ module: 'ShipClient' });

/** Most recent delivered blocks sent as have_positions on reconnect, for fork detection. */
const MAX_HAVE_POSITIONS = 512;
/** Primary may lag the stream by this many blocks and still be considered healthy for failback. */
const FAILBACK_HEAD_TOLERANCE = 20;
const PROBE_TIMEOUT_MS = 15000;

const privateAddresses = new BlockList();
privateAddresses.addSubnet('10.0.0.0', 8, 'ipv4');
privateAddresses.addSubnet('100.64.0.0', 10, 'ipv4');
privateAddresses.addSubnet('127.0.0.0', 8, 'ipv4');
privateAddresses.addSubnet('169.254.0.0', 16, 'ipv4');
privateAddresses.addSubnet('172.16.0.0', 12, 'ipv4');
privateAddresses.addSubnet('192.168.0.0', 16, 'ipv4');
privateAddresses.addAddress('::1', 'ipv6');
privateAddresses.addSubnet('fc00::', 7, 'ipv6');
privateAddresses.addSubnet('fe80::', 10, 'ipv6');

/**
 * True only for wss:// URLs on public hosts. Access credentials are sent to these
 * and nothing else: plain ws://, private/loopback addresses and internal names never get them.
 */
export function isPublicWssUrl(url: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  if (parsed.protocol !== 'wss:') return false;

  const host = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, '');
  const family = isIP(host);
  if (family !== 0) {
    return !privateAddresses.check(host, family === 6 ? 'ipv6' : 'ipv4');
  }
  if (!host.includes('.')) return false;
  return !/\.(localhost|local|internal|lan)$/.test(host);
}

export class ShipClient extends EventEmitter {
  private ws: WebSocket | null = null;
  private abi: ABI | null = null;
  private options: Required<Omit<ShipClientOptions, 'access'>>;
  private access: ShipAccessCredentials | null;
  private state: ShipClientState = 'disconnected';
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private stallTimer: NodeJS.Timeout | null = null;
  private failbackTimer: NodeJS.Timeout | null = null;
  private probeWs: WebSocket | null = null;
  /** Blocks delivered on the current chain, oldest first. Drives resume and have_positions. */
  private deliveredPositions: Array<{ block_num: number; block_id: string }> = [];
  private urls: string[];
  private urlIndex = 0;
  private currentUrl: string;
  private receivedAbi = false;
  private readonly maxInFlight = 10;

  constructor(options: ShipClientOptions) {
    super();
    this.options = {
      url: options.url,
      failoverUrls: options.failoverUrls || [],
      startBlock: options.startBlock || 0,
      endBlock: options.endBlock || 0xffffffff,
      fetchBlock: options.fetchBlock !== false,
      fetchTraces: options.fetchTraces || false,
      fetchDeltas: options.fetchDeltas || false,
      maxReconnectAttempts: options.maxReconnectAttempts || 100,
      reconnectDelayMs: options.reconnectDelayMs || 3000,
      stallTimeoutMs: options.stallTimeoutMs || 30000,
      handshakeTimeoutMs: options.handshakeTimeoutMs || 10000,
      failbackIntervalMs: options.failbackIntervalMs || 60000,
    };
    this.access = options.access ?? null;
    // Dedupe so a failover entry equal to the primary can't consume a rotation slot.
    const seen = new Set<string>();
    this.urls = [this.options.url, ...this.options.failoverUrls].filter((url) => {
      const key = url.replace(/\/+$/, '');
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    this.currentUrl = this.urls[0];
  }

  get connectionState(): ShipClientState {
    return this.state;
  }

  private resetStallTimer(): void {
    if (this.stallTimer) clearTimeout(this.stallTimer);
    if (this.state !== 'streaming' && this.state !== 'connected') return;

    this.stallTimer = setTimeout(() => {
      log.warn(
        { url: this.currentUrl, timeoutMs: this.options.stallTimeoutMs },
        'SHiP stall detected — no data received, forcing reconnect'
      );
      this.forceReconnect();
    }, this.options.stallTimeoutMs);
  }

  private clearStallTimer(): void {
    if (this.stallTimer) {
      clearTimeout(this.stallTimer);
      this.stallTimer = null;
    }
  }

  private forceReconnect(): void {
    this.clearStallTimer();
    this.clearFailback();
    this.dropSocket();
    this.state = 'disconnected';
    this.scheduleReconnect();
  }

  /** Close the current socket without letting it drive further state changes. */
  private dropSocket(): void {
    if (!this.ws) return;
    this.ws.removeAllListeners();
    // close() on a socket still handshaking emits 'error'; unhandled, that would crash.
    this.ws.on('error', () => {});
    this.ws.close();
    this.ws = null;
  }

  private socketOptions(url: string): WebSocket.ClientOptions {
    const options: WebSocket.ClientOptions = {
      perMessageDeflate: false,
      handshakeTimeout: this.options.handshakeTimeoutMs,
      // A redirect must not carry access headers to another host.
      followRedirects: false,
    };
    if (this.access && isPublicWssUrl(url)) {
      options.headers = {
        'CF-Access-Client-Id': this.access.clientId,
        'CF-Access-Client-Secret': this.access.clientSecret,
      };
    }
    return options;
  }

  async connect(): Promise<void> {
    if (this.state !== 'disconnected') return;
    this.state = 'connecting';
    this.receivedAbi = false;

    return new Promise((resolve, reject) => {
      log.info(
        { url: this.currentUrl, accessHeaders: !!this.access && isPublicWssUrl(this.currentUrl) },
        'Connecting to SHiP'
      );

      this.ws = new WebSocket(this.currentUrl, this.socketOptions(this.currentUrl));

      this.ws.binaryType = 'arraybuffer';

      this.ws.on('open', () => {
        log.info('SHiP WebSocket connected, waiting for ABI');
        this.reconnectAttempts = 0;
        this.resetStallTimer();
        resolve();
      });

      this.ws.on('message', (data: ArrayBuffer | Buffer | string) => {
        this.resetStallTimer();
        if (!this.receivedAbi) {
          this.handleAbiMessage(data);
        } else {
          this.handleMessage(data);
        }
      });

      this.ws.on('close', (code, reason) => {
        log.warn({ code, reason: reason.toString() }, 'SHiP WebSocket closed');
        this.clearStallTimer();
        this.clearFailback();
        this.state = 'disconnected';
        this.ws = null;
        this.scheduleReconnect();
      });

      this.ws.on('error', (err) => {
        log.error({ err }, 'SHiP WebSocket error');
        if (this.state === 'connecting') {
          reject(err);
        }
      });
    });
  }

  private handleAbiMessage(data: ArrayBuffer | Buffer | string): void {
    try {
      // SHiP sends the ABI as the first message (JSON string)
      let abiText: string;
      if (typeof data === 'string') {
        abiText = data;
      } else {
        const buffer = data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        abiText = new TextDecoder().decode(buffer);
      }

      const abiJson = JSON.parse(abiText);
      this.abi = ABI.from(abiJson);
      this.receivedAbi = true;
      this.state = 'connected';

      log.info(
        { version: abiJson.version },
        'Received SHiP ABI, sending status request'
      );

      // Send get_status_request to kick off the protocol
      this.sendStatusRequest();
    } catch (err) {
      log.error({ err }, 'Failed to parse SHiP ABI');
      this.disconnect();
    }
  }

  private sendStatusRequest(): void {
    if (!this.ws || !this.abi) return;

    const request = Serializer.encode({
      type: 'request',
      object: ['get_status_request_v0', {}],
      abi: this.abi,
    });

    this.ws.send(request.array);
  }

  disconnect(): void {
    this.clearStallTimer();
    this.clearFailback();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.dropSocket();
    this.state = 'disconnected';
    log.info('SHiP client disconnected');
  }

  private handleMessage(data: ArrayBuffer | Buffer | string): void {
    if (!this.abi) return;

    try {
      let buffer: Uint8Array;
      if (typeof data === 'string') {
        buffer = new TextEncoder().encode(data);
      } else if (data instanceof ArrayBuffer) {
        buffer = new Uint8Array(data);
      } else {
        buffer = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      }

      const decoded = Serializer.decode({
        data: buffer,
        type: 'result',
        abi: this.abi,
      });

      const [type, result] = decoded as [string, any];

      if (type === 'get_status_result_v0') {
        this.handleStatusResult(result);
      } else if (type === 'get_blocks_result_v0') {
        this.handleBlockResult(result);
      }
    } catch (err) {
      log.error({ err }, 'Failed to decode SHiP message');
    }
  }

  private handleStatusResult(status: any): void {
    const result: ShipGetStatusResult = {
      head: {
        block_num: Number(status.head.block_num),
        block_id: String(status.head.block_id),
      },
      last_irreversible: {
        block_num: Number(status.last_irreversible.block_num),
        block_id: String(status.last_irreversible.block_id),
      },
      trace_begin_block: Number(status.trace_begin_block),
      trace_end_block: Number(status.trace_end_block),
      chain_state_begin_block: Number(status.chain_state_begin_block),
      chain_state_end_block: Number(status.chain_state_end_block),
      chain_id: String(status.chain_id),
    };

    log.info(
      {
        head: result.head.block_num,
        lib: result.last_irreversible.block_num,
        traceRange: `${result.trace_begin_block}-${result.trace_end_block}`,
        deltaRange: `${result.chain_state_begin_block}-${result.chain_state_end_block}`,
      },
      'SHiP status received'
    );

    // If startBlock is 0, start from HEAD
    if (this.options.startBlock === 0) {
      this.options.startBlock = result.head.block_num;
    }

    this.emit('status', result);
    this.requestBlocks();
  }

  private handleBlockResult(result: any): void {
    if (!result.this_block) {
      // No block data — just ack
      this.ack();
      return;
    }

    const blockNum = Number(result.this_block.block_num);
    const blockId = String(result.this_block.block_id);

    // After a reconnect SHiP can re-send blocks we already delivered (the node was behind
    // our have_positions). Same id means same block; passing it on would look like a fork.
    if (this.isAlreadyDelivered(blockNum, blockId)) {
      this.ack();
      return;
    }

    let block: ShipBlockHeader | null = null;
    let traces: ShipTransactionTrace[] = [];
    let deltas: ShipTableDelta[] = [];

    // Decode block
    if (result.block) {
      try {
        // result.block is a wharfkit Bytes object with an .array property
        const blockBytes = result.block.array ?? (result.block instanceof Uint8Array
          ? result.block
          : new Uint8Array(result.block));
        const decoded = Serializer.decode({
          data: blockBytes,
          type: 'signed_block',
          abi: this.abi!,
        }) as any;
        // Parse header extensions
        const headerExtensions: Array<{ type: number; data: any }> = [];
        if (decoded.header_extensions && Array.isArray(decoded.header_extensions)) {
          for (const ext of decoded.header_extensions) {
            const extType = Number(ext.type ?? ext[0]);
            const extData = ext.data ?? ext[1];
            headerExtensions.push({ type: extType, data: extData });
          }
        }

        // Extract new_producers from legacy field OR header extensions (WTMSIG)
        let newProducers = undefined;

        // Legacy: new_producers field in block header
        if (decoded.new_producers) {
          newProducers = {
            version: Number(decoded.new_producers.version),
            producers: decoded.new_producers.producers.map((p: any) => ({
              producer_name: String(p.producer_name),
              block_signing_key: String(p.block_signing_key),
            })),
          };
        }

        // WTMSIG: producer_schedule_change_extension (extension ID 1)
        if (!newProducers) {
          const scheduleExt = headerExtensions.find(e => e.type === 1);
          if (scheduleExt && scheduleExt.data) {
            try {
              // The extension data is a serialized producer_authority_schedule
              const extBytes = scheduleExt.data.array ?? scheduleExt.data;
              if (extBytes && extBytes.length > 0) {
                const schedule = Serializer.decode({
                  data: extBytes instanceof Uint8Array ? extBytes : new Uint8Array(extBytes),
                  type: 'producer_authority_schedule',
                  abi: this.abi!,
                }) as any;
                if (schedule && schedule.producers) {
                  newProducers = {
                    version: Number(schedule.version),
                    producers: schedule.producers.map((p: any) => ({
                      producer_name: String(p.producer_name),
                      block_signing_key: p.authority?.[1]?.keys?.[0]?.key
                        ? String(p.authority[1].keys[0].key)
                        : 'unknown',
                    })),
                  };
                  log.info(
                    { version: newProducers.version, producers: newProducers.producers.length, blockNum },
                    'Schedule change detected from header extension (WTMSIG)'
                  );
                }
              }
            } catch (extErr) {
              log.warn({ err: extErr, blockNum }, 'Failed to decode producer_schedule_change_extension');
            }
          }
        }

        block = {
          timestamp: String(decoded.timestamp),
          producer: String(decoded.producer),
          confirmed: Number(decoded.confirmed),
          previous: String(decoded.previous),
          transaction_mroot: String(decoded.transaction_mroot),
          action_mroot: String(decoded.action_mroot),
          schedule_version: Number(decoded.schedule_version),
          new_producers: newProducers,
          header_extensions: headerExtensions,
        };
      } catch (err) {
        log.error({ err, blockNum }, 'Failed to decode block');
      }
    }

    // Decode traces
    if (result.traces) {
      try {
        const traceBytes = result.traces.array ?? (result.traces instanceof Uint8Array
          ? result.traces
          : new Uint8Array(result.traces));
        const decoded = Serializer.decode({
          data: traceBytes,
          type: 'transaction_trace[]',
          abi: this.abi!,
        }) as any[];
        traces = (decoded || []).map((d: any) => {
          // transaction_trace is a variant, unwrap [type, value]
          const t = Array.isArray(d) ? d[1] : d;
          return t;
        }).map((t: any) => ({
          id: String(t.id),
          status: Number(t.status),
          action_traces: (t.action_traces || []).map((a: any) => {
            const trace = Array.isArray(a) ? a[1] : a;
            return {
              act: {
                account: String(trace.act.account),
                name: String(trace.act.name),
                authorization: (trace.act.authorization || []).map((auth: any) => ({
                  actor: String(auth.actor),
                  permission: String(auth.permission),
                })),
                data: trace.act.data,
              },
              receiver: String(trace.receiver),
            };
          }),
        }));
      } catch (err) {
        log.error({ err, blockNum }, 'Failed to decode traces');
      }
    }

    // Decode deltas
    if (result.deltas) {
      try {
        const deltaBytes = result.deltas.array ?? (result.deltas instanceof Uint8Array
          ? result.deltas
          : new Uint8Array(result.deltas));
        const decoded = Serializer.decode({
          data: deltaBytes,
          type: 'table_delta[]',
          abi: this.abi!,
        }) as any[];
        deltas = (decoded || []).map((d: any) => {
          // table_delta is a variant, unwrap [type, value]
          const delta = Array.isArray(d) ? d[1] : d;
          return delta;
        }).map((d: any) => ({
          name: String(d.name),
          rows: (d.rows || []).map((r: any) => ({
            present: Boolean(r.present),
            data: r.data,
          })),
        }));
      } catch (err) {
        log.error({ err, blockNum }, 'Failed to decode deltas');
      }
    }

    const shipResult: ShipResult = {
      head: {
        block_num: Number(result.head.block_num),
        block_id: String(result.head.block_id),
      },
      this_block: { block_num: blockNum, block_id: blockId },
      prev_block: result.prev_block
        ? {
            block_num: Number(result.prev_block.block_num),
            block_id: String(result.prev_block.block_id),
          }
        : null,
      block,
      traces,
      deltas,
    };

    this.recordDelivered(blockNum, blockId);
    this.emit('block', shipResult);
    this.ack();
  }

  private isAlreadyDelivered(blockNum: number, blockId: string): boolean {
    for (let i = this.deliveredPositions.length - 1; i >= 0; i--) {
      const position = this.deliveredPositions[i];
      if (position.block_num === blockNum) return position.block_id === blockId;
      if (position.block_num < blockNum) return false;
    }
    return false;
  }

  /** Reconnects resume after this block rather than from the block we originally started at. */
  private recordDelivered(blockNum: number, blockId: string): void {
    // A lower or equal number with a new id is a fork: drop the abandoned branch.
    while (
      this.deliveredPositions.length > 0 &&
      this.deliveredPositions[this.deliveredPositions.length - 1].block_num >= blockNum
    ) {
      this.deliveredPositions.pop();
    }
    this.deliveredPositions.push({ block_num: blockNum, block_id: blockId });
    if (this.deliveredPositions.length > MAX_HAVE_POSITIONS) {
      this.deliveredPositions.shift();
    }
    this.options.startBlock = blockNum + 1;
  }

  private requestBlocks(): void {
    if (!this.ws || !this.abi || this.state !== 'connected') return;
    this.state = 'streaming';

    const startBlock = this.options.startBlock || 0;

    log.info(
      {
        startBlock: startBlock === 0 ? 'HEAD' : startBlock,
        url: this.currentUrl,
        havePositions: this.deliveredPositions.length,
        fetchBlock: this.options.fetchBlock,
        fetchTraces: this.options.fetchTraces,
        fetchDeltas: this.options.fetchDeltas,
      },
      'Requesting blocks from SHiP'
    );

    const request = Serializer.encode({
      type: 'request',
      object: [
        'get_blocks_request_v0',
        {
          start_block_num: startBlock,
          end_block_num: this.options.endBlock,
          max_messages_in_flight: this.maxInFlight,
          have_positions: this.deliveredPositions,
          irreversible_only: false,
          fetch_block: this.options.fetchBlock,
          fetch_traces: this.options.fetchTraces,
          fetch_deltas: this.options.fetchDeltas,
        },
      ],
      abi: this.abi,
    });

    this.ws.send(request.array);
    this.armFailback();
  }

  private ack(count: number = 1): void {
    if (!this.ws || !this.abi || this.ws.readyState !== WebSocket.OPEN) return;

    const request = Serializer.encode({
      type: 'request',
      object: [
        'get_blocks_ack_request_v0',
        { num_messages: count },
      ],
      abi: this.abi,
    });

    this.ws.send(request.array);
  }

  updateStartBlock(blockNum: number): void {
    this.options.startBlock = blockNum;
  }

  private armFailback(): void {
    this.clearFailbackTimer();
    if (this.urlIndex === 0) return;
    this.failbackTimer = setTimeout(() => {
      this.failbackTimer = null;
      void this.checkPrimary();
    }, this.options.failbackIntervalMs);
    this.failbackTimer.unref();
  }

  private clearFailbackTimer(): void {
    if (this.failbackTimer) {
      clearTimeout(this.failbackTimer);
      this.failbackTimer = null;
    }
  }

  private clearFailback(): void {
    this.clearFailbackTimer();
    this.closeProbe();
  }

  private closeProbe(): void {
    if (!this.probeWs) return;
    this.probeWs.removeAllListeners();
    this.probeWs.on('error', () => {});
    this.probeWs.terminate();
    this.probeWs = null;
  }

  /** While on a failover endpoint, return to the primary once it has caught up with the stream. */
  private async checkPrimary(): Promise<void> {
    const head = await this.probeHead(this.urls[0]);
    if (this.state !== 'streaming' || this.urlIndex === 0) return;

    const required = this.options.startBlock - 1 - FAILBACK_HEAD_TOLERANCE;
    if (head === null || head < required) {
      log.debug({ url: this.urls[0], head, required }, 'Primary SHiP not ready for failback');
      this.armFailback();
      return;
    }

    log.info({ url: this.urls[0], head }, 'Primary SHiP healthy, returning from failover');
    this.clearStallTimer();
    this.clearFailback();
    this.dropSocket();
    this.state = 'disconnected';
    this.urlIndex = 0;
    this.currentUrl = this.urls[0];
    this.reconnectAttempts = 0;
    this.connect().catch((err) => {
      log.error({ err }, 'Reconnect to primary failed');
      this.scheduleReconnect();
    });
  }

  /** Read head block from a SHiP endpoint on a throwaway socket. Null if it isn't serving. */
  private probeHead(url: string): Promise<number | null> {
    return new Promise((resolve) => {
      this.closeProbe();
      const socket = new WebSocket(url, this.socketOptions(url));
      socket.binaryType = 'arraybuffer';
      this.probeWs = socket;

      let abi: ABI | null = null;
      const finish = (head: number | null) => {
        clearTimeout(timeout);
        if (this.probeWs === socket) this.closeProbe();
        resolve(head);
      };
      const timeout = setTimeout(() => finish(null), PROBE_TIMEOUT_MS);
      timeout.unref();

      socket.on('error', () => finish(null));
      socket.on('close', () => finish(null));
      socket.on('message', (data: ArrayBuffer | Buffer) => {
        try {
          if (!abi) {
            abi = ABI.from(JSON.parse(new TextDecoder().decode(new Uint8Array(data))));
            socket.send(
              Serializer.encode({
                type: 'request',
                object: ['get_status_request_v0', {}],
                abi,
              }).array
            );
            return;
          }
          const [type, status] = Serializer.decode({
            data: new Uint8Array(data),
            type: 'result',
            abi,
          }) as [string, any];
          finish(type === 'get_status_result_v0' ? Number(status.head.block_num) : null);
        } catch (err) {
          log.debug({ err, url }, 'SHiP probe failed');
          finish(null);
        }
      });
    });
  }

  private scheduleReconnect(): void {
    // A failed reconnect reports through both 'close' and the connect() rejection. Without this
    // guard the URL rotates twice per failure, which with two URLs retries the same one forever.
    if (this.reconnectTimer) return;

    if (this.reconnectAttempts >= this.options.maxReconnectAttempts) {
      log.error('Max reconnection attempts reached');
      this.emit('max_reconnects');
      return;
    }

    // Cycle through URLs round-robin on each attempt
    if (this.urls.length > 1) {
      this.urlIndex = (this.urlIndex + 1) % this.urls.length;
      this.currentUrl = this.urls[this.urlIndex];
      log.info({ url: this.currentUrl, urlIndex: this.urlIndex }, 'Switching SHiP endpoint');
    }

    const delay = Math.min(
      this.options.reconnectDelayMs * Math.pow(2, Math.min(this.reconnectAttempts, 6)),
      60000
    );

    this.reconnectAttempts++;
    log.info({ attempt: this.reconnectAttempts, delayMs: delay }, 'Scheduling reconnect');

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        await this.connect();
      } catch (err) {
        log.error({ err }, 'Reconnect failed');
        this.scheduleReconnect();
      }
    }, delay);
  }
}
