/** SHiP protocol message types */

export interface ProducerAuthority {
  producer_name: string;
  authority: any; // block_signing_authority variant
}

export interface ProducerScheduleChange {
  version: number;
  producers: ProducerAuthority[];
}

export interface ShipBlockHeader {
  timestamp: string;
  producer: string;
  confirmed: number;
  previous: string;
  transaction_mroot: string;
  action_mroot: string;
  schedule_version: number;
  /** Legacy field — empty when WTMSIG is active */
  new_producers?: {
    version: number;
    producers: Array<{ producer_name: string; block_signing_key: string }>;
  };
  /** Header extensions — contains producer_schedule_change_extension (ID 1) when WTMSIG is active */
  header_extensions: Array<{ type: number; data: any }>;
  /**
   * Savanna header fields. Present only on chains whose SHiP ABI has a Savanna block_header;
   * schedule_version is then meaningless (-1) and the schedule must be tracked from these.
   */
  savanna?: SavannaHeaderFields;
}

export interface SavannaHeaderFields {
  slot: number;
  qcClaim: { blockNum: number; isStrong: boolean };
  proposerDiff: {
    version: number;
    proposalSlot: number;
    removeIndexes: number[];
    insertIndexes: Array<{ index: number; value: { producer_name: string; block_signing_key: string } }>;
  } | null;
}

export interface ShipBlock {
  block_num: number;
  block_id: string;
  block: ShipBlockHeader;
}

export interface ShipTableDelta {
  name: string;
  rows: Array<{
    present: boolean;
    data: any;
  }>;
}

export interface ShipActionTrace {
  act: {
    account: string;
    name: string;
    authorization: Array<{ actor: string; permission: string }>;
    data: any;
  };
  receiver: string;
}

export interface ShipTransactionTrace {
  id: string;
  status: number;
  action_traces: ShipActionTrace[];
}

export interface ShipResult {
  head: { block_num: number; block_id: string };
  /** Optional so synthetic results (tests, catchup) needn't supply it. */
  last_irreversible?: { block_num: number; block_id: string } | null;
  this_block: { block_num: number; block_id: string } | null;
  prev_block: { block_num: number; block_id: string } | null;
  block: ShipBlockHeader | null;
  traces: ShipTransactionTrace[];
  deltas: ShipTableDelta[];
}

export interface ShipGetStatusResult {
  head: { block_num: number; block_id: string };
  last_irreversible: { block_num: number; block_id: string };
  trace_begin_block: number;
  trace_end_block: number;
  chain_state_begin_block: number;
  chain_state_end_block: number;
  chain_id: string;
}

export type ShipClientState = 'disconnected' | 'connecting' | 'connected' | 'streaming';

/** Cloudflare Access service token. Sent only to public wss:// hosts. */
export interface ShipAccessCredentials {
  clientId: string;
  clientSecret: string;
}

export interface ShipClientOptions {
  url: string;
  failoverUrls?: string[];
  startBlock?: number;
  endBlock?: number;
  fetchBlock?: boolean;
  fetchTraces?: boolean;
  fetchDeltas?: boolean;
  maxReconnectAttempts?: number;
  reconnectDelayMs?: number;
  /** Kill the connection if no message arrives within this many ms (default 30000) */
  stallTimeoutMs?: number;
  /** Abort a connection attempt that hasn't completed its handshake in this many ms (default 10000) */
  handshakeTimeoutMs?: number;
  /** While on a failover endpoint, check the primary this often (default 60000) */
  failbackIntervalMs?: number;
  access?: ShipAccessCredentials;
  /** Epoch of the chain's block timestamps, in Unix ms (default 2000-01-01; Wire uses 2025-01-01) */
  blockTimestampEpochMs?: number;
}
