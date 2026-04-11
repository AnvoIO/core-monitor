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
}
