/** API types (null-safe for UI) */

export type RunnerMode = "once" | "loop";

export interface HealthResponse {
  ok: boolean;
  version?: string;
}

export interface RunnerStatus {
  running: boolean;
  mode?: RunnerMode;
  intervalSec?: number;
  dbPath?: string;
  lastCycleStartedAt?: number;
  lastCycleFinishedAt?: number;
  lastError?: string;
  lastErrorAt?: number;
}

export interface SelfCheckStep {
  name: string;
  pass: boolean;
  message?: string;
}

export interface SelfCheckReport {
  runId: string;
  pass: boolean;
  steps: SelfCheckStep[];
  summary?: string;
}

export type TriggerOutcome = "TP1_FIRST" | "SL_FIRST" | "NEITHER";

export interface TokenRow {
  pairAddress: string;
  baseSymbol: string;
  baseAddress: string;
  quoteSymbol?: string;
  priceUsd: number | null;
  liquidityUsd: number | null;
  volumeH24: number | null;
  priceChangeH24: number | null;
  txnsH24Buys: number | null;
  txnsH24Sells: number | null;
  /** Age in seconds (preferred); backend may send this. */
  ageSec?: number | null;
  pairCreatedAtMs: number | null;
  dexId: string | null;
  url: string | null;
  triggerOutcome?: TriggerOutcome | null;
  triggerStatus?: "DONE" | "NO_DATA" | "PENDING" | null;
}

export interface TokenDetail extends TokenRow {
  chainId?: string | null;
  volumeM5?: number | null;
  volumeH1?: number | null;
  volumeH6?: number | null;
  priceChangeM5?: number | null;
  priceChangeH1?: number | null;
  priceChangeH6?: number | null;
  fdv?: number | null;
  marketCap?: number | null;
}

/** Candle point: timeSec = UNIX seconds (number) */
export interface CandlePoint {
  timeSec: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface TriggerDetail {
  outcome: TriggerOutcome | null;
  status: "DONE" | "NO_DATA" | "PENDING";
  tp1HitTs?: number | null;
  slHitTs?: number | null;
  tp1Price?: number | null;
  slPrice?: number | null;
  mfePct?: number | null;
  maePct?: number | null;
  buHitAfterTp1?: number | null;
  postTp1MaxPct?: number | null;
}

export interface TokensResponse {
  items: TokenRow[];
  total: number;
  page?: number;
  pageSize?: number;
}

export interface FiltersState {
  ageMaxHours: number | null; // null = no filter (e.g. "all age")
  minLiquidity: number;
  minVolume: number;
  dexId: string | null;
  triggerStatus: string | null;
  triggerOutcome: string | null; // TP1_FIRST | SL_FIRST | NEITHER | null
}
