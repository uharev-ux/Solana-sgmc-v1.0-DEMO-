/**
 * HTTP API client for DexScanner backend.
 * Contract: GET/POST endpoints; mock fallback when API_BASE_URL unset or fetch fails (dev only).
 */

import type {
  HealthResponse,
  RunnerStatus,
  RunnerMode,
  SelfCheckReport,
  TokensResponse,
  TokenDetail,
  TriggerDetail,
  CandlePoint,
} from "./types";
import {
  healthSchema,
  runnerStatusSchema,
  selfCheckReportSchema,
  tokensResponseSchema,
  tokenDetailSchema,
  triggerDetailSchema,
  startRunnerSchema,
} from "./schemas";
import { mockApi } from "./mock-api";

const FETCH_TIMEOUT_MS = 8000;

function getBaseUrl(): string {
  const raw =
    typeof window !== "undefined"
      ? (process.env.NEXT_PUBLIC_API_BASE_URL ?? "")
      : (process.env.API_BASE_URL ?? "");
  return raw ? raw.replace(/\/+$/, "") : "";
}

export function isMockMode(): boolean {
  return typeof window !== "undefined" && !process.env.NEXT_PUBLIC_API_BASE_URL;
}

const USE_MOCK = isMockMode();

function buildUrl(path: string): string {
  const base = getBaseUrl();
  return base ? `${base}${path.startsWith("/") ? path : `/${path}`}` : path;
}

async function fetchWithTimeout(
  url: string,
  options: RequestInit & { timeout?: number } = {}
): Promise<Response> {
  const { timeout = FETCH_TIMEOUT_MS, ...rest } = options;
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...rest, signal: controller.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function fetchJson<T>(path: string, options?: RequestInit, parse?: (data: unknown) => T): Promise<T> {
  if (USE_MOCK) {
    const data = await mockApi.fetchJson<unknown>(path, options);
    return (parse ? parse(data) : data) as T;
  }
  const u = buildUrl(path);
  const res = await fetchWithTimeout(u, {
    ...options,
    headers: { "Content-Type": "application/json", ...options?.headers },
  });
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
  const data = (await res.json()) as unknown;
  return (parse ? parse(data) : data) as T;
}

// --- Health (retry 2 attempts for real API only) ---
export async function getHealth(): Promise<HealthResponse> {
  if (USE_MOCK) {
    const data = await fetchJson("/api/health", undefined, (d) => healthSchema.parse(d));
    return data as HealthResponse;
  }
  const path = "/api/health";
  let lastErr: Error | null = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const u = buildUrl(path);
      const res = await fetchWithTimeout(u, {
        headers: { "Content-Type": "application/json" },
      });
      if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
      const data = (await res.json()) as unknown;
      return healthSchema.parse(data) as HealthResponse;
    } catch (e) {
      lastErr = e instanceof Error ? e : new Error(String(e));
    }
  }
  throw lastErr ?? new Error("Health check failed");
}

// --- Status ---
export async function getStatus(): Promise<RunnerStatus> {
  const data = await fetchJson("/api/status", undefined, (d) => runnerStatusSchema.parse(d));
  return data as RunnerStatus;
}

// --- Runner ---
export async function startRunner(params: { mode: RunnerMode; intervalSec?: number; dbPath?: string }): Promise<void> {
  const body = startRunnerSchema.parse(params);
  if (USE_MOCK) {
    mockApi.startRunner(body);
    return;
  }
  const u = buildUrl("/api/runner/start");
  const res = await fetchWithTimeout(u, {
    method: "POST",
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
}

export async function stopRunner(): Promise<void> {
  if (USE_MOCK) {
    mockApi.stopRunner();
    return;
  }
  const u = buildUrl("/api/runner/stop");
  const res = await fetchWithTimeout(u, { method: "POST", headers: { "Content-Type": "application/json" } });
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
}

// --- Self-check ---
export async function runSelfCheck(): Promise<{ runId: string }> {
  if (USE_MOCK) return mockApi.runSelfCheck();
  const data = await fetchJson<{ runId: string }>("/api/selfcheck/run", { method: "POST" });
  return data;
}

export async function getSelfCheckReport(runId: string): Promise<SelfCheckReport> {
  const data = await fetchJson(`/api/selfcheck/${encodeURIComponent(runId)}`, undefined, (d) =>
    selfCheckReportSchema.parse(d)
  );
  return data as SelfCheckReport;
}

// --- Tokens (limit/offset) ---
export interface GetTokensParams {
  limit?: number;
  offset?: number;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  ageMaxHours?: number | null;
  minLiquidity?: number;
  minVolume?: number;
  dexId?: string;
  triggerStatus?: string | null;
  triggerOutcome?: string | null;
  search?: string;
}

export async function getTokens(params: GetTokensParams = {}): Promise<TokensResponse> {
  const sp = new URLSearchParams();
  if (params.limit != null) sp.set("limit", String(params.limit));
  if (params.offset != null) sp.set("offset", String(params.offset));
  if (params.sortBy) sp.set("sortBy", params.sortBy);
  if (params.sortDir) sp.set("sortDir", params.sortDir);
  if (params.ageMaxHours != null) sp.set("ageMaxHours", String(params.ageMaxHours));
  if (params.minLiquidity != null) sp.set("minLiquidity", String(params.minLiquidity));
  if (params.minVolume != null) sp.set("minVolume", String(params.minVolume));
  if (params.dexId) sp.set("dexId", params.dexId);
  if (params.triggerStatus != null && params.triggerStatus !== "") sp.set("triggerStatus", params.triggerStatus);
  if (params.triggerOutcome != null && params.triggerOutcome !== "") sp.set("triggerOutcome", params.triggerOutcome);
  if (params.search) sp.set("search", params.search);
  const q = sp.toString();
  const path = `/api/tokens${q ? `?${q}` : ""}`;
  const data = await fetchJson(path, undefined, (d) => tokensResponseSchema.parse(d));
  return data as TokensResponse;
}

export async function getToken(tokenId: string): Promise<TokenDetail | null> {
  const data = await fetchJson(
    `/api/token/${encodeURIComponent(tokenId)}`,
    undefined,
    (d) => (d == null ? null : tokenDetailSchema.parse(d))
  );
  return data as TokenDetail | null;
}

export async function getTokenCandles(tokenId: string, tf: "5m" | "15m"): Promise<CandlePoint[]> {
  const path = `/api/token/${encodeURIComponent(tokenId)}/candles?tf=${tf}`;
  const data = await fetchJson(path, undefined, (d) =>
    Array.isArray(d)
      ? (d as { timeSec: number; open: number; high: number; low: number; close: number }[]).map((x) => ({
          timeSec: x.timeSec,
          open: x.open,
          high: x.high,
          low: x.low,
          close: x.close,
        }))
      : []
  );
  return (data ?? []) as CandlePoint[];
}

export async function getTokenTrigger(tokenId: string): Promise<TriggerDetail | null> {
  const data = await fetchJson(
    `/api/token/${encodeURIComponent(tokenId)}/trigger`,
    undefined,
    (d) => (d == null ? null : triggerDetailSchema.parse(d))
  );
  return data as TriggerDetail | null;
}

// --- Logs ---
export async function getLogsTail(lines = 200): Promise<string[]> {
  const path = `/api/logs/tail?lines=${lines}`;
  if (USE_MOCK) return mockApi.getLogsTail(lines);
  const data = await fetchJson<{ lines?: string[] }>(path);
  return data?.lines ?? [];
}
