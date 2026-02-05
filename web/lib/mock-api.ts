/**
 * Mock API for dev when NEXT_PUBLIC_API_BASE_URL is not set.
 * Safe to tree-shake in prod if only used behind USE_MOCK.
 */

import type { RunnerMode } from "./types";
import type { TokensResponse, TokenRow, RunnerStatus, SelfCheckReport } from "./types";

const now = () => Date.now();

let mockRunning = false;
let mockMode: RunnerMode = "once";
let mockIntervalSec = 60;
let mockDbPath = "dexscreener.sqlite";
let mockLastCycleStart: number | undefined;
let mockLastError: string | undefined;

function genTokens(count: number, offset: number): TokenRow[] {
  const tokens: TokenRow[] = [];
  for (let i = 0; i < count; i++) {
    const idx = offset + i;
    const ageMs = (24 - (idx % 25)) * 3600 * 1000;
    const pairCreatedAtMs = now() - ageMs;
    const ageSec = Math.floor(ageMs / 1000);
    tokens.push({
      pairAddress: `pair_${idx}_${now()}`,
      baseSymbol: `TOKEN${idx}`,
      baseAddress: `addr_${idx}`,
      quoteSymbol: "SOL",
      priceUsd: 0.00001 + Math.random() * 0.1,
      liquidityUsd: 10000 + Math.random() * 500000,
      volumeH24: 5000 + Math.random() * 200000,
      priceChangeH24: (Math.random() - 0.5) * 40,
      txnsH24Buys: Math.floor(Math.random() * 500),
      txnsH24Sells: Math.floor(Math.random() * 500),
      ageSec,
      pairCreatedAtMs,
      dexId: "raydium",
      url: `https://dexscreener.com/solana/pair_${idx}`,
      triggerOutcome: idx % 5 === 0 ? "TP1_FIRST" : idx % 5 === 1 ? "SL_FIRST" : "NEITHER",
      triggerStatus: idx % 3 === 0 ? "DONE" : idx % 3 === 1 ? "PENDING" : "NO_DATA",
    });
  }
  return tokens;
}

export const mockApi = {
  async fetchJson<T>(path: string, _options?: RequestInit): Promise<T> {
    if (path.includes("/api/health")) {
      return { ok: true, version: "0.1.0" } as T;
    }
    if (path.includes("/api/status")) {
      const status: RunnerStatus = {
        running: mockRunning,
        mode: mockMode,
        intervalSec: mockIntervalSec,
        dbPath: mockDbPath,
        lastCycleStartedAt: mockLastCycleStart,
        lastCycleFinishedAt: mockLastCycleStart ? mockLastCycleStart + 5000 : undefined,
        lastError: mockLastError ?? undefined,
        lastErrorAt: mockLastError ? now() - 10000 : undefined,
      };
      return status as T;
    }
    if (path.includes("/api/tokens")) {
      const url = new URL(path, "http://x");
      const limit = parseInt(url.searchParams.get("limit") ?? "200", 10);
      const offset = parseInt(url.searchParams.get("offset") ?? "0", 10);
      const ageMaxHours = url.searchParams.get("ageMaxHours");
      const triggerStatus = url.searchParams.get("triggerStatus");
      const triggerOutcome = url.searchParams.get("triggerOutcome");
      let items = genTokens(1500, 0);
      if (ageMaxHours != null && ageMaxHours !== "") {
        const maxAgeMs = parseInt(ageMaxHours, 10) * 3600 * 1000;
        items = items.filter((r) => r.pairCreatedAtMs != null && Date.now() - r.pairCreatedAtMs <= maxAgeMs);
      }
      if (triggerStatus != null && triggerStatus !== "") {
        items = items.filter((r) => r.triggerStatus === triggerStatus);
      }
      if (triggerOutcome != null && triggerOutcome !== "") {
        items = items.filter((r) => r.triggerOutcome === triggerOutcome);
      }
      const total = items.length;
      const pageItems = items.slice(offset, offset + limit);
      return { items: pageItems, total } as T;
    }
    if (path.match(/\/api\/token\/[^/]+$/) && !path.includes("/candles") && !path.includes("/trigger")) {
      const tokenId = path.replace(/.*\/api\/token\//, "").replace(/\?.*/, "");
      const row = genTokens(1, 0)[0];
      return { ...row, baseAddress: tokenId, pairAddress: row.pairAddress } as T;
    }
    if (path.includes("/api/token/") && path.includes("/candles")) {
      const pts: { timeSec: number; open: number; high: number; low: number; close: number }[] = [];
      let p = 0.0001;
      const baseSec = Math.floor(now() / 1000);
      for (let i = 0; i < 50; i++) {
        const change = (Math.random() - 0.48) * 0.02;
        p = p * (1 + change);
        pts.push({
          timeSec: baseSec - (50 - i) * 300,
          open: p,
          high: p * 1.01,
          low: p * 0.99,
          close: p,
        });
      }
      return pts as T;
    }
    if (path.includes("/api/token/") && path.includes("/trigger")) {
      return {
        outcome: "TP1_FIRST",
        status: "DONE",
        mfePct: 2.5,
        maePct: -0.8,
        postTp1MaxPct: 1.2,
      } as T;
    }
    if (path.includes("/api/selfcheck/") && !path.endsWith("/run")) {
      const runId = path.split("/api/selfcheck/")[1]?.replace(/\/$/, "") ?? "mock-run";
      const report: SelfCheckReport = {
        runId,
        pass: true,
        steps: [
          { name: "DB exists", pass: true },
          { name: "Pairs age ≤ 24h", pass: true },
          { name: "Schema version", pass: true },
        ],
        summary: "All checks passed.",
      };
      return report as T;
    }
    if (path.includes("/api/logs/tail")) {
      const lines = Array.from({ length: 30 }, (_, i) => `[${new Date().toISOString()}] Mock log line ${i + 1}`);
      return { lines } as T;
    }
    return {} as T;
  },

  startRunner(params: { mode: RunnerMode; intervalSec?: number; dbPath?: string }) {
    mockRunning = true;
    mockMode = params.mode;
    mockIntervalSec = params.intervalSec ?? 60;
    if (params.dbPath) mockDbPath = params.dbPath;
    mockLastCycleStart = now();
    mockLastError = undefined;
  },

  stopRunner() {
    mockRunning = false;
  },

  async runSelfCheck(): Promise<{ runId: string }> {
    return { runId: `mock-${now()}` };
  },

  getLogsTail(_lines: number): Promise<string[]> {
    return Promise.resolve(
      Array.from({ length: 30 }, (_, i) => `[${new Date().toISOString()}] Mock log line ${i + 1}`)
    );
  },
};
