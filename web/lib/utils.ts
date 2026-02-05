import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/** Format number as $ with K/M/B; "—" if null */
export function formatUsd(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return "—";
  if (value >= 1e9) return `$${(value / 1e9).toFixed(2)}B`;
  if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
  if (value >= 1e3) return `$${(value / 1e3).toFixed(2)}K`;
  return `$${value.toFixed(2)}`;
}

/** Resolve age in seconds: prefer ageSec, fallback from pairCreatedAtMs. */
export function resolveAgeSec({
  ageSec,
  pairCreatedAtMs,
}: {
  ageSec?: number | null;
  pairCreatedAtMs?: number | null;
}): number | null {
  if (ageSec != null && !Number.isNaN(ageSec)) return Math.floor(ageSec);
  if (pairCreatedAtMs != null && !Number.isNaN(pairCreatedAtMs))
    return Math.floor((Date.now() - pairCreatedAtMs) / 1000);
  return null;
}

/** Age from seconds: "2h 13m" / "—" if null */
export function formatAge(ageSec: number | null | undefined): string {
  if (ageSec == null || Number.isNaN(ageSec) || ageSec < 0) return "—";
  const sec = Math.floor(ageSec);
  if (sec < 60) return `${sec}m`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  const h = Math.floor(min / 60);
  const m = min % 60;
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

/** Percent: "+12.3%" / "-5.1%" / "—"; showPlus default true. */
export function formatPct(
  value: number | null | undefined,
  opts?: { showPlus?: boolean }
): string {
  if (value == null || Number.isNaN(value)) return "—";
  const showPlus = opts?.showPlus !== false;
  if (value > 0) return showPlus ? `+${value.toFixed(2)}%` : `${value.toFixed(2)}%`;
  if (value < 0) return `${value.toFixed(2)}%`;
  return "0.00%";
}

export function buildDexscreenerUrl(pairAddress: string): string {
  return `https://dexscreener.com/solana/${pairAddress}`;
}

export function buildGmgnUrl(tokenAddress: string): string {
  return `https://gmgn.ai/sol/token/${tokenAddress}`;
}
