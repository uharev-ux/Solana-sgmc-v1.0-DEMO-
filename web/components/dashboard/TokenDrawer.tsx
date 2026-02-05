"use client";

import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Copy, ExternalLink, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getToken, getTokenCandles, getTokenTrigger } from "@/lib/api";
import type { TokenDetail, TriggerDetail } from "@/lib/types";
import { formatUsd, formatPct, formatAge, resolveAgeSec, buildDexscreenerUrl, buildGmgnUrl } from "@/lib/utils";
import { MiniChart } from "./MiniChart";
import { cn } from "@/lib/utils";

const prefersReducedMotion =
  typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

interface TokenDrawerProps {
  tokenAddress: string | null;
  pairAddress: string | null;
  onClose: () => void;
}

export function TokenDrawer({ tokenAddress, pairAddress, onClose }: TokenDrawerProps) {
  const [token, setToken] = useState<TokenDetail | null>(null);
  const [trigger, setTrigger] = useState<TriggerDetail | null>(null);
  const [candles5m, setCandles5m] = useState<{ timeSec: number; open: number; high: number; low: number; close: number }[]>([]);
  const [candles15m, setCandles15m] = useState<{ timeSec: number; open: number; high: number; low: number; close: number }[]>([]);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!tokenAddress) {
      setToken(null);
      setTrigger(null);
      setCandles5m([]);
      setCandles15m([]);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const [t, tr, c5, c15] = await Promise.all([
          getToken(tokenAddress),
          getTokenTrigger(tokenAddress),
          getTokenCandles(tokenAddress, "5m"),
          getTokenCandles(tokenAddress, "15m"),
        ]);
        if (!cancelled) {
          setToken(t ?? null);
          setTrigger(tr ?? null);
          setCandles5m(c5 ?? []);
          setCandles15m(c15 ?? []);
        }
      } catch {
        if (!cancelled) {
          setToken(null);
          setTrigger(null);
          setCandles5m([]);
          setCandles15m([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tokenAddress]);

  const copyAddress = () => {
    const addr = token?.baseAddress ?? tokenAddress ?? "";
    if (addr) {
      void navigator.clipboard.writeText(addr);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const ageSec = token
    ? resolveAgeSec({ ageSec: token.ageSec, pairCreatedAtMs: token.pairCreatedAtMs })
    : null;

  const dexUrl = pairAddress ? buildDexscreenerUrl(pairAddress) : "#";
  const gmgnUrl = (token?.baseAddress ?? tokenAddress) ? buildGmgnUrl(token?.baseAddress ?? tokenAddress ?? "") : "#";

  return (
    <AnimatePresence>
      {tokenAddress != null && (
        <motion.div
          className="fixed inset-y-0 right-0 z-50 w-full max-w-md border-l border-border bg-card shadow-glow"
          initial={prefersReducedMotion ? false : { x: "100%" }}
          animate={{ x: 0 }}
          exit={{ x: "100%" }}
          transition={{ type: "tween", duration: 0.2 }}
        >
          <div className="flex h-full flex-col overflow-auto">
            <div className="sticky top-0 z-10 flex items-center justify-between border-b border-border bg-card p-4">
              <h2 className="text-lg font-semibold">
                {token?.baseSymbol ?? tokenAddress?.slice(0, 8) ?? "—"}
              </h2>
              <Button variant="ghost" size="icon" onClick={onClose}>
                <X className="h-4 w-4" />
              </Button>
            </div>

            <div className="flex-1 space-y-4 p-4">
              <div className="flex items-center gap-2">
                <code className="flex-1 truncate rounded bg-muted px-2 py-1 text-xs text-muted-foreground">
                  {token?.baseAddress ?? tokenAddress ?? "—"}
                </code>
                <Button variant="ghost" size="icon" onClick={copyAddress}>
                  {copied ? (
                    <span className="text-xs text-green-500">OK</span>
                  ) : (
                    <Copy className="h-4 w-4" />
                  )}
                </Button>
              </div>

              <div className="grid grid-cols-2 gap-2 text-sm">
                <Stat label="Age" value={formatAge(ageSec)} />
                <Stat label="Price" value={formatUsd(token?.priceUsd)} />
                <Stat label="Liq" value={formatUsd(token?.liquidityUsd)} />
                <Stat label="Vol 24h" value={formatUsd(token?.volumeH24)} />
                <Stat label="% 24h" value={formatPct(token?.priceChangeH24, { showPlus: true })} />
                <Stat
                  label="Txns"
                  value={
                    token?.txnsH24Buys != null && token?.txnsH24Sells != null
                      ? String(token.txnsH24Buys + token.txnsH24Sells)
                      : "—"
                  }
                />
              </div>

              <div className="flex flex-wrap gap-2">
                {token?.dexId && (
                  <span className="rounded bg-muted px-2 py-0.5 text-xs">{token.dexId}</span>
                )}
                {token?.triggerOutcome && (
                  <span
                    className={cn(
                      "rounded px-2 py-0.5 text-xs",
                      token.triggerOutcome === "TP1_FIRST" && "bg-green-500/20 text-green-400",
                      token.triggerOutcome === "SL_FIRST" && "bg-red-500/20 text-red-400",
                      token.triggerOutcome === "NEITHER" && "bg-muted text-muted-foreground"
                    )}
                  >
                    {token.triggerOutcome}
                  </span>
                )}
                {token?.triggerStatus && (
                  <span
                    className={cn(
                      "rounded px-2 py-0.5 text-xs",
                      token.triggerStatus === "DONE" && "text-green-500",
                      token.triggerStatus === "PENDING" && "text-amber-500",
                      token.triggerStatus === "NO_DATA" && "text-muted-foreground"
                    )}
                  >
                    {token.triggerStatus}
                  </span>
                )}
              </div>

              <div className="flex gap-2">
                <Button variant="outline" size="sm" asChild>
                  <a href={gmgnUrl} target="_blank" rel="noopener noreferrer">
                    GMGN
                    <ExternalLink className="h-3 w-3" />
                  </a>
                </Button>
                <Button variant="outline" size="sm" asChild>
                  <a href={dexUrl} target="_blank" rel="noopener noreferrer">
                    Dexscreener
                    <ExternalLink className="h-3 w-3" />
                  </a>
                </Button>
              </div>

              <div className="space-y-2">
                <p className="text-xs font-medium text-muted-foreground">5m</p>
                <MiniChart data={candles5m} height={80} className="rounded border border-border" />
                <p className="text-xs font-medium text-muted-foreground">15m</p>
                <MiniChart data={candles15m} height={80} className="rounded border border-border" />
              </div>

              {trigger && (
                <div className="space-y-2 rounded border border-border p-3">
                  <p className="text-sm font-medium">Trigger</p>
                  <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground">
                    <span>Outcome:</span>
                    <span className={cn(
                      trigger.outcome === "TP1_FIRST" && "text-green-500",
                      trigger.outcome === "SL_FIRST" && "text-red-500"
                    )}>
                      {trigger.outcome ?? "—"}
                    </span>
                    <span>Status:</span>
                    <span>{trigger.status}</span>
                    {trigger.mfePct != null && (
                      <>
                        <span>MFE %:</span>
                        <span>{formatPct(trigger.mfePct)}</span>
                      </>
                    )}
                    {trigger.maePct != null && (
                      <>
                        <span>MAE %:</span>
                        <span>{formatPct(trigger.maePct)}</span>
                      </>
                    )}
                    {trigger.postTp1MaxPct != null && (
                      <>
                        <span>Post-TP1 max %:</span>
                        <span>{formatPct(trigger.postTp1MaxPct)}</span>
                      </>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span className="text-muted-foreground">{label}:</span>{" "}
      <span className="font-medium">{value}</span>
    </div>
  );
}
