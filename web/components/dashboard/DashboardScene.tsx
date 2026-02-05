"use client";

import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import { Search, RefreshCw } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { getTokens } from "@/lib/api";
import type { TokenRow } from "@/lib/types";
import { useAppStore } from "@/store/use-app-store";
import { PresetsBar } from "./PresetsBar";
import { TokenTable } from "./TokenTable";
import { TokenDrawer } from "./TokenDrawer";

const prefersReducedMotion =
  typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

export function DashboardScene() {
  const {
    filters,
    selectedTokenAddress,
    selectedPairAddress,
    setSelectedToken,
    pageIndex,
    pageSize,
  } = useAppStore();
  const [tokens, setTokens] = useState<TokenRow[]>([]);
  const [total, setTotal] = useState(0);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchTokens = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getTokens({
        limit: pageSize,
        offset: pageIndex * pageSize,
        ageMaxHours: filters.ageMaxHours ?? undefined,
        minLiquidity: filters.minLiquidity || undefined,
        minVolume: filters.minVolume || undefined,
        dexId: filters.dexId ?? undefined,
        triggerStatus: filters.triggerStatus ?? undefined,
        triggerOutcome: filters.triggerOutcome ?? undefined,
        search: search.trim() || undefined,
        sortBy: "pairCreatedAtMs",
        sortDir: "desc",
      });
      setTokens(res.items ?? []);
      setTotal(res.total ?? 0);
    } catch (e) {
      setTokens([]);
      setTotal(0);
      setError(e instanceof Error ? e.message : "Failed to load tokens");
    } finally {
      setLoading(false);
    }
  }, [pageIndex, pageSize, filters, search]);

  useEffect(() => {
    fetchTokens();
  }, [fetchTokens]);

  const doneCount = tokens.filter((t) => t.triggerStatus === "DONE").length;
  const noDataCount = tokens.filter((t) => t.triggerStatus === "NO_DATA").length;
  const pendingCount = tokens.filter((t) => t.triggerStatus === "PENDING").length;

  return (
    <div className="flex h-screen flex-col">
      <div className="space-y-4 p-4">
        <motion.h1
          className="text-2xl font-semibold text-foreground"
          initial={prefersReducedMotion ? false : { opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.2 }}
        >
          Dashboard
        </motion.h1>

        <div className="flex flex-col gap-3">
          <div className="flex items-center gap-3">
            <div className="relative flex-1 max-w-sm">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Search symbol or address..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-9"
              />
            </div>
          </div>
          <PresetsBar />
        </div>

        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <Card>
            <CardContent className="p-3">
              <p className="text-xs text-muted-foreground">Total rows</p>
              {loading ? (
                <Skeleton className="mt-1 h-7 w-12" />
              ) : (
                <p className="text-xl font-semibold">{total}</p>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-3">
              <p className="text-xs text-muted-foreground">Signals</p>
              {loading ? (
                <Skeleton className="mt-1 h-7 w-12" />
              ) : (
                <p className="text-xl font-semibold">{tokens.length}</p>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-3">
              <p className="text-xs text-muted-foreground">Trigger DONE</p>
              {loading ? (
                <Skeleton className="mt-1 h-7 w-12" />
              ) : (
                <p className="text-xl font-semibold text-green-500">{doneCount}</p>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-3">
              <p className="text-xs text-muted-foreground">NO_DATA / PENDING</p>
              {loading ? (
                <Skeleton className="mt-1 h-7 w-12" />
              ) : (
                <p className="text-xl font-semibold text-muted-foreground">
                  {noDataCount} / {pendingCount}
                </p>
              )}
            </CardContent>
          </Card>
        </div>

        {error && (
          <Card className="border-destructive/50 bg-destructive/5">
            <CardContent className="flex items-center justify-between p-3">
              <p className="text-sm text-destructive">{error}</p>
              <Button variant="outline" size="sm" onClick={() => fetchTokens()}>
                <RefreshCw className="h-4 w-4" />
                Retry
              </Button>
            </CardContent>
          </Card>
        )}

        <div className="flex-1 min-h-0">
          {loading && !tokens.length ? (
            <div className="space-y-2 rounded-lg border border-border p-4">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
            </div>
          ) : error && tokens.length === 0 ? (
            <div className="flex h-64 items-center justify-center rounded-lg border border-border text-muted-foreground">
              Error loading data. Use Retry above.
            </div>
          ) : tokens.length === 0 ? (
            <div className="flex h-64 items-center justify-center rounded-lg border border-border text-muted-foreground">
              No rows match filters.
            </div>
          ) : (
            <>
              <div className="mb-2 flex items-center justify-between text-sm text-muted-foreground">
                <span>
                  Page {pageIndex + 1}, {tokens.length} rows (total {total})
                </span>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={pageIndex === 0}
                    onClick={() => useAppStore.getState().setPagination(Math.max(0, pageIndex - 1), pageSize)}
                  >
                    Prev
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={(pageIndex + 1) * pageSize >= total}
                    onClick={() =>
                      useAppStore.getState().setPagination(pageIndex + 1, pageSize)
                    }
                  >
                    Next
                  </Button>
                </div>
              </div>
              <TokenTable
                data={tokens}
                onRowClick={(row) => setSelectedToken(row.baseAddress, row.pairAddress)}
                selectedTokenAddress={selectedTokenAddress}
              />
            </>
          )}
        </div>
      </div>

      <TokenDrawer
        tokenAddress={selectedTokenAddress}
        pairAddress={selectedPairAddress}
        onClose={() => setSelectedToken(null, null)}
      />
    </div>
  );
}
