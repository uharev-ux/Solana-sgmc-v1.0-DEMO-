"use client";

import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import { Play, Square, RefreshCw, Copy, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { getStatus, startRunner, stopRunner, runSelfCheck, getSelfCheckReport, getLogsTail } from "@/lib/api";
import type { RunnerStatus, SelfCheckReport } from "@/lib/types";
import { cn } from "@/lib/utils";

const prefersReducedMotion = typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

export function LauncherScene() {
  const [status, setStatus] = useState<RunnerStatus | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [selfCheckReport, setSelfCheckReport] = useState<SelfCheckReport | null>(null);
  const [selfCheckRunId, setSelfCheckRunId] = useState<string | null>(null);
  const [copyReport, setCopyReport] = useState(false);
  const [intervalSec, setIntervalSec] = useState(60);
  const [dbPath, setDbPath] = useState("dexscreener.sqlite");
  const [loading, setLoading] = useState<string | null>(null);
  const [statusError, setStatusError] = useState<string | null>(null);

  const refreshStatus = useCallback(async () => {
    try {
      const s = await getStatus();
      setStatus(s);
      setStatusError(null);
    } catch (e) {
      setStatus({ running: false });
      setStatusError(e instanceof Error ? e.message : "Failed to fetch status");
    }
  }, []);

  const refreshLogs = useCallback(async () => {
    try {
      const lines = await getLogsTail(200);
      setLogs(Array.isArray(lines) ? lines : []);
    } catch {
      setLogs([]);
    }
  }, []);

  useEffect(() => {
    refreshStatus();
    refreshLogs();
    const t = setInterval(refreshStatus, 3000);
    return () => clearInterval(t);
  }, [refreshStatus, refreshLogs]);

  const handleStartOnce = async () => {
    setLoading("once");
    try {
      await startRunner({ mode: "once", dbPath });
      await refreshStatus();
    } finally {
      setLoading(null);
    }
  };

  const handleStartLoop = async () => {
    setLoading("loop");
    try {
      await startRunner({ mode: "loop", intervalSec, dbPath });
      await refreshStatus();
    } finally {
      setLoading(null);
    }
  };

  const handleStop = async () => {
    setLoading("stop");
    try {
      await stopRunner();
      await refreshStatus();
    } finally {
      setLoading(null);
    }
  };

  const handleSelfCheck = async () => {
    setLoading("selfcheck");
    setSelfCheckReport(null);
    try {
      const { runId } = await runSelfCheck();
      setSelfCheckRunId(runId);
      const report = await getSelfCheckReport(runId);
      setSelfCheckReport(report);
    } catch (e) {
      setSelfCheckReport({
        runId: selfCheckRunId ?? "error",
        pass: false,
        steps: [{ name: "Error", pass: false, message: String(e) }],
      });
    } finally {
      setLoading(null);
    }
  };

  const copyReportToClipboard = () => {
    if (!selfCheckReport) return;
    const text = [
      `Self-check ${selfCheckReport.pass ? "PASS" : "FAIL"}`,
      ...selfCheckReport.steps.map((s) => `${s.pass ? "PASS" : "FAIL"} ${s.name}${s.message ? `: ${s.message}` : ""}`),
      selfCheckReport.summary ?? "",
    ].join("\n");
    void navigator.clipboard.writeText(text);
    setCopyReport(true);
    setTimeout(() => setCopyReport(false), 2000);
  };

  return (
    <div className="space-y-6 p-6">
      <motion.h1
        className="text-2xl font-semibold text-foreground"
        initial={prefersReducedMotion ? false : { opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.2 }}
      >
        Launcher
      </motion.h1>

      {/* Runner controls */}
      <Card>
        <CardHeader>
          <CardTitle>Runner</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-center gap-3">
            <Button
              variant="default"
              size="sm"
              onClick={handleStartOnce}
              disabled={status?.running ?? false}
            >
              <Play className="h-4 w-4" />
              Start once
            </Button>
            <Button
              variant="default"
              size="sm"
              onClick={handleStartLoop}
              disabled={status?.running ?? false}
            >
              <Play className="h-4 w-4" />
              Start loop
            </Button>
            <Button
              variant="destructive"
              size="sm"
              onClick={handleStop}
              disabled={!status?.running}
            >
              <Square className="h-4 w-4" />
              Stop
            </Button>
            <div className="flex items-center gap-2">
              <Label className="text-muted-foreground">Interval (s)</Label>
              <Input
                type="number"
                min={1}
                value={intervalSec}
                onChange={(e) => setIntervalSec(Number(e.target.value) || 60)}
                className="w-20"
              />
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-muted-foreground">DB path</Label>
              <Input
                value={dbPath}
                onChange={(e) => setDbPath(e.target.value)}
                className="w-48"
              />
            </div>
          </div>
          {loading && (
            <p className="text-sm text-muted-foreground">Loading: {loading}</p>
          )}
        </CardContent>
      </Card>

      {statusError && (
        <Card className="border-destructive/50 bg-destructive/5">
          <CardContent className="flex items-center justify-between p-3">
            <p className="text-sm text-destructive">{statusError}</p>
            <Button variant="outline" size="sm" onClick={() => refreshStatus()}>
              <RefreshCw className="h-4 w-4" />
              Retry
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Status */}
      <Card>
        <CardHeader>
          <CardTitle>Status</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1 text-sm">
          <p><span className="text-muted-foreground">Worker:</span> {status?.running ? "Running" : "Stopped"}</p>
          <p><span className="text-muted-foreground">Mode:</span> {status?.mode ?? "—"}</p>
          <p><span className="text-muted-foreground">Last cycle:</span>{" "}
            {status?.lastCycleFinishedAt
              ? new Date(status.lastCycleFinishedAt).toLocaleString()
              : "—"}
          </p>
          <p><span className="text-muted-foreground">DB path:</span> {status?.dbPath ?? "—"}</p>
          {status?.lastError && (
            <p className="text-destructive">Last error: {status.lastError}</p>
          )}
        </CardContent>
      </Card>

      {/* Self-check */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Self-check</CardTitle>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={handleSelfCheck} disabled={!!loading}>
              <RefreshCw className="h-4 w-4" />
              Run Self-check
            </Button>
            {selfCheckReport && (
              <Button variant="ghost" size="sm" onClick={copyReportToClipboard}>
                {copyReport ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                {copyReport ? "Copied" : "Copy report"}
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {loading === "selfcheck" ? (
            <div className="space-y-2">
              <Skeleton className="h-5 w-24" />
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
            </div>
          ) : selfCheckReport ? (
            <div className="space-y-2">
              <p className={cn("font-medium", selfCheckReport.pass ? "text-green-500" : "text-destructive")}>
                {selfCheckReport.pass ? "PASS" : "FAIL"}
              </p>
              <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
                {selfCheckReport.steps.map((s, i) => (
                  <li key={i}>
                    {s.pass ? "PASS" : "FAIL"} — {s.name}
                    {s.message != null && s.message !== "" && `: ${s.message}`}
                  </li>
                ))}
              </ul>
              {selfCheckReport.summary && (
                <p className="text-sm text-muted-foreground">{selfCheckReport.summary}</p>
              )}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Run self-check to see report.</p>
          )}
        </CardContent>
      </Card>

      {/* Log tail */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Log tail (last 200 lines)</CardTitle>
          <Button variant="ghost" size="sm" onClick={refreshLogs}>
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </CardHeader>
        <CardContent>
          <pre className="max-h-64 overflow-auto rounded border border-border bg-muted/30 p-3 text-xs text-muted-foreground">
            {logs.length === 0 ? "No logs." : logs.join("\n")}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}
