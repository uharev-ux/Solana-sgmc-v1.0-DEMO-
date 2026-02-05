import { z } from "zod";

export const healthSchema = z.object({
  ok: z.boolean(),
  version: z.string().optional(),
});

export const runnerStatusSchema = z.object({
  running: z.boolean(),
  mode: z.enum(["once", "loop"]).optional(),
  intervalSec: z.number().optional(),
  dbPath: z.string().optional(),
  lastCycleStartedAt: z.number().optional(),
  lastCycleFinishedAt: z.number().optional(),
  lastError: z.string().optional().nullable(),
  lastErrorAt: z.number().optional().nullable(),
});

export const selfCheckStepSchema = z.object({
  name: z.string(),
  pass: z.boolean(),
  message: z.string().optional(),
});

export const selfCheckReportSchema = z.object({
  runId: z.string(),
  pass: z.boolean(),
  steps: z.array(selfCheckStepSchema),
  summary: z.string().optional(),
});

export const tokenRowSchema = z.object({
  pairAddress: z.string(),
  baseSymbol: z.string(),
  baseAddress: z.string(),
  quoteSymbol: z.string().optional().nullable(),
  priceUsd: z.number().nullable(),
  liquidityUsd: z.number().nullable(),
  volumeH24: z.number().nullable(),
  priceChangeH24: z.number().nullable(),
  txnsH24Buys: z.number().nullable(),
  txnsH24Sells: z.number().nullable(),
  ageSec: z.number().nullable().optional(),
  pairCreatedAtMs: z.number().nullable(),
  dexId: z.string().nullable(),
  url: z.string().nullable(),
  triggerOutcome: z.enum(["TP1_FIRST", "SL_FIRST", "NEITHER"]).nullable().optional(),
  triggerStatus: z.enum(["DONE", "NO_DATA", "PENDING"]).nullable().optional(),
});

export const tokenDetailSchema = tokenRowSchema.extend({
  chainId: z.string().nullable().optional(),
  volumeM5: z.number().nullable().optional(),
  volumeH1: z.number().nullable().optional(),
  volumeH6: z.number().nullable().optional(),
  priceChangeM5: z.number().nullable().optional(),
  priceChangeH1: z.number().nullable().optional(),
  priceChangeH6: z.number().nullable().optional(),
  fdv: z.number().nullable().optional(),
  marketCap: z.number().nullable().optional(),
});

/** timeSec = UNIX seconds */
export const candlePointSchema = z.object({
  timeSec: z.number(),
  open: z.number(),
  high: z.number(),
  low: z.number(),
  close: z.number(),
});

export const triggerDetailSchema = z.object({
  outcome: z.enum(["TP1_FIRST", "SL_FIRST", "NEITHER"]).nullable(),
  status: z.enum(["DONE", "NO_DATA", "PENDING"]),
  tp1HitTs: z.number().nullable().optional(),
  slHitTs: z.number().nullable().optional(),
  tp1Price: z.number().nullable().optional(),
  slPrice: z.number().nullable().optional(),
  mfePct: z.number().nullable().optional(),
  maePct: z.number().nullable().optional(),
  buHitAfterTp1: z.number().nullable().optional(),
  postTp1MaxPct: z.number().nullable().optional(),
});

export const tokensResponseSchema = z.object({
  items: z.array(tokenRowSchema),
  total: z.number(),
  page: z.number().optional(),
  pageSize: z.number().optional(),
});

export const startRunnerSchema = z.object({
  mode: z.enum(["once", "loop"]),
  intervalSec: z.number().min(1).optional(),
  dbPath: z.string().optional(),
});

export type HealthResponseZ = z.infer<typeof healthSchema>;
export type RunnerStatusZ = z.infer<typeof runnerStatusSchema>;
export type SelfCheckReportZ = z.infer<typeof selfCheckReportSchema>;
export type TokenRowZ = z.infer<typeof tokenRowSchema>;
export type TokensResponseZ = z.infer<typeof tokensResponseSchema>;
