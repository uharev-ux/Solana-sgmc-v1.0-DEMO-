# DexScanner Dashboard — Solana DexScreener Dip Scanner UI

Modern Web UI (Dashboard + Launcher) for the Solana DexScreener Dip Scanner. Backend is Python + SQLite; this app talks to it via HTTP API (or runs with mock data when API is not configured).

## Stack

- **Next.js 14** (App Router) + TypeScript
- **Tailwind CSS** + design tokens (dark, minimal)
- **shadcn-style** UI (Button, Card, Input, Label, Select)
- **TanStack Table v8** + **TanStack Virtual** (virtualized table)
- **Framer Motion** (respects `prefers-reduced-motion`)
- **TradingView Lightweight Charts** (mini candlestick charts)
- **Zustand** (global state: filters, presets, selected token)
- **Zod** (API response validation)

## Scenes

1. **Launcher** — Start once / Start loop / Stop, Run Self-check, status, log tail, self-check report (PASS/FAIL, copy).
2. **Dashboard** — Search, presets, filters, KPI cards (total rows, signals, trigger DONE/NO_DATA/PENDING), virtualized token table, row click → TokenDrawer.
3. **TokenDrawer** — Symbol, address (copy), stats, badges (dex, trigger outcome), mini 5m/15m charts, trigger details (TP1_FIRST / SL_FIRST / NEITHER), GMGN / Dexscreener links.
4. **Presets** — Save/apply filter presets (localStorage). Default: age ≤ 24h.

## API contract (backend to implement)

- `GET /api/health`
- `GET /api/status`
- `POST /api/runner/start` — body: `{ mode, intervalSec?, dbPath? }`
- `POST /api/runner/stop`
- `POST /api/selfcheck/run` — returns `{ runId }`
- `GET /api/selfcheck/{runId}` — self-check report
- `GET /api/tokens` — query: `limit`, `offset`, `sortBy`, `sortDir`, `ageMaxHours`, `minLiquidity`, `minVolume`, `dexId`, `triggerStatus`, `triggerOutcome`, `search`
- `GET /api/token/{token}` — token detail
- `GET /api/token/{token}/candles?tf=5m|15m` — candle array
- `GET /api/token/{token}/trigger` — trigger evaluation
- `GET /api/logs/tail?lines=200` — log lines

## Run locally

### 1. Install dependencies

```bash
cd web
npm install
```

### 2. Environment (optional)

Create `.env.local`:

```bash
# Backend API base URL (no trailing slash).
# Omit to use mock data (dev only).
NEXT_PUBLIC_API_BASE_URL=http://localhost:5000
```

### 3. Dev server

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000). Without `NEXT_PUBLIC_API_BASE_URL`, the UI uses **mock data** and all API calls are handled in-memory.

### 4. Build for production

```bash
npm run build
npm start
```

Set `NEXT_PUBLIC_API_BASE_URL` to your backend URL in production.

## Project structure (App Router)

```
web/
├── app/
│   ├── globals.css       # Design tokens (colors, radius, shadows)
│   ├── layout.tsx
│   └── page.tsx          # Tabs: Launcher | Dashboard
├── components/
│   ├── launcher/
│   │   └── LauncherScene.tsx
│   ├── dashboard/
│   │   ├── DashboardScene.tsx
│   │   ├── TokenTable.tsx    # Virtualized table
│   │   ├── TokenDrawer.tsx   # Right-side drawer
│   │   ├── MiniChart.tsx     # Lightweight Charts
│   │   └── PresetsBar.tsx
│   └── ui/
│       ├── button.tsx
│       ├── card.tsx
│       ├── input.tsx
│       ├── label.tsx
│       └── select.tsx
├── lib/
│   ├── api.ts            # HTTP client + mock fallback
│   ├── types.ts          # API types
│   ├── schemas.ts        # Zod schemas
│   ├── mock-api.ts       # Dev mock (when no API URL)
│   └── utils.ts          # cn, formatUsd, formatAge, formatPct
├── store/
│   └── use-app-store.ts  # Zustand: filters, presets, selectedTokenId
├── .env.example
├── package.json
├── tailwind.config.ts
└── README.md
```

## Design tokens (globals.css + Tailwind)

- **Background**: dark gray (`--background`, `--card`, `--muted`)
- **Borders**: thin, `--border`
- **Shadows**: `--shadow-subtle`, `--shadow-glow` (soft)
- **Transitions**: `--transition-duration` (150ms)
- **Reduced motion**: animations minimized when `prefers-reduced-motion: reduce`

Numbers: `$`, K/M/B for large values; age in `Xh` / `Xm`.
