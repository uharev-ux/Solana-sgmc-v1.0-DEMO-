import { create } from "zustand";
import type { FiltersState } from "@/lib/types";

const DEFAULT_FILTERS: FiltersState = {
  ageMaxHours: 24,
  minLiquidity: 0,
  minVolume: 0,
  dexId: null,
  triggerStatus: null,
  triggerOutcome: null,
};

interface Preset {
  id: string;
  name: string;
  filters: FiltersState;
}

interface AppState {
  filters: FiltersState;
  presets: Preset[];
  selectedTokenAddress: string | null;
  selectedPairAddress: string | null;
  pageIndex: number;
  pageSize: number;
  setFilters: (f: Partial<FiltersState>) => void;
  applyPreset: (id: string) => void;
  savePreset: (name: string) => void;
  setSelectedToken: (tokenAddress: string | null, pairAddress: string | null) => void;
  setPagination: (pageIndex: number, pageSize: number) => void;
  loadPresets: () => void;
  savePresets: () => void;
}

const PRESETS_KEY = "dexscanner-presets";
const DEFAULT_PAGE_SIZE = 200;

export const useAppStore = create<AppState>((set, get) => ({
  filters: DEFAULT_FILTERS,
  presets: [{ id: "default", name: "Default (age ≤ 24h)", filters: { ...DEFAULT_FILTERS } }],
  selectedTokenAddress: null,
  selectedPairAddress: null,
  pageIndex: 0,
  pageSize: DEFAULT_PAGE_SIZE,

  setFilters: (f) =>
    set((s) => ({
      filters: { ...s.filters, ...f },
    })),

  applyPreset: (id) => {
    const preset = get().presets.find((p) => p.id === id);
    if (preset) set({ filters: { ...preset.filters } });
  },

  savePreset: (name) => {
    const { filters, presets } = get();
    const id = `preset_${Date.now()}`;
    const next = [...presets, { id, name, filters: { ...filters } }];
    set({ presets: next });
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(PRESETS_KEY, JSON.stringify(next));
    }
  },

  setSelectedToken: (tokenAddress, pairAddress) =>
    set({ selectedTokenAddress: tokenAddress ?? null, selectedPairAddress: pairAddress ?? null }),

  setPagination: (pageIndex, pageSize) => set({ pageIndex, pageSize }),

  loadPresets: () => {
    if (typeof localStorage === "undefined") return;
    try {
      const raw = localStorage.getItem(PRESETS_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as Preset[];
        if (Array.isArray(parsed) && parsed.length) {
          const defaultPreset = get().presets.find((p) => p.id === "default");
          const others = parsed.filter((p) => p.id !== "default");
          set({
            presets: defaultPreset ? [defaultPreset, ...others] : others,
          });
        }
      }
    } catch {
      // ignore
    }
  },

  savePresets: () => {
    const next = get().presets;
    if (typeof localStorage !== "undefined") localStorage.setItem(PRESETS_KEY, JSON.stringify(next));
  },
}));
