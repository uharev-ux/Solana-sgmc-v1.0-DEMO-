"use client";

import { useEffect, useState } from "react";
import { useAppStore } from "@/store/use-app-store";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
export function PresetsBar() {
  const { filters, presets, setFilters, applyPreset, savePreset, loadPresets } = useAppStore();
  useEffect(() => {
    loadPresets();
  }, [loadPresets]);

  const [presetName, setPresetName] = useState("");
  const activePresetId =
    presets.find(
      (p) =>
        (p.filters.ageMaxHours ?? null) === (filters.ageMaxHours ?? null) &&
        p.filters.minLiquidity === filters.minLiquidity &&
        p.filters.minVolume === filters.minVolume &&
        (p.filters.triggerStatus ?? null) === (filters.triggerStatus ?? null) &&
        (p.filters.triggerOutcome ?? null) === (filters.triggerOutcome ?? null)
    )?.id ?? "default";

  const handleSavePreset = () => {
    if (presetName.trim()) {
      savePreset(presetName.trim());
      setPresetName("");
    }
  };

  const age24On = filters.ageMaxHours === 24;
  const toggleAge24 = () => setFilters({ ageMaxHours: age24On ? null : 24 });

  const tp1On = filters.triggerOutcome === "TP1_FIRST";
  const toggleTp1 = () => setFilters({ triggerOutcome: tp1On ? null : "TP1_FIRST" });

  const slOn = filters.triggerOutcome === "SL_FIRST";
  const toggleSl = () => setFilters({ triggerOutcome: slOn ? null : "SL_FIRST" });

  const pendingOn = filters.triggerStatus === "PENDING";
  const togglePending = () => setFilters({ triggerStatus: pendingOn ? null : "PENDING" });

  return (
    <div className="flex flex-wrap items-center gap-4 rounded-lg border border-border bg-card p-3">
      <div className="flex items-center gap-2">
        <Label className="text-muted-foreground text-sm">Preset</Label>
        <Select value={activePresetId} onValueChange={applyPreset}>
          <SelectTrigger className="w-48">
            <SelectValue placeholder="Default (age ≤ 24h)" />
          </SelectTrigger>
          <SelectContent>
            {presets.map((p) => (
              <SelectItem key={p.id} value={p.id}>
                {p.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="flex items-center gap-2">
        <Input
          placeholder="Preset name"
          value={presetName}
          onChange={(e) => setPresetName(e.target.value)}
          className="w-36"
        />
        <Button variant="outline" size="sm" onClick={handleSavePreset}>
          Save preset
        </Button>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-muted-foreground text-xs">Quick:</span>
        <Button
          variant={age24On ? "default" : "outline"}
          size="sm"
          onClick={toggleAge24}
        >
          Age ≤ 24h
        </Button>
        <Button
          variant={tp1On ? "default" : "outline"}
          size="sm"
          onClick={toggleTp1}
        >
          TP1_FIRST
        </Button>
        <Button
          variant={slOn ? "default" : "outline"}
          size="sm"
          onClick={toggleSl}
        >
          SL_FIRST
        </Button>
        <Button
          variant={pendingOn ? "default" : "outline"}
          size="sm"
          onClick={togglePending}
        >
          PENDING
        </Button>
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-muted-foreground text-sm">Age ≤ (h)</Label>
        <Input
          type="number"
          min={0}
          value={filters.ageMaxHours ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            setFilters({ ageMaxHours: v === "" ? null : Number(v) || 24 });
          }}
          placeholder="all"
          className="w-16"
        />
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-muted-foreground text-sm">Min Liq</Label>
        <Input
          type="number"
          min={0}
          value={filters.minLiquidity}
          onChange={(e) => setFilters({ minLiquidity: Number(e.target.value) || 0 })}
          className="w-24"
        />
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-muted-foreground text-sm">Min Vol</Label>
        <Input
          type="number"
          min={0}
          value={filters.minVolume}
          onChange={(e) => setFilters({ minVolume: Number(e.target.value) || 0 })}
          className="w-24"
        />
      </div>
    </div>
  );
}
