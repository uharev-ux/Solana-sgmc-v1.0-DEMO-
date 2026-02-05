"use client";

import { useState, useEffect } from "react";
import { getHealth, isMockMode } from "@/lib/api";
import { cn } from "@/lib/utils";

const POLL_MS = 5000;

export function ConnectionBanner() {
  const [online, setOnline] = useState<boolean | null>(null);
  const mock = typeof window !== "undefined" && isMockMode();

  useEffect(() => {
    if (mock) {
      setOnline(true);
      return;
    }
    let mounted = true;
    const check = async () => {
      try {
        await getHealth();
        if (mounted) setOnline(true);
      } catch {
        if (mounted) setOnline(false);
      }
    };
    check();
    const id = setInterval(check, POLL_MS);
    return () => {
      mounted = false;
      clearInterval(id);
    };
  }, [mock]);

  if (online === null && !mock) return null;

  return (
    <div
      className={cn(
        "border-b px-4 py-1.5 text-center text-xs font-medium",
        mock
          ? "border-amber-500/50 bg-amber-500/10 text-amber-600 dark:text-amber-400"
          : online
            ? "border-green-500/30 bg-green-500/5 text-green-600 dark:text-green-400"
            : "border-red-500/30 bg-red-500/10 text-red-600 dark:text-red-400"
      )}
    >
      {mock ? "MOCK MODE" : online ? "ONLINE" : "OFFLINE"}
    </div>
  );
}
