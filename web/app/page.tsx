"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Rocket, LayoutDashboard } from "lucide-react";
import { ConnectionBanner } from "@/components/ConnectionBanner";
import { LauncherScene } from "@/components/launcher/LauncherScene";
import { DashboardScene } from "@/components/dashboard/DashboardScene";
import { cn } from "@/lib/utils";

type Tab = "launcher" | "dashboard";

export default function Home() {
  const [tab, setTab] = useState<Tab>("dashboard");

  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-20 border-b border-border bg-card/80 shadow-subtle backdrop-blur-sm">
        <div className="flex items-center gap-6 px-6 py-3">
          <h1 className="text-lg font-semibold text-foreground">DexScanner — Solana Dip Scanner</h1>
          <nav className="flex gap-1">
            <TabButton
              active={tab === "launcher"}
              onClick={() => setTab("launcher")}
              icon={<Rocket className="h-4 w-4" />}
            >
              Launcher
            </TabButton>
            <TabButton
              active={tab === "dashboard"}
              onClick={() => setTab("dashboard")}
              icon={<LayoutDashboard className="h-4 w-4" />}
            >
              Dashboard
            </TabButton>
          </nav>
        </div>
      </header>
      <ConnectionBanner />

      <main>
        {tab === "launcher" && <LauncherScene />}
        {tab === "dashboard" && <DashboardScene />}
      </main>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  icon,
  children,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors",
        active
          ? "bg-accent text-accent-foreground shadow-subtle"
          : "text-muted-foreground hover:bg-muted hover:text-foreground"
      )}
    >
      {icon}
      {children}
    </button>
  );
}
