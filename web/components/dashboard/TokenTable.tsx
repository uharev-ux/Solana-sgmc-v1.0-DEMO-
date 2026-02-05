"use client";

import { useRef, useMemo } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
  type Row,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { motion } from "framer-motion";
import type { TokenRow } from "@/lib/types";
import { formatUsd, formatPct, formatAge, resolveAgeSec } from "@/lib/utils";
import { cn } from "@/lib/utils";

const ROW_HEIGHT = 48;

const COLUMNS_GRID = "minmax(80px,1fr) minmax(70px,1fr) minmax(70px,1fr) minmax(70px,1fr) minmax(60px,1fr) minmax(50px,1fr) minmax(70px,1fr) minmax(70px,1fr)";

const prefersReducedMotion =
  typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

interface TokenTableProps {
  data: TokenRow[];
  onRowClick: (row: TokenRow) => void;
  selectedTokenAddress: string | null;
}

export function TokenTable({ data, onRowClick, selectedTokenAddress }: TokenTableProps) {
  const columns = useMemo<ColumnDef<TokenRow>[]>(
    () => [
      {
        accessorKey: "baseSymbol",
        header: "Symbol",
        cell: ({ row }) => (
          <span className="font-medium">{row.original.baseSymbol ?? "—"}</span>
        ),
      },
      {
        accessorKey: "priceUsd",
        header: "Price",
        cell: ({ row }) => formatUsd(row.original.priceUsd),
      },
      {
        accessorKey: "liquidityUsd",
        header: "Liq",
        cell: ({ row }) => formatUsd(row.original.liquidityUsd),
      },
      {
        accessorKey: "volumeH24",
        header: "Vol 24h",
        cell: ({ row }) => formatUsd(row.original.volumeH24),
      },
      {
        accessorKey: "priceChangeH24",
        header: "% 24h",
        cell: ({ row }) => {
          const v = row.original.priceChangeH24;
          return (
            <span className={v != null && v < 0 ? "text-red-500" : v != null && v > 0 ? "text-green-500" : ""}>
              {formatPct(v, { showPlus: true })}
            </span>
          );
        },
      },
      {
        id: "age",
        header: "Age",
        cell: ({ row }) =>
          formatAge(resolveAgeSec({ ageSec: row.original.ageSec, pairCreatedAtMs: row.original.pairCreatedAtMs })),
      },
      {
        accessorKey: "dexId",
        header: "DEX",
        cell: ({ row }) => row.original.dexId ?? "—",
      },
      {
        accessorKey: "triggerStatus",
        header: "Trigger",
        cell: ({ row }) => (
          <span
            className={cn(
              row.original.triggerStatus === "DONE" && "text-green-500",
              row.original.triggerStatus === "NO_DATA" && "text-muted-foreground",
              row.original.triggerStatus === "PENDING" && "text-amber-500"
            )}
          >
            {row.original.triggerStatus ?? "—"}
          </span>
        ),
      },
    ],
    []
  );

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  const { rows } = table.getRowModel();
  const parentRef = useRef<HTMLDivElement>(null);

  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 10,
  });

  return (
    <div
      ref={parentRef}
      className="h-[calc(100vh-280px)] min-h-[300px] overflow-auto rounded-lg border border-border"
    >
      {/* Header */}
      <div
        className="sticky top-0 z-10 grid border-b border-border bg-card px-4 py-2 shadow-subtle text-left text-xs font-medium text-muted-foreground"
        style={{ gridTemplateColumns: COLUMNS_GRID }}
      >
        {table.getHeaderGroups().map((hg) =>
          hg.headers.map((h) => (
            <div key={h.id}>{flexRender(h.column.columnDef.header, h.getContext())}</div>
          ))
        )}
      </div>
      {/* Virtualized body */}
      <div
        style={{
          height: `${rowVirtualizer.getTotalSize()}px`,
          position: "relative",
        }}
      >
        {rowVirtualizer.getVirtualItems().map((virtualRow) => {
          const row = rows[virtualRow.index] as Row<TokenRow>;
          const token = row.original;
          const isSelected = selectedTokenAddress === token.baseAddress;
          return (
            <motion.div
              key={row.id}
              data-index={virtualRow.index}
              ref={rowVirtualizer.measureElement}
              className={cn(
                "absolute left-0 top-0 grid w-full cursor-pointer border-b border-border/50 px-4 py-2 text-sm transition-colors",
                isSelected && "bg-accent",
                !isSelected && "hover:bg-muted/50 hover:shadow-glow"
              )}
              style={{
                transform: `translateY(${virtualRow.start}px)`,
                gridTemplateColumns: COLUMNS_GRID,
              }}
              onClick={() => onRowClick(token)}
              initial={prefersReducedMotion ? false : { opacity: 0.95 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.1 }}
            >
              {row.getVisibleCells().map((cell) => (
                <div key={cell.id} className="flex items-center truncate">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </div>
              ))}
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}
