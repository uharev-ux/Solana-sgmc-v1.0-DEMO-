"use client";

import { useEffect, useRef } from "react";
import { createChart, type IChartApi, type ISeriesApi, type CandlestickData, type UTCTimestamp } from "lightweight-charts";
import type { CandlePoint } from "@/lib/types";

interface MiniChartProps {
  data: CandlePoint[];
  height?: number;
  className?: string;
  positiveColor?: string;
  negativeColor?: string;
}

export function MiniChart({
  data,
  height = 80,
  className,
  positiveColor = "rgba(34, 197, 94, 0.8)",
  negativeColor = "rgba(239, 68, 68, 0.8)",
}: MiniChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    const chart = createChart(containerRef.current, {
      height,
      layout: { background: { color: "transparent" }, textColor: "hsl(220, 8%, 55%)" },
      grid: { vertLines: { visible: false }, horzLines: { color: "hsl(220, 10%, 18%)" } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.1, bottom: 0.1 } },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
      crosshair: { vertLine: { visible: false }, horzLine: { visible: false } },
    });

    const candlestickSeries = chart.addCandlestickSeries({
      upColor: positiveColor,
      downColor: negativeColor,
      borderDownColor: negativeColor,
      borderUpColor: positiveColor,
    });

    const mapped: CandlestickData[] = data.map((d) => ({
      time: d.timeSec as UTCTimestamp,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));

    candlestickSeries.setData(mapped);
    chart.timeScale().fitContent();

    chartRef.current = chart;
    seriesRef.current = candlestickSeries;

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [data, height, positiveColor, negativeColor]);

  if (data.length === 0) {
    return (
      <div className={className} style={{ height: `${height}px` }}>
        <div className="flex h-full items-center justify-center text-xs text-muted-foreground">
          No data
        </div>
      </div>
    );
  }

  return <div ref={containerRef} className={className} style={{ height: `${height}px` }} />;
}
