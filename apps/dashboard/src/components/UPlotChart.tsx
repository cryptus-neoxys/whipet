import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

export interface ChartSeries {
  label: string;
  stroke: string;
  width?: number;
  dash?: number[];
  fill?: string;
}

export function UPlotChart({
  data,
  series,
  height = 200,
  yFormat,
}: {
  data: uPlot.AlignedData;
  series: ChartSeries[];
  height?: number;
  yFormat?: (value: number) => string;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const dataRef = useRef(data);
  const seriesRef = useRef(series);
  const yFormatRef = useRef(yFormat);
  dataRef.current = data;
  seriesRef.current = series;
  yFormatRef.current = yFormat;

  const hasData = (data[0]?.length ?? 0) > 1;

  useEffect(() => {
    const container = containerRef.current;
    if (!container || !hasData) return;
    const width = Math.max(320, container.clientWidth || 600);
    const options: uPlot.Options = {
      width,
      height,
      padding: [8, 8, 0, 0],
      legend: { show: true, live: true },
      cursor: { drag: { x: true, y: false } },
      scales: { x: { time: true }, y: { auto: true } },
      axes: [
        {
          stroke: '#7d8ca3',
          grid: { stroke: 'rgba(148, 163, 184, 0.08)' },
          ticks: { stroke: 'rgba(148, 163, 184, 0.18)' },
          font: '11px ui-monospace, monospace',
        },
        {
          stroke: '#7d8ca3',
          grid: { stroke: 'rgba(148, 163, 184, 0.08)' },
          ticks: { stroke: 'rgba(148, 163, 184, 0.18)' },
          font: '11px ui-monospace, monospace',
          size: 58,
          values: (_u, splits) =>
            splits.map((value) => (yFormatRef.current ? yFormatRef.current(value) : String(value))),
        },
      ],
      series: [
        {},
        ...seriesRef.current.map((item) => ({
          label: item.label,
          stroke: item.stroke,
          width: item.width ?? 1.5,
          dash: item.dash,
          fill: item.fill,
          points: { show: false },
        })),
      ],
    };
    const chart = new uPlot(options, dataRef.current, container);
    chartRef.current = chart;
    const observer = new ResizeObserver(() => {
      const nextWidth = container.clientWidth;
      if (nextWidth > 0) chart.setSize({ width: nextWidth, height });
    });
    observer.observe(container);
    return () => {
      observer.disconnect();
      chart.destroy();
      chartRef.current = null;
    };
  }, [hasData, height]);

  useEffect(() => {
    chartRef.current?.setData(data);
  }, [data]);

  return (
    <div className="chart-wrap" ref={containerRef}>
      {!hasData ? <div className="chart-empty">collecting samples…</div> : null}
    </div>
  );
}
