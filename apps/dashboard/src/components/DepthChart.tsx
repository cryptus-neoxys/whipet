import { useCallback, useEffect, useMemo, useRef } from 'react';
import { formatPrice } from '../lib/format';
import type { BookView, LevelView } from '../lib/types';

interface Point {
  price: number;
  cumulative: number;
}

interface DepthGeometry {
  book: BookView | null;
  bids: Point[];
  asks: Point[];
  minPrice: number;
  maxPrice: number;
  maxDepth: number;
  empty: boolean;
}

const NO_POINTS: Point[] = [];

const EMPTY_GEOMETRY: DepthGeometry = {
  book: null,
  bids: NO_POINTS,
  asks: NO_POINTS,
  minPrice: 0,
  maxPrice: 0,
  maxDepth: 0,
  empty: true,
};

/** Cumulative depth per level. Allocates one array per book snapshot. */
function cumulative(levels: LevelView[]): Point[] {
  const points: Point[] = new Array(levels.length);
  let total = 0;
  for (let index = 0; index < levels.length; index += 1) {
    const level = levels[index];
    total += level.qty;
    points[index] = { price: level.price_cents, cumulative: total };
  }
  return points;
}

/**
 * Derive everything that depends on the book once per snapshot. Redraws caused by
 * resizes reuse this instead of rebuilding the cumulative arrays and rescans.
 */
function buildGeometry(book: BookView | null): DepthGeometry {
  if (!book) return EMPTY_GEOMETRY;

  const bids = cumulative(book.bids);
  const asks = cumulative(book.asks);
  if (bids.length === 0 && asks.length === 0) return EMPTY_GEOMETRY;

  let minPrice = Number.POSITIVE_INFINITY;
  let maxPrice = Number.NEGATIVE_INFINITY;
  let maxDepth = 0;
  for (let index = 0; index < bids.length; index += 1) {
    const point = bids[index];
    if (point.price < minPrice) minPrice = point.price;
    if (point.price > maxPrice) maxPrice = point.price;
    if (point.cumulative > maxDepth) maxDepth = point.cumulative;
  }
  for (let index = 0; index < asks.length; index += 1) {
    const point = asks[index];
    if (point.price < minPrice) minPrice = point.price;
    if (point.price > maxPrice) maxPrice = point.price;
    if (point.cumulative > maxDepth) maxDepth = point.cumulative;
  }

  return { book, bids, asks, minPrice, maxPrice, maxDepth, empty: false };
}

export function DepthChart({ book }: { book: BookView | null }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  // CSS-pixel size of the canvas, refreshed only by the ResizeObserver.
  const sizeRef = useRef({ width: 0, height: 0 });
  const geometry = useMemo(() => buildGeometry(book), [book]);
  const geometryRef = useRef(geometry);
  const frameRef = useRef<number | null>(null);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const { width, height } = sizeRef.current;
    const dpr = window.devicePixelRatio || 1;
    const pixelWidth = Math.max(1, Math.floor(width * dpr));
    const pixelHeight = Math.max(1, Math.floor(height * dpr));
    // Assigning width/height reallocates the backing store and resets context
    // state, so only do it when the pixel size actually changed.
    if (canvas.width !== pixelWidth) canvas.width = pixelWidth;
    if (canvas.height !== pixelHeight) canvas.height = pixelHeight;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    if (width <= 0 || height <= 0) return;

    const current = geometryRef.current;
    if (current.empty) {
      ctx.fillStyle = 'rgba(148, 163, 184, 0.55)';
      ctx.font = '12px ui-monospace, monospace';
      ctx.textAlign = 'center';
      ctx.fillText('depth appears with the first resting orders', width / 2, height / 2);
      return;
    }

    const { bids, asks, minPrice, maxPrice, book: snapshot } = current;
    const span = Math.max(1, maxPrice - minPrice);
    const maxDepth = Math.max(1, current.maxDepth);
    const pad = { top: 16, right: 12, bottom: 22, left: 12 };
    const plotWidth = width - pad.left - pad.right;
    const plotHeight = height - pad.top - pad.bottom;
    const x = (price: number) => pad.left + ((price - minPrice) / span) * plotWidth;
    const y = (depth: number) => pad.top + plotHeight - (depth / maxDepth) * plotHeight;

    ctx.strokeStyle = 'rgba(148, 163, 184, 0.12)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i += 1) {
      const lineY = pad.top + (plotHeight / 4) * i;
      ctx.beginPath();
      ctx.moveTo(pad.left, lineY);
      ctx.lineTo(width - pad.right, lineY);
      ctx.stroke();
    }

    const drawSide = (points: Point[], color: string, fill: string) => {
      if (points.length === 0) return;
      ctx.beginPath();
      ctx.moveTo(x(points[0].price), y(0));
      for (const point of points) {
        ctx.lineTo(x(point.price), y(point.cumulative));
      }
      ctx.lineTo(x(points[points.length - 1].price), y(0));
      ctx.closePath();
      ctx.fillStyle = fill;
      ctx.fill();
      ctx.beginPath();
      for (let index = 0; index < points.length; index += 1) {
        const point = points[index];
        if (index === 0) ctx.moveTo(x(point.price), y(point.cumulative));
        else ctx.lineTo(x(point.price), y(point.cumulative));
      }
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.5;
      ctx.stroke();
    };

    drawSide(bids, '#34d399', 'rgba(52, 211, 153, 0.16)');
    drawSide(asks, '#fb7185', 'rgba(251, 113, 133, 0.14)');

    const mid = snapshot?.mid_cents;
    if (mid !== null && mid !== undefined) {
      ctx.strokeStyle = 'rgba(226, 232, 240, 0.35)';
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(x(mid), pad.top);
      ctx.lineTo(x(mid), height - pad.bottom);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    ctx.fillStyle = 'rgba(148, 163, 184, 0.75)';
    ctx.font = '11px ui-monospace, monospace';
    ctx.textAlign = 'left';
    ctx.fillText(formatPrice(minPrice, snapshot?.symbol), pad.left, height - 6);
    ctx.textAlign = 'right';
    ctx.fillText(formatPrice(maxPrice, snapshot?.symbol), width - pad.right, height - 6);
    ctx.textAlign = 'left';
    ctx.fillText(`depth ${maxDepth}`, pad.left, 12);
  }, []);

  const scheduleDraw = useCallback(() => {
    if (frameRef.current !== null) return;
    frameRef.current = window.requestAnimationFrame(() => {
      frameRef.current = null;
      draw();
    });
  }, [draw]);

  // Publish the latest geometry and coalesce this frame's redraw into one rAF.
  useEffect(() => {
    geometryRef.current = geometry;
    scheduleDraw();
  }, [geometry, scheduleDraw]);

  // Mount once: track the canvas size and redraw only when it changes.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const measure = () => {
      const rect = canvas.getBoundingClientRect();
      const size = sizeRef.current;
      if (size.width === rect.width && size.height === rect.height) return;
      size.width = rect.width;
      size.height = rect.height;
      scheduleDraw();
    };

    measure();
    // Covers the case where the size was already recorded (e.g. a StrictMode
    // remount) but the canvas content was never drawn.
    scheduleDraw();

    const observer = new ResizeObserver(measure);
    observer.observe(canvas);
    return () => {
      observer.disconnect();
      if (frameRef.current !== null) {
        window.cancelAnimationFrame(frameRef.current);
        frameRef.current = null;
      }
    };
  }, [scheduleDraw]);

  const label = book
    ? `Cumulative market depth for ${book.symbol}. Best bid ${formatPrice(
        book.best_bid_cents,
        book.symbol,
      )}, best ask ${formatPrice(book.best_ask_cents, book.symbol)}.`
    : 'Cumulative market depth. No book snapshot yet.';

  return <canvas ref={canvasRef} className="depth-chart" role="img" aria-label={label} />;
}
