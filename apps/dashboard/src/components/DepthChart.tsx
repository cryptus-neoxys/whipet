import { useEffect, useRef } from 'react';
import { formatPrice } from '../lib/format';
import type { BookView } from '../lib/types';

interface Point {
  price: number;
  cumulative: number;
}

function cumulative(levels: { price_cents: number; qty: number }[]): Point[] {
  let total = 0;
  return levels.map((level) => {
    total += level.qty;
    return { price: level.price_cents, cumulative: total };
  });
}

export function DepthChart({ book }: { book: BookView | null }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const draw = () => {
      const rect = canvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      const ctx = canvas.getContext('2d');
      if (!ctx) return;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, rect.width, rect.height);

      const bids = book ? cumulative(book.bids) : [];
      const asks = book ? cumulative(book.asks) : [];
      const prices = [...bids, ...asks].map((point) => point.price);
      if (prices.length === 0) {
        ctx.fillStyle = 'rgba(148, 163, 184, 0.55)';
        ctx.font = '12px ui-monospace, monospace';
        ctx.textAlign = 'center';
        ctx.fillText('depth appears with the first resting orders', rect.width / 2, rect.height / 2);
        return;
      }

      const minPrice = Math.min(...prices);
      const maxPrice = Math.max(...prices);
      const span = Math.max(1, maxPrice - minPrice);
      const maxDepth = Math.max(
        1,
        ...bids.map((point) => point.cumulative),
        ...asks.map((point) => point.cumulative),
      );
      const pad = { top: 16, right: 12, bottom: 22, left: 12 };
      const plotWidth = rect.width - pad.left - pad.right;
      const plotHeight = rect.height - pad.top - pad.bottom;
      const x = (price: number) => pad.left + ((price - minPrice) / span) * plotWidth;
      const y = (depth: number) => pad.top + plotHeight - (depth / maxDepth) * plotHeight;

      ctx.strokeStyle = 'rgba(148, 163, 184, 0.12)';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const lineY = pad.top + (plotHeight / 4) * i;
        ctx.beginPath();
        ctx.moveTo(pad.left, lineY);
        ctx.lineTo(rect.width - pad.right, lineY);
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

      if (book?.mid_cents !== null && book?.mid_cents !== undefined) {
        ctx.strokeStyle = 'rgba(226, 232, 240, 0.35)';
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(x(book.mid_cents), pad.top);
        ctx.lineTo(x(book.mid_cents), rect.height - pad.bottom);
        ctx.stroke();
        ctx.setLineDash([]);
      }

      ctx.fillStyle = 'rgba(148, 163, 184, 0.75)';
      ctx.font = '11px ui-monospace, monospace';
      ctx.textAlign = 'left';
      ctx.fillText(formatPrice(minPrice, book?.symbol), pad.left, rect.height - 6);
      ctx.textAlign = 'right';
      ctx.fillText(formatPrice(maxPrice, book?.symbol), rect.width - pad.right, rect.height - 6);
      ctx.textAlign = 'left';
      ctx.fillText(`depth ${maxDepth}`, pad.left, 12);
    };

    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(canvas);
    return () => observer.disconnect();
  }, [book]);

  const label = book
    ? `Cumulative market depth for ${book.symbol}. Best bid ${formatPrice(
        book.best_bid_cents,
        book.symbol,
      )}, best ask ${formatPrice(book.best_ask_cents, book.symbol)}.`
    : 'Cumulative market depth. No book snapshot yet.';

  return <canvas ref={canvasRef} className="depth-chart" role="img" aria-label={label} />;
}
