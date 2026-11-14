import type { ApiError, BatchOrderAck, HealthView, OrderAck, OrderRequest, StateView } from './types';

async function json<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const body = (await response.json()) as ApiError;
      if (body.error) message = body.error;
    } catch {
      // Keep the status text when the body is not JSON.
    }
    throw new Error(message);
  }
  return (await response.json()) as T;
}

export async function fetchState(): Promise<StateView> {
  return json<StateView>(await fetch('/api/state', { headers: { accept: 'application/json' } }));
}

export async function fetchHealth(): Promise<HealthView> {
  return json<HealthView>(await fetch('/api/health', { headers: { accept: 'application/json' } }));
}

export async function submitOrder(order: OrderRequest): Promise<OrderAck> {
  return json<OrderAck>(
    await fetch('/api/orders', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(order),
    }),
  );
}

export async function submitOrderBatch(orders: OrderRequest[]): Promise<BatchOrderAck> {
  return json<BatchOrderAck>(
    await fetch('/api/orders/batch?acks=full', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ orders }),
    }),
  );
}
