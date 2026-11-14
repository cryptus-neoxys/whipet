import { createContext, useContext } from 'react';
import type { ReactNode } from 'react';
import { useExchangeFeed } from '../hooks/useExchangeFeed';
import type { ExchangeFeed } from '../hooks/useExchangeFeed';

const FeedContext = createContext<ExchangeFeed | null>(null);

export function FeedProvider({ children }: { children: ReactNode }) {
  const feed = useExchangeFeed();
  return <FeedContext.Provider value={feed}>{children}</FeedContext.Provider>;
}

export function useFeed(): ExchangeFeed {
  const feed = useContext(FeedContext);
  if (!feed) throw new Error('useFeed must be used inside FeedProvider');
  return feed;
}
