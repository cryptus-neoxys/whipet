import { createRootRoute, createRoute, createRouter, Outlet } from '@tanstack/react-router';
import { Header } from './components/Header';
import { Footer } from './components/Footer';
import { FeedProvider } from './lib/feed';
import { LivePage } from './routes/LivePage';
import { AboutPage } from './routes/AboutPage';
import { BenchPage } from './routes/BenchPage';

function RootLayout() {
  return (
    <FeedProvider>
      <div className="app-shell">
        <Header />
        <main className="main">
          <Outlet />
        </main>
        <Footer />
      </div>
    </FeedProvider>
  );
}

const rootRoute = createRootRoute({ component: RootLayout });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: LivePage,
});

const benchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/bench',
  component: BenchPage,
});

const aboutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/about',
  component: AboutPage,
});

const routeTree = rootRoute.addChildren([indexRoute, benchRoute, aboutRoute]);

export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
