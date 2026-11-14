import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  useRouterState,
} from '@tanstack/react-router';
import { Header } from './components/Header';
import { Footer } from './components/Footer';
import { ErrorBoundary } from './components/ErrorBoundary';
import { FeedProvider } from './lib/feed';
import { LivePage } from './routes/LivePage';
import { AboutPage } from './routes/AboutPage';
import { BenchPage } from './routes/BenchPage';

function RootLayout() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });

  return (
    <FeedProvider>
      <a className="skip-link" href="#main">
        Skip to main content
      </a>
      <div className="app-shell">
        <Header />
        <main className="main" id="main" tabIndex={-1}>
          <ErrorBoundary key={pathname}>
            <Outlet />
          </ErrorBoundary>
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
