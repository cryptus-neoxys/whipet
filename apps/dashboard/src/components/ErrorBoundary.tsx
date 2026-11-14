import { Component } from 'react';
import type { ErrorInfo, ReactNode } from 'react';

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  error: Error | null;
}

/**
 * Route-level boundary. The feed and the node keep running; only the broken
 * view is replaced. A class component is required for componentDidCatch.
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Keep the original stack in the console; the fallback stays readable.
    console.error('dashboard route failed to render', error, info.componentStack);
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;

    return (
      <div className="error-fallback" role="alert">
        <p className="eyebrow">dashboard error</p>
        <h1>This view stopped rendering.</h1>
        <p className="error-lede">
          The node and the feed may still be healthy. Reloading rebuilds the view; it does not
          restart the matcher or the log.
        </p>
        <pre className="error-detail">{error.message || error.name}</pre>
        <div className="error-actions">
          <button
            type="button"
            className="beat-action"
            onClick={() => this.setState({ error: null })}
          >
            Try again
          </button>
          <button
            type="button"
            className="beat-action beat-action-quiet"
            onClick={() => window.location.reload()}
          >
            Reload page
          </button>
        </div>
      </div>
    );
  }
}
