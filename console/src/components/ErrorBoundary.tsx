import { Component, type ErrorInfo, type ReactNode } from 'react';

export class ErrorBoundary extends Component<{ children: ReactNode }, { hasError: boolean }> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Management console rendering failed', error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return <div className="state-panel state-error"><strong>The console failed to render.</strong><span>Open the legacy diagnostics page to continue investigating.</span><a href="/FsStatService/legacy">Open Legacy Diagnostics</a></div>;
    }
    return this.props.children;
  }
}
