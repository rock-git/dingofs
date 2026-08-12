import type { ButtonHTMLAttributes, HTMLAttributes, ReactNode } from 'react';

export function Button({ className = '', variant = 'primary', ...props }: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: 'primary' | 'secondary' | 'ghost' | 'danger' }) {
  return <button className={`button button-${variant} ${className}`} {...props} />;
}

export function Card({ className = '', children, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <section className={`card ${className}`} {...props}>{children}</section>;
}

export function Badge({ children, tone = 'neutral' }: { children: ReactNode; tone?: 'neutral' | 'good' | 'warn' | 'bad' | 'info' }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}

export function LoadingState() {
  return <div className="state-panel"><span className="spinner" /> Loading data…</div>;
}

export function ErrorState({ error, onRetry }: { error: unknown; onRetry: () => void }) {
  const message = error instanceof Error ? error.message : 'The request failed.';
  return <div className="state-panel state-error"><strong>Unable to load this section.</strong><span>{message}</span><Button variant="secondary" onClick={onRetry}>Retry</Button></div>;
}

export function EmptyState({ message = 'No records found.' }: { message?: string }) {
  return <div className="state-panel"><span>{message}</span></div>;
}
