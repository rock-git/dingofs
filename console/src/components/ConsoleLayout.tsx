import { Link, NavLink, Outlet } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Button } from './ui';
import { useTheme } from '../hooks/useTheme';

const links = [
  ['/', 'Overview'],
  ['/filesystems', 'File Systems'],
  ['/mds', 'MDS Nodes'],
  ['/clients', 'Clients'],
  ['/cache-members', 'Cache Members'],
] as const;

export function ConsoleLayout() {
  const queryClient = useQueryClient();
  const { theme, setTheme } = useTheme();
  return <div className="console-shell">
    <aside className="sidebar">
      <Link className="brand" to="/"><span className="brand-mark">D</span><span><strong>DingoFS</strong><small>MDS Console</small></span></Link>
      <nav aria-label="Main navigation">{links.map(([to, label]) => <NavLink key={to} to={to} end={to === '/'} className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>{label}</NavLink>)}</nav>
      <div className="sidebar-footer"><Link to="/server-details">Server Details</Link><Link to="/version">Version</Link><Link to="/locks">Distributed Locks</Link><Link to="/cache-summary">Cache Summary</Link><Link to="/tools/parse-key">Parse Key</Link><a href="/FsStatService/legacy">Legacy Diagnostics</a></div>
    </aside>
    <main className="main-content"><header className="topbar"><div><span className="eyebrow">MANAGEMENT CONSOLE</span><h1>DingoFS Operations</h1></div><div className="topbar-actions"><Button variant="secondary" onClick={() => void queryClient.invalidateQueries()}>Refresh All</Button><select aria-label="Theme" value={theme} onChange={(event) => setTheme(event.target.value as typeof theme)}><option value="system">System theme</option><option value="light">Light theme</option><option value="dark">Dark theme</option></select></div></header><Outlet /></main>
  </div>;
}
