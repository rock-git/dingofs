import { Link } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { useOverview, useFilesystems, useMdsNodes, useClients, useCacheMembers } from '../hooks/useResources';
import { Badge, Button, Card, ErrorState, LoadingState } from '../components/ui';
import { formatBytes, formatTime, toneForState } from '../shared/format';

function ResourceCard({ title, path, count, children, loading, error, onRetry, onRefresh, refreshing }: { title: string; path: string; count?: number; children?: React.ReactNode; loading: boolean; error: unknown; onRetry: () => void; onRefresh: () => void; refreshing: boolean }) {
  return <Card className="resource-card"><div className="card-heading"><div><span className="eyebrow">RESOURCE</span><h2>{title}</h2></div><div className="card-heading-actions">{count !== undefined && <strong className="big-number">{count}</strong>}<Button variant="ghost" onClick={onRefresh} disabled={refreshing}>{refreshing ? '…' : '↻'}</Button></div></div>{loading ? <LoadingState /> : error ? <ErrorState error={error} onRetry={onRetry} /> : <>{children}<Link className="view-link" to={path}>View all →</Link></>}</Card>;
}

export function OverviewPage() {
  const queryClient = useQueryClient();
  const overview = useOverview();
  const filesystems = useFilesystems();
  const mds = useMdsNodes();
  const clients = useClients();
  const cache = useCacheMembers();
  const filesystemPage = filesystems.data?.pages[0];
  const mdsPage = mds.data?.pages[0];
  const clientsPage = clients.data?.pages[0];
  const cachePage = cache.data?.pages[0];
  if (overview.isLoading) return <LoadingState />;
  return <div className="page-stack">
    {overview.error ? <ErrorState error={overview.error} onRetry={() => void overview.refetch()} /> : <Card className="hero-card"><div><span className="eyebrow">CLUSTER OVERVIEW</span><h2>{overview.data?.clusterId ?? 'Unknown cluster'}</h2><p>Serving MDS <strong>{overview.data?.servingMdsId}</strong> · {overview.data?.storageEngine}</p></div><div className="build-info"><span>API {overview.data?.apiVersion}</span><span>{overview.data?.build.version}</span><span>{overview.data?.build.commit}</span></div></Card>}
    <div className="resource-grid">
      <ResourceCard title="File Systems" path="/filesystems" count={filesystemPage?.summary.total ?? filesystemPage?.items.length} loading={filesystems.isLoading} error={filesystems.error} onRetry={() => void filesystems.refetch()} onRefresh={() => void filesystems.refetch()} refreshing={filesystems.isFetching}>{filesystemPage?.items.slice(0, 4).map((item) => <div className="preview-row" key={item.id}><span><strong>{item.name}</strong><small>{formatBytes(item.capacityBytes)} · {item.mountPointCount} mounts</small></span><Badge tone={toneForState(item.lifecycleState)}>{item.lifecycleState}</Badge></div>)}</ResourceCard>
      <ResourceCard title="MDS Nodes" path="/mds" count={mdsPage?.summary.total ?? mdsPage?.items.length} loading={mds.isLoading} error={mds.error} onRetry={() => void mds.refetch()} onRefresh={() => void mds.refetch()} refreshing={mds.isFetching}>{mdsPage?.items.slice(0, 4).map((item) => <div className="preview-row" key={item.id}><span><strong>MDS {item.id}</strong><small>{item.host}:{item.port}</small></span><Badge tone={toneForState(item.health.state)}>{item.health.state}</Badge></div>)}</ResourceCard>
      <ResourceCard title="Clients" path="/clients" count={clientsPage?.summary.total ?? clientsPage?.items.length} loading={clients.isLoading} error={clients.error} onRetry={() => void clients.refetch()} onRefresh={() => void clients.refetch()} refreshing={clients.isFetching}>{clientsPage?.items.slice(0, 4).map((item) => <div className="preview-row" key={item.id}><span><strong>{item.hostname || item.id}</strong><small>{item.filesystem} · {item.mountpoint}</small></span><Badge tone={toneForState(item.health.state)}>{item.health.state}</Badge></div>)}</ResourceCard>
      <ResourceCard title="Cache Members" path="/cache-members" count={cachePage?.summary.total ?? cachePage?.items.length} loading={cache.isLoading} error={cache.error} onRetry={() => void cache.refetch()} onRefresh={() => void cache.refetch()} refreshing={cache.isFetching}>{cachePage?.items.slice(0, 4).map((item) => <div className="preview-row" key={item.id}><span><strong>{item.id}</strong><small>{item.host} · {item.group}</small></span><Badge tone={toneForState(item.state)}>{item.state}</Badge></div>)}</ResourceCard>
    </div>
    <div className="last-updated">Data loaded at {formatTime(filesystemPage?.generatedAt ?? mdsPage?.generatedAt ?? clientsPage?.generatedAt ?? cachePage?.generatedAt)} · <button className="inline-action" onClick={() => void queryClient.invalidateQueries()}>Refresh all modules</button></div>
  </div>;
}
