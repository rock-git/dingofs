import type { ColumnDef } from '@tanstack/react-table';
import { useState, type ReactNode } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { DataTable } from '../components/DataTable';
import { Badge, Button, Card, ErrorState, LoadingState } from '../components/ui';
import { useCacheMembers, useClients, useFilesystems, useMdsNodes } from '../hooks/useResources';
import { useListUrlState } from '../hooks/useListUrlState';
import type { CacheMember, Client, Filesystem, ListResponse, MdsNode } from '../types/api';
import { formatBytes, formatTime, toneForState } from '../shared/format';

function PageHeader({ title, description, onRefresh, isFetching }: { title: string; description: string; onRefresh: () => void; isFetching: boolean }) {
  return <div className="page-header"><div><span className="eyebrow">RESOURCE LIST</span><h2>{title}</h2><p>{description}</p></div><Button variant="secondary" onClick={onRefresh}>{isFetching ? 'Refreshing…' : 'Refresh'}</Button></div>;
}

function flattenPages<T>(pages: ListResponse<T>[] | undefined): T[] {
  return pages?.flatMap((page) => page.items) ?? [];
}

function TableSection<T extends object>({ data, columns, loading, error, onRetry, generatedAt, tableState, getRowId, details, hasNextPage, isFetchingNextPage, onFetchNextPage }: { data: T[]; columns: ColumnDef<T, unknown>[]; loading: boolean; error: unknown; onRetry: () => void; generatedAt?: string; tableState: ReturnType<typeof useListUrlState>; getRowId?: (row: T) => string; details?: ReactNode; hasNextPage?: boolean; isFetchingNextPage?: boolean; onFetchNextPage?: () => Promise<unknown> }) {
  if (loading) return <LoadingState />;
  if (error) return <ErrorState error={error} onRetry={onRetry} />;
  const filtered = data.filter((item) => JSON.stringify(item).toLowerCase().includes(tableState.filter.toLowerCase()));
  return <div className={details ? 'resource-with-details' : ''}><Card><div className="table-controls"><input aria-label="Search resources" placeholder="Search resources…" value={tableState.filter} onChange={(event) => tableState.setFilter(event.target.value)} /><span className="last-updated">Updated {formatTime(generatedAt)}</span></div><DataTable data={filtered} columns={columns} getRowId={getRowId} emptyMessage="No matching resources." sorting={tableState.sorting} onSortingChange={tableState.setSorting} page={tableState.page} onPageChange={tableState.setPage} pageSize={tableState.pageSize} onPageSizeChange={tableState.setPageSize} hasNextPage={hasNextPage} isFetchingNextPage={isFetchingNextPage} onFetchNextPage={onFetchNextPage} /></Card>{details}</div>;
}

function ActionsMenu({ filesystem, onDetails }: { filesystem: Filesystem; onDetails: () => void }) {
  return <details className="action-menu"><summary>Actions</summary><div className="action-menu-content"><span className="action-group">Filesystem</span><button onClick={onDetails}>Details</button><a href={`/FsStatService/filesystems/${filesystem.id}/tree`}>Directory tree</a><span className="action-group">Console diagnostics</span><a href={`/FsStatService/filesystems/${filesystem.id}/quota`}>Quota</a><a href={`/FsStatService/filesystems/${filesystem.id}/dir-stats`}>Dir stats</a><a href={`/FsStatService/filesystems/${filesystem.id}/mountpoints`}>Mount points</a><a href={`/FsStatService/filesystems/${filesystem.id}/file-sessions`}>File sessions</a><a href={`/FsStatService/filesystems/${filesystem.id}/deleted-files`}>Deleted files</a><a href={`/FsStatService/filesystems/${filesystem.id}/deleted-slices`}>Deleted slices</a><a href={`/FsStatService/filesystems/${filesystem.id}/slice-references`}>Slice references</a><a href={`/FsStatService/filesystems/${filesystem.id}/oplog`}>OpLog</a><span className="action-group">Tools</span><a href="/FsStatService/tools/parse-key">Parse key</a></div></details>;
}

export function FilesystemsPage() {
  const query = useFilesystems();
  const tableState = useListUrlState();
  const navigate = useNavigate();
  const { fsId } = useParams<{ fsId?: string }>();
  const data = flattenPages(query.data?.pages);
  const selected = fsId ? data.find((item) => item.id === fsId) : null;
  const columns: ColumnDef<Filesystem, unknown>[] = [
    { accessorKey: 'id', header: 'ID' },
    { accessorKey: 'name', header: 'Name', cell: ({ row }) => <button className="inline-action" onClick={() => navigate(`/filesystems/${row.original.id}`)}>{row.original.name}</button> },
    { accessorKey: 'lifecycleState', header: 'Lifecycle', cell: ({ getValue }) => <Badge tone={toneForState(String(getValue()))}>{String(getValue())}</Badge> },
    { accessorKey: 'type', header: 'Type' },
    { accessorKey: 'partitionType', header: 'Partition' },
    { accessorKey: 'owner', header: 'Owner' },
    { accessorKey: 'capacityBytes', header: 'Capacity', cell: ({ getValue }) => formatBytes(String(getValue())) },
    { accessorKey: 'mountPointCount', header: 'Mounts' },
    { accessorKey: 'updatedAt', header: 'Updated', cell: ({ getValue }) => formatTime(String(getValue())) },
    { id: 'actions', header: 'Actions', cell: ({ row }) => <ActionsMenu filesystem={row.original} onDetails={() => navigate(`/filesystems/${row.original.id}`)} /> },
  ];
  const details = fsId ? <aside className="details-panel"><div className="details-heading"><div><span className="eyebrow">FILE SYSTEM</span><h3>{selected?.name ?? fsId}</h3></div><button className="close-button" onClick={() => navigate('/filesystems')} aria-label="Close details">×</button></div>{selected ? <dl><dt>ID</dt><dd>{selected.id}</dd><dt>Lifecycle</dt><dd><Badge tone={toneForState(selected.lifecycleState)}>{selected.lifecycleState}</Badge></dd><dt>Storage</dt><dd>{selected.storage?.type ?? 'unknown'}{selected.storage?.endpoint ? ` · ${selected.storage.endpoint}` : ''}{selected.storage?.bucket ? ` · ${selected.storage.bucket}` : ''}</dd><dt>Capacity</dt><dd>{formatBytes(selected.capacityBytes)}</dd><dt>Chunk / block</dt><dd>{formatBytes(selected.chunkSizeBytes)} / {formatBytes(selected.blockSizeBytes)}</dd><dt>Mount points</dt><dd>{selected.mountPointCount}</dd><dt>UUID</dt><dd className="break-value">{selected.uuid || '—'}</dd><dt>Version</dt><dd>{selected.version || '—'}</dd><dt>Updated</dt><dd>{formatTime(selected.updatedAt)}</dd></dl> : <div className="state-panel">This file system is not present in the loaded collection.</div>}</aside> : undefined;
  return <div className="page-stack"><PageHeader title="File Systems" description="Lifecycle, capacity, and storage metadata for managed file systems." onRefresh={() => void query.refetch()} isFetching={query.isFetching} /><TableSection data={data} columns={columns} loading={query.isLoading} error={query.error} onRetry={() => void query.refetch()} generatedAt={query.data?.pages[0]?.generatedAt} tableState={tableState} getRowId={(row) => row.id} details={details} hasNextPage={query.hasNextPage} isFetchingNextPage={query.isFetchingNextPage} onFetchNextPage={() => query.fetchNextPage()} /></div>;
}

export function MdsPage() {
  const query = useMdsNodes();
  const tableState = useListUrlState();
  const columns: ColumnDef<MdsNode, unknown>[] = [{ accessorKey: 'id', header: 'ID' }, { id: 'address', header: 'Address', accessorFn: (row) => `${row.host}:${row.port}` }, { accessorKey: 'state', header: 'State' }, { accessorKey: 'health.state', header: 'Health', cell: ({ row }) => <Badge tone={toneForState(row.original.health.state)}>{row.original.health.state}</Badge> }, { accessorKey: 'createdAt', header: 'Created', cell: ({ getValue }) => formatTime(String(getValue())) }, { accessorKey: 'lastOnlineAt', header: 'Last online', cell: ({ getValue }) => formatTime(String(getValue())) }, { id: 'tools', header: 'Tools', cell: ({ row }) => <a href={`http://${row.original.host}:${row.original.port}/FsStatService`} target="_blank" rel="noopener noreferrer">Open ↗</a> }];
  return <div className="page-stack"><PageHeader title="MDS Nodes" description="MDS membership, state, and last heartbeat information." onRefresh={() => void query.refetch()} isFetching={query.isFetching} /><TableSection data={flattenPages(query.data?.pages)} columns={columns} loading={query.isLoading} error={query.error} onRetry={() => void query.refetch()} generatedAt={query.data?.pages[0]?.generatedAt} tableState={tableState} getRowId={(row) => row.id} hasNextPage={query.hasNextPage} isFetchingNextPage={query.isFetchingNextPage} onFetchNextPage={() => query.fetchNextPage()} /></div>;
}

export function ClientsPage() {
  const query = useClients();
  const tableState = useListUrlState();
  const columns: ColumnDef<Client, unknown>[] = [{ accessorKey: 'id', header: 'ID' }, { accessorKey: 'hostname', header: 'Host' }, { accessorKey: 'mountpoint', header: 'Mount point' }, { accessorKey: 'filesystem', header: 'File system' }, { accessorKey: 'health.state', header: 'Health', cell: ({ row }) => <Badge tone={toneForState(row.original.health.state)}>{row.original.health.state}</Badge> }, { accessorKey: 'lastOnlineAt', header: 'Last online', cell: ({ getValue }) => formatTime(String(getValue())) }];
  return <div className="page-stack"><PageHeader title="Clients" description="Connected clients and their last heartbeat information." onRefresh={() => void query.refetch()} isFetching={query.isFetching} /><TableSection data={flattenPages(query.data?.pages)} columns={columns} loading={query.isLoading} error={query.error} onRetry={() => void query.refetch()} generatedAt={query.data?.pages[0]?.generatedAt} tableState={tableState} getRowId={(row) => row.id} hasNextPage={query.hasNextPage} isFetchingNextPage={query.isFetchingNextPage} onFetchNextPage={() => query.fetchNextPage()} /></div>;
}

export function CacheMembersPage() {
  const query = useCacheMembers();
  const tableState = useListUrlState();
  const columns: ColumnDef<CacheMember, unknown>[] = [{ accessorKey: 'id', header: 'Member ID' }, { accessorKey: 'host', header: 'Host' }, { accessorKey: 'group', header: 'Group' }, { accessorKey: 'weight', header: 'Weight' }, { accessorKey: 'locked', header: 'Locked', cell: ({ getValue }) => getValue() ? 'Yes' : 'No' }, { accessorKey: 'state', header: 'State', cell: ({ getValue }) => <Badge tone={toneForState(String(getValue()))}>{String(getValue())}</Badge> }, { accessorKey: 'lastOnlineAt', header: 'Last online', cell: ({ getValue }) => formatTime(String(getValue())) }];
  return <div className="page-stack"><PageHeader title="Cache Members" description="Cache member membership, weights, locks, and heartbeat state." onRefresh={() => void query.refetch()} isFetching={query.isFetching} /><TableSection data={flattenPages(query.data?.pages)} columns={columns} loading={query.isLoading} error={query.error} onRetry={() => void query.refetch()} generatedAt={query.data?.pages[0]?.generatedAt} tableState={tableState} getRowId={(row) => row.id} hasNextPage={query.hasNextPage} isFetchingNextPage={query.isFetchingNextPage} onFetchNextPage={() => query.fetchNextPage()} /></div>;
}
