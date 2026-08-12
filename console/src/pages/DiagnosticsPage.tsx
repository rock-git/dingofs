import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import { Card, ErrorState, LoadingState } from '../components/ui';
import { managementApi } from '../services/api';
import { formatBytes, formatTime } from '../shared/format';

type DiagnosticPageKind = 'quota' | 'dir-stats' | 'mountpoints' | 'file-sessions' | 'chunks' | 'shard';
type DiagnosticResponse = {
  filesystemId: string;
  generatedAt: string;
  filesystem?: Record<string, string>;
  directories?: Array<Record<string, string>>;
  items?: Array<Record<string, unknown>>;
  ino?: string;
  partition?: { shards?: Array<Record<string, unknown>>; [key: string]: unknown };
};

const metadata: Record<DiagnosticPageKind, { title: string; description: string }> = {
  quota: { title: 'Quota', description: 'File-system and directory quota usage.' },
  'dir-stats': { title: 'Directory Statistics', description: 'Directory byte, inode, and child-directory usage.' },
  mountpoints: { title: 'Mount Points', description: 'Clients currently mounting this file system.' },
  'file-sessions': { title: 'File Sessions', description: 'Open file sessions registered for this file system.' },
  chunks: { title: 'Chunks', description: 'Chunk and slice layout for the selected inode.' },
  shard: { title: 'Partition Shards', description: 'Partition shard boundaries for the selected inode.' },
};

function Value({ value }: { value: unknown }) {
  if (value === null || value === undefined || value === '') return <>—</>;
  return <>{String(value)}</>;
}

function DiagnosticContent({ kind, data }: { kind: DiagnosticPageKind; data: DiagnosticResponse }) {
  if (kind === 'quota') {
    const quota = data.filesystem ?? {};
    return <><div className="metric-grid">{[['Max bytes', quota.maxBytes], ['Used bytes', quota.usedBytes], ['Max inodes', quota.maxInodes], ['Used inodes', quota.usedInodes]].map(([label, value]) => <div className="metric" key={label}><span>{label}</span><strong>{label.toLowerCase().includes('bytes') ? formatBytes(String(value)) : <Value value={value} />}</strong></div>)}</div><DiagnosticTable rows={data.directories ?? []} columns={['ino', 'maxBytes', 'usedBytes', 'maxInodes', 'usedInodes']} /></>;
  }
  if (kind === 'shard') {
    const shards = data.partition?.shards ?? [];
    return <><pre className="diagnostic-json">{JSON.stringify(data.partition, null, 2)}</pre><DiagnosticTable rows={shards} columns={['start', 'end', 'size', 'version']} /></>;
  }
  if (kind === 'chunks') {
    return <>{(data.items ?? []).map((chunk) => <Card className="nested-card" key={String(chunk.index)}><div className="card-heading"><h3>Chunk {String(chunk.index)}</h3><span>v{String(chunk.version)} · {formatBytes(String(chunk.chunkSizeBytes))}</span></div><DiagnosticTable rows={(chunk.slices as Array<Record<string, unknown>> | undefined) ?? []} columns={['id', 'pos', 'size', 'off', 'len']} /></Card>)}</>;
  }
  const columns = kind === 'dir-stats' ? ['ino', 'lengthBytes', 'inodes', 'directories'] : kind === 'mountpoints' ? ['clientId', 'hostname', 'ip', 'port', 'path'] : ['ino', 'sessionId', 'clientId', 'createdAt', 'expiresAt'];
  return <DiagnosticTable rows={data.items ?? []} columns={columns} />;
}

function DiagnosticTable({ rows, columns }: { rows: Array<Record<string, unknown>>; columns: string[] }) {
  if (rows.length === 0) return <div className="state-panel">No records found.</div>;
  return <div className="table-scroll"><table className="data-table"><thead><tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr></thead><tbody>{rows.map((row, index) => <tr key={String(row.ino ?? row.id ?? index)}>{columns.map((column) => <td key={column}>{column.endsWith('At') ? formatTime(String(row[column] ?? '')) : column.toLowerCase().includes('bytes') ? formatBytes(String(row[column] ?? '')) : <Value value={row[column]} />}</td>)}</tr>)}</tbody></table></div>;
}

export function DiagnosticsPage({ kind }: { kind: DiagnosticPageKind }) {
  const { fsId, ino } = useParams<{ fsId: string; ino?: string }>();
  const path = `/filesystems/${encodeURIComponent(fsId ?? '')}${kind === 'chunks' ? `/files/${encodeURIComponent(ino ?? '')}/chunks` : kind === 'shard' ? `/files/${encodeURIComponent(ino ?? '')}/shard` : `/${kind}`}`;
  const query = useQuery({ queryKey: ['diagnostic', path], queryFn: ({ signal }) => managementApi.getDiagnostic<DiagnosticResponse>(path, signal), enabled: Boolean(fsId && (kind !== 'chunks' && kind !== 'shard' || ino)), staleTime: Infinity, retry: false });
  const info = metadata[kind];
  return <div className="page-stack"><div className="page-header"><div><span className="eyebrow">DIAGNOSTICS</span><h2>{info.title}</h2><p>{info.description} · File system {fsId}{ino ? ` · inode ${ino}` : ''}</p></div><Link className="button button-secondary" to={`/filesystems/${fsId}`}>Back to file system</Link></div>{query.isLoading ? <LoadingState /> : query.error ? <ErrorState error={query.error} onRetry={() => void query.refetch()} /> : <Card className="diagnostic-card"><div className="table-controls"><span className="last-updated">Updated {formatTime(query.data?.generatedAt)}</span></div><DiagnosticContent kind={kind} data={query.data!} /></Card>}</div>;
}
