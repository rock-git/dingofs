import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Card, ErrorState, LoadingState } from "../components/ui";
import { managementApi } from "../services/api";
import { formatBytes, formatTime } from "../shared/format";

type JsonRecord = Record<string, unknown>;
type RemainingResponse = JsonRecord & {
  generatedAt?: string;
  items?: JsonRecord[];
};
type TableColumn = {
  key: string;
  label?: string;
  bytes?: boolean;
  time?: boolean;
};

function Value({
  value,
  bytes = false,
  time = false,
}: {
  value: unknown;
  bytes?: boolean;
  time?: boolean;
}) {
  if (value === null || value === undefined || value === "") return <>—</>;
  if (bytes) return <>{formatBytes(String(value))}</>;
  if (time) return <>{formatTime(String(value))}</>;
  if (Array.isArray(value)) return <>{value.map(String).join(", ") || "—"}</>;
  if (typeof value === "object") return <code>{JSON.stringify(value)}</code>;
  return <>{String(value)}</>;
}

function RecordsTable({
  rows,
  columns,
  linkKey,
  linkForRow,
}: {
  rows: JsonRecord[];
  columns: TableColumn[];
  linkKey?: string;
  linkForRow?: (row: JsonRecord) => string;
}) {
  if (rows.length === 0)
    return <div className="state-panel">No records found.</div>;
  return (
    <div className="table-scroll">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>{column.label ?? column.key}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr
              key={`${String(row.id ?? row.ino ?? row.name ?? "row")}-${index}`}
            >
              {columns.map((column) => {
                const content = (
                  <Value
                    value={row[column.key]}
                    bytes={column.bytes}
                    time={column.time}
                  />
                );
                return (
                  <td key={column.key}>
                    {linkKey === column.key && linkForRow ? (
                      <Link to={linkForRow(row)}>{content}</Link>
                    ) : (
                      content
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PageFrame({
  title,
  description,
  generatedAt,
  children,
  back = "/filesystems",
}: {
  title: string;
  description: string;
  generatedAt?: string;
  children: ReactNode;
  back?: string;
}) {
  return (
    <div className="page-stack">
      <div className="page-header">
        <div>
          <span className="eyebrow">MIGRATED DIAGNOSTICS</span>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
        <Link className="button button-secondary" to={back}>
          Back
        </Link>
      </div>
      <Card>
        <div className="table-controls">
          <span className="last-updated">
            Updated {formatTime(generatedAt)}
          </span>
        </div>
        {children}
      </Card>
    </div>
  );
}

function useRemaining(path: string, enabled = true) {
  return useQuery({
    queryKey: ["remaining-diagnostic", path],
    queryFn: ({ signal }) =>
      managementApi.getRemaining<RemainingResponse>(path, signal),
    enabled,
    staleTime: Infinity,
    retry: false,
  });
}

function RemainingListPage({
  title,
  description,
  path,
  columns,
  back,
  linkKey,
  linkForRow,
}: {
  title: string;
  description: string;
  path: string;
  columns: TableColumn[];
  back?: string;
  linkKey?: string;
  linkForRow?: (row: JsonRecord) => string;
}) {
  const query = useRemaining(path);
  if (query.isLoading) return <LoadingState />;
  if (query.error)
    return (
      <ErrorState error={query.error} onRetry={() => void query.refetch()} />
    );
  return (
    <PageFrame
      title={title}
      description={description}
      generatedAt={query.data?.generatedAt}
      back={back}
    >
      <RecordsTable
        rows={query.data?.items ?? []}
        columns={columns}
        linkKey={linkKey}
        linkForRow={linkForRow}
      />
    </PageFrame>
  );
}

export function ServerPage() {
  const query = useRemaining("/server");
  if (query.isLoading) return <LoadingState />;
  if (query.error)
    return (
      <ErrorState error={query.error} onRetry={() => void query.refetch()} />
    );
  return (
    <PageFrame
      title="MDS Server"
      description="Read-only server and in-process MDS diagnostic information."
      generatedAt={query.data?.generatedAt}
      back="/"
    >
      <pre className="diagnostic-json">
        {JSON.stringify(query.data?.server, null, 2)}
      </pre>
    </PageFrame>
  );
}

export function VersionPage() {
  const query = useRemaining("/version");
  if (query.isLoading) return <LoadingState />;
  if (query.error)
    return (
      <ErrorState error={query.error} onRetry={() => void query.refetch()} />
    );
  const build = (query.data?.build as JsonRecord[] | undefined) ?? [];
  const sdk = (query.data?.sdk as JsonRecord[] | undefined) ?? [];
  return (
    <PageFrame
      title="Version Information"
      description="DingoFS and Dingo SDK build information."
      generatedAt={query.data?.generatedAt}
      back="/"
    >
      <div className="nested-card">
        <h3>DingoFS</h3>
        <RecordsTable
          rows={build}
          columns={[
            { key: "name", label: "Name" },
            { key: "value", label: "Value" },
          ]}
        />
      </div>
      <div className="nested-card">
        <h3>Dingo SDK</h3>
        <RecordsTable
          rows={sdk}
          columns={[
            { key: "name", label: "Name" },
            { key: "value", label: "Value" },
          ]}
        />
      </div>
    </PageFrame>
  );
}

export function FilesystemDetailsPage() {
  const { fsId } = useParams<{ fsId: string }>();
  const query = useRemaining(
    `/filesystems/${encodeURIComponent(fsId ?? "")}/details`,
    Boolean(fsId),
  );
  if (query.isLoading) return <LoadingState />;
  if (query.error)
    return (
      <ErrorState error={query.error} onRetry={() => void query.refetch()} />
    );
  const filesystem = query.data?.filesystem as JsonRecord | undefined;
  const mounts = (filesystem?.mountPoints as JsonRecord[] | undefined) ?? [];
  const metrics = [
    ["Lifecycle", filesystem?.lifecycleState],
    ["Type", filesystem?.type],
    ["Owner", filesystem?.owner],
    ["UUID", filesystem?.uuid],
    ["Capacity", filesystem?.capacityBytes],
    ["Chunk size", filesystem?.chunkSizeBytes],
    ["Block size", filesystem?.blockSizeBytes],
    ["Version", filesystem?.version],
  ];
  return (
    <PageFrame
      title={`File System ${fsId}`}
      description="Allowlisted file-system configuration and mount information."
      generatedAt={query.data?.generatedAt}
    >
      <div className="metric-grid">
        {metrics.map(([label, value]) => (
          <div className="metric" key={String(label)}>
            <span>{String(label)}</span>
            <strong>
              {["Capacity", "Chunk size", "Block size"].includes(
                String(label),
              ) ? (
                <Value value={value} bytes />
              ) : (
                <Value value={value} />
              )}
            </strong>
          </div>
        ))}
      </div>
      <div className="nested-card">
        <h3>Storage and partition policy</h3>
        <pre className="diagnostic-json">
          {JSON.stringify(
            {
              storage: filesystem?.storage,
              partitionPolicy: filesystem?.partitionPolicy,
            },
            null,
            2,
          )}
        </pre>
      </div>
      <div className="nested-card">
        <h3>Mount points</h3>
        <RecordsTable
          rows={mounts}
          columns={[
            { key: "clientId", label: "Client" },
            { key: "hostname", label: "Hostname" },
            { key: "ip", label: "IP" },
            { key: "port", label: "Port" },
            { key: "path", label: "Path" },
            { key: "cto", label: "CTO" },
          ]}
        />
      </div>
    </PageFrame>
  );
}

export function InodeDetailsPage({ deleted = false }: { deleted?: boolean }) {
  const { fsId, ino } = useParams<{ fsId: string; ino: string }>();
  const resource = deleted ? "deleted-files" : "inodes";
  const query = useRemaining(
    `/filesystems/${encodeURIComponent(fsId ?? "")}/${resource}/${encodeURIComponent(ino ?? "")}`,
    Boolean(fsId && ino),
  );
  if (query.isLoading) return <LoadingState />;
  if (query.error)
    return (
      <ErrorState error={query.error} onRetry={() => void query.refetch()} />
    );
  const inode = query.data?.inode as JsonRecord | undefined;
  const metrics = [
    ["Type", inode?.type],
    ["Length", inode?.lengthBytes],
    ["UID", inode?.uid],
    ["GID", inode?.gid],
    ["Mode", inode?.mode],
    ["Links", inode?.nlink],
    ["Version", inode?.version],
    ["Open count", inode?.openCount],
  ];
  return (
    <PageFrame
      title={`Inode ${ino}`}
      description={
        deleted
          ? "Deleted inode metadata retained in the trash store."
          : "Allowlisted inode metadata from the MDS store."
      }
      generatedAt={query.data?.generatedAt}
    >
      <div className="metric-grid">
        {metrics.map(([label, value]) => (
          <div className="metric" key={String(label)}>
            <span>{String(label)}</span>
            <strong>
              {label === "Length" ? (
                <Value value={value} bytes />
              ) : (
                <Value value={value} />
              )}
            </strong>
          </div>
        ))}
      </div>
      <div className="nested-card">
        <h3>Times and relationships</h3>
        <RecordsTable
          rows={[inode ?? {}]}
          columns={[
            { key: "ctime", label: "Created", time: true },
            { key: "mtime", label: "Modified", time: true },
            { key: "atime", label: "Accessed", time: true },
            { key: "dtime", label: "Deleted", time: true },
            { key: "parents", label: "Parents" },
            { key: "shardBoundaries", label: "Shard boundaries" },
          ]}
        />
      </div>
      <div className="nested-card">
        <h3>Additional metadata</h3>
        <pre className="diagnostic-json">
          {JSON.stringify(
            {
              symlink: inode?.symlink,
              rdev: inode?.rdev,
              sharedSlice: inode?.sharedSlice,
              xattrNames: inode?.xattrNames,
            },
            null,
            2,
          )}
        </pre>
      </div>
    </PageFrame>
  );
}

export function DirectoryTreePage() {
  const { fsId } = useParams<{ fsId: string }>();
  const navigate = useNavigate();
  const [parentIno, setParentIno] = useState("0");
  const [trail, setTrail] = useState<Array<{ ino: string; name: string }>>([]);
  const path = `/filesystems/${encodeURIComponent(fsId ?? "")}/tree?parentIno=${encodeURIComponent(parentIno)}`;
  const query = useRemaining(path, Boolean(fsId));
  if (query.isLoading) return <LoadingState />;
  if (query.error)
    return (
      <ErrorState error={query.error} onRetry={() => void query.refetch()} />
    );
  const items = query.data?.items ?? [];
  const openDirectory = (item: JsonRecord) => {
    const ino = String(item.ino ?? "");
    setTrail([...trail, { ino, name: String(item.name ?? ino) }]);
    setParentIno(ino);
  };
  const goToTrail = (index: number) => {
    if (index < 0) {
      setTrail([]);
      setParentIno("0");
      return;
    }
    setTrail(trail.slice(0, index + 1));
    setParentIno(trail[index].ino);
  };
  return (
    <div className="page-stack">
      <div className="page-header">
        <div>
          <span className="eyebrow">MIGRATED DIAGNOSTICS</span>
          <h2>Directory Tree</h2>
          <p>
            Browse directory entries without loading the entire file-system
            tree.
          </p>
        </div>
        <Link className="button button-secondary" to={`/filesystems/${fsId}`}>
          Back
        </Link>
      </div>
      <Card>
        <div className="table-controls">
          <div className="breadcrumbs">
            <button className="inline-action" onClick={() => goToTrail(-1)}>
              Root
            </button>
            {trail.map((entry, index) => (
              <span key={`${entry.ino}-${index}`}>
                {" "}
                /{" "}
                <button
                  className="inline-action"
                  onClick={() => goToTrail(index)}
                >
                  {entry.name}
                </button>
              </span>
            ))}
          </div>
          <span className="last-updated">
            Updated {formatTime(query.data?.generatedAt)}
          </span>
        </div>
        <RecordsTable
          rows={items.map((item) => ({
            ...item,
            actions:
              item.type === "directory" ? "Open directory" : "Open inode",
          }))}
          columns={[
            { key: "name", label: "Name" },
            { key: "ino", label: "Inode" },
            { key: "type", label: "Type" },
            { key: "node", label: "MDS node" },
            { key: "description", label: "Description" },
            { key: "actions", label: "Action" },
          ]}
        />
        <div className="tree-actions">
          {items.map((item) => (
            <span key={String(item.ino)}>
              {item.type === "directory" ? (
                <button
                  className="button button-secondary"
                  onClick={() => openDirectory(item)}
                >
                  Open {String(item.name)}
                </button>
              ) : (
                <>
                  <button
                    className="button button-secondary"
                    onClick={() =>
                      navigate(
                        `/filesystems/${fsId}/inodes/${encodeURIComponent(String(item.ino))}`,
                      )
                    }
                  >
                    Inode {String(item.ino)}
                  </button>
                  <Link
                    className="button button-secondary"
                    to={`/filesystems/${fsId}/files/${encodeURIComponent(String(item.ino))}/chunks`}
                  >
                    Chunks
                  </Link>
                  <Link
                    className="button button-secondary"
                    to={`/filesystems/${fsId}/files/${encodeURIComponent(String(item.ino))}/shard`}
                  >
                    Shard
                  </Link>
                </>
              )}
            </span>
          ))}
        </div>
      </Card>
    </div>
  );
}

export function ParseKeyPage() {
  const [key, setKey] = useState("");
  const [submitted, setSubmitted] = useState("");
  const query = useRemaining(
    `/tools/parse-key?key=${encodeURIComponent(submitted)}`,
    Boolean(submitted),
  );
  return (
    <PageFrame
      title="Parse Storage Key"
      description="Decode a hexadecimal metadata key using the server-side codec."
      generatedAt={query.data?.generatedAt}
      back="/"
    >
      <form
        className="inline-form"
        onSubmit={(event) => {
          event.preventDefault();
          setSubmitted(key.trim());
        }}
      >
        <label htmlFor="metadata-key">Key (hex)</label>
        <input
          id="metadata-key"
          value={key}
          onChange={(event) => setKey(event.target.value)}
          placeholder="Enter a metadata key"
        />
        <button className="button" type="submit">
          Parse
        </button>
      </form>
      {query.isLoading ? (
        <LoadingState />
      ) : query.error ? (
        <ErrorState error={query.error} onRetry={() => void query.refetch()} />
      ) : submitted ? (
        <pre className="diagnostic-json">
          {String(query.data?.result ?? "")}
        </pre>
      ) : (
        <div className="state-panel">Enter a key to begin.</div>
      )}
    </PageFrame>
  );
}

export function LocksPage() {
  return (
    <RemainingListPage
      title="Distributed Locks"
      description="Current store-backed distribution lock leases."
      path="/locks"
      columns={[
        { key: "name", label: "Name" },
        { key: "owner", label: "Owner" },
        { key: "epoch", label: "Epoch" },
        { key: "expiresAt", label: "Expires", time: true },
      ]}
      back="/"
    />
  );
}

export function IdGeneratorsPage() {
  return (
    <RemainingListPage
      title="ID Generators"
      description="Read-only allocation ranges for file-system, slice, and inode IDs."
      path="/id-generators"
      columns={[
        { key: "scope", label: "Scope" },
        { key: "description", label: "Description" },
      ]}
      back="/"
    />
  );
}

export function CacheSummaryPage() {
  return (
    <RemainingListPage
      title="MDS Cache Summary"
      description="Cache counts, bytes, and hit ratios grouped by file system."
      path="/cache-summary"
      columns={[
        { key: "filesystemId", label: "File system" },
        { key: "filesystemName", label: "Name" },
        { key: "name", label: "Cache" },
        { key: "count", label: "Count" },
        { key: "total_count", label: "Accumulated" },
        { key: "clean_count", label: "Cleaned" },
        { key: "bytes", label: "Bytes", bytes: true },
        { key: "miss_count", label: "Misses" },
        { key: "hit_count", label: "Hits" },
        { key: "hitRatio", label: "Hit ratio" },
      ]}
      back="/"
    />
  );
}

export function DeletedFilesPage() {
  const { fsId } = useParams<{ fsId: string }>();
  return (
    <RemainingListPage
      title="Deleted Files"
      description="Inodes waiting for trash cleanup."
      path={`/filesystems/${encodeURIComponent(fsId ?? "")}/deleted-files`}
      columns={[
        { key: "id", label: "Inode" },
        { key: "type", label: "Type" },
        { key: "lengthBytes", label: "Length", bytes: true },
        { key: "nlink", label: "Links" },
        { key: "version", label: "Version" },
        { key: "ctime", label: "Created", time: true },
      ]}
      linkKey="id"
      linkForRow={(row) =>
        `/filesystems/${fsId}/deleted-files/${encodeURIComponent(String(row.id))}`
      }
      back={`/filesystems/${fsId}`}
    />
  );
}

export function DeletedSlicesPage() {
  const { fsId } = useParams<{ fsId: string }>();
  return (
    <RemainingListPage
      title="Deleted Slices"
      description="Slices retained for deferred garbage collection."
      path={`/filesystems/${encodeURIComponent(fsId ?? "")}/deleted-slices`}
      columns={[
        { key: "ino", label: "Inode" },
        { key: "chunkIndex", label: "Chunk" },
        { key: "slice", label: "Slice" },
        { key: "blockSizeBytes", label: "Block size", bytes: true },
        { key: "deletedAt", label: "Deleted", time: true },
      ]}
      back={`/filesystems/${fsId}`}
    />
  );
}

export function SliceReferencesPage() {
  const { fsId } = useParams<{ fsId: string }>();
  return (
    <RemainingListPage
      title="Slice References"
      description="Global slice reference counts and referencing inodes."
      path={`/filesystems/${encodeURIComponent(fsId ?? "")}/slice-references`}
      columns={[
        { key: "id", label: "Slice ID" },
        { key: "sizeBytes", label: "Size", bytes: true },
        { key: "refCount", label: "References" },
        { key: "inodes", label: "Inodes" },
      ]}
      back={`/filesystems/${fsId}`}
    />
  );
}

export function OpLogPage() {
  const { fsId } = useParams<{ fsId: string }>();
  return (
    <RemainingListPage
      title="File-system OpLog"
      description="Read-only file-system membership and lifecycle operations."
      path={`/filesystems/${encodeURIComponent(fsId ?? "")}/oplog`}
      columns={[
        { key: "time", label: "Time", time: true },
        { key: "type", label: "Type" },
        { key: "epoch", label: "Epoch" },
        { key: "comment", label: "Comment" },
        { key: "parameter", label: "Parameter" },
      ]}
      back={`/filesystems/${fsId}`}
    />
  );
}
