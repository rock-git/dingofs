import { flexRender, getCoreRowModel, getSortedRowModel, useReactTable, type ColumnDef, type SortingState } from '@tanstack/react-table';
import { useEffect, useMemo } from 'react';
import { Button, EmptyState } from './ui';

export function DataTable<T extends object>({
  columns,
  data,
  getRowId,
  emptyMessage,
  sorting,
  onSortingChange,
  page,
  onPageChange,
  pageSize,
  onPageSizeChange,
  hasNextPage = false,
  isFetchingNextPage = false,
  onFetchNextPage,
}: {
  columns: ColumnDef<T, unknown>[];
  data: T[];
  getRowId?: (row: T) => string;
  emptyMessage?: string;
  sorting: SortingState;
  onSortingChange: (value: SortingState) => void;
  page: number;
  onPageChange: (value: number) => void;
  pageSize: number;
  onPageSizeChange: (value: number) => void;
  hasNextPage?: boolean;
  isFetchingNextPage?: boolean;
  onFetchNextPage?: () => Promise<unknown>;
}) {
  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: (updater) => {
      const next = typeof updater === 'function' ? updater(sorting) : updater;
      onSortingChange(next);
    },
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId,
  });
  const rows = table.getRowModel().rows;
  const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
  const visibleRows = useMemo(() => rows.slice(page * pageSize, (page + 1) * pageSize), [rows, page, pageSize]);

  useEffect(() => {
    if (page >= pageCount && !hasNextPage) onPageChange(Math.max(0, pageCount - 1));
  }, [hasNextPage, onPageChange, page, pageCount]);

  if (rows.length === 0) return <EmptyState message={emptyMessage} />;

  const canGoNext = page + 1 < pageCount || hasNextPage;
  const nextPage = async () => {
    if (page + 1 >= pageCount && hasNextPage && onFetchNextPage) await onFetchNextPage();
    onPageChange(page + 1);
  };

  return <>
    <div className="table-toolbar">
      <span>{rows.length.toLocaleString()} loaded records{hasNextPage ? ' · more available' : ''}</span>
      <label>Rows <select value={pageSize} onChange={(event) => onPageSizeChange(Number(event.target.value))}><option value={25}>25</option><option value={50}>50</option><option value={100}>100</option></select></label>
    </div>
    <div className="table-scroll"><table className="data-table"><thead>{table.getHeaderGroups().map((headerGroup) => <tr key={headerGroup.id}>{headerGroup.headers.map((header) => <th key={header.id}>{header.isPlaceholder ? null : <button className="sort-button" onClick={header.column.getToggleSortingHandler()}>{flexRender(header.column.columnDef.header, header.getContext())}{header.column.getIsSorted() === 'asc' ? ' ↑' : header.column.getIsSorted() === 'desc' ? ' ↓' : ''}</button>}</th>)}</tr>)}</thead><tbody>{visibleRows.map((row) => <tr key={row.id}>{row.getVisibleCells().map((cell) => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)}</tbody></table></div>
    <div className="pagination"><Button variant="ghost" disabled={page === 0} onClick={() => onPageChange(page - 1)}>Previous</Button><span>Page {page + 1} of {pageCount}{hasNextPage ? '+' : ''}</span><Button variant="ghost" disabled={!canGoNext || isFetchingNextPage} onClick={() => void nextPage()}>{isFetchingNextPage ? 'Loading…' : 'Next'}</Button></div>
  </>;
}
