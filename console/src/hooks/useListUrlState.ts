import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import type { SortingState } from '@tanstack/react-table';

const pageSizes = [25, 50, 100] as const;

function parsePositive(value: string | null, fallback: number) {
  const parsed = value ? Number(value) : NaN;
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export function useListUrlState() {
  const [params, setParams] = useSearchParams();
  const filter = params.get('q') ?? '';
  const pageSizeCandidate = parsePositive(params.get('pageSize'), 25);
  const pageSize = pageSizes.includes(pageSizeCandidate as (typeof pageSizes)[number]) ? pageSizeCandidate : 25;
  const page = parsePositive(params.get('page'), 1) - 1;
  const sort = params.get('sort');
  const direction = params.get('dir');
  const sorting: SortingState = sort ? [{ id: sort, desc: direction === 'desc' }] : [];

  const update = useCallback((changes: Record<string, string | number | null>, resetPage = false) => {
    setParams((previous) => {
      const next = new URLSearchParams(previous);
      Object.entries(changes).forEach(([key, value]) => {
        if (value === null || value === '') next.delete(key);
        else next.set(key, String(value));
      });
      if (resetPage) next.delete('page');
      return next;
    }, { replace: true });
  }, [setParams]);

  const setFilter = useCallback((value: string) => update({ q: value }, true), [update]);
  const setPage = useCallback((value: number) => update({ page: value + 1 }), [update]);
  const setPageSize = useCallback((value: number) => update({ pageSize: value }, true), [update]);
  const setSorting = useCallback((value: SortingState) => {
    const first = value[0];
    update({ sort: first?.id ?? null, dir: first ? (first.desc ? 'desc' : 'asc') : null }, true);
  }, [update]);

  return { filter, setFilter, page, setPage, pageSize, setPageSize, sorting, setSorting };
}
