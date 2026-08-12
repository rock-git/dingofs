import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { managementApi } from '../services/api';

const listOptions = { staleTime: Infinity, retry: false } as const;

export function useOverview() {
  return useQuery({ queryKey: ['overview'], queryFn: ({ signal }) => managementApi.getOverview(signal), ...listOptions });
}

export function useFilesystems() {
  return useInfiniteQuery({
    queryKey: ['filesystems'],
    initialPageParam: null as string | null,
    queryFn: ({ signal, pageParam }) => managementApi.getFilesystems(signal, pageParam),
    getNextPageParam: (page) => page.nextCursor ?? undefined,
    ...listOptions,
  });
}

export function useMdsNodes() {
  return useInfiniteQuery({
    queryKey: ['mds-nodes'],
    initialPageParam: null as string | null,
    queryFn: ({ signal, pageParam }) => managementApi.getMdsNodes(signal, pageParam),
    getNextPageParam: (page) => page.nextCursor ?? undefined,
    ...listOptions,
  });
}

export function useClients() {
  return useInfiniteQuery({
    queryKey: ['clients'],
    initialPageParam: null as string | null,
    queryFn: ({ signal, pageParam }) => managementApi.getClients(signal, pageParam),
    getNextPageParam: (page) => page.nextCursor ?? undefined,
    ...listOptions,
  });
}

export function useCacheMembers() {
  return useInfiniteQuery({
    queryKey: ['cache-members'],
    initialPageParam: null as string | null,
    queryFn: ({ signal, pageParam }) => managementApi.getCacheMembers(signal, pageParam),
    getNextPageParam: (page) => page.nextCursor ?? undefined,
    ...listOptions,
  });
}
