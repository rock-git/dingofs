import type { components } from './generated';

export type Health = components['schemas']['Health'];
export type Summary = components['schemas']['Summary'];
export type Overview = components['schemas']['Overview'];
export type Filesystem = components['schemas']['Filesystem'];
export type MdsNode = components['schemas']['MdsNode'];
export type Client = components['schemas']['Client'];
export type CacheMember = components['schemas']['CacheMember'];

export type ListResponse<T> = {
  summary: Summary;
  items: T[];
  generatedAt: string;
  nextCursor: string | null;
  truncated?: boolean;
};
