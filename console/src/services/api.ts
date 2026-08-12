import type {
  CacheMember,
  Client,
  Filesystem,
  ListResponse,
  MdsNode,
  Overview,
} from '../types/api';

const API_ROOT = '/FsStatService/api/v1';
const CORE_TIMEOUT_MS = 10_000;

export class ManagementApiError extends Error {
  readonly code: string;
  readonly requestId: string;
  readonly status: number;

  constructor(code: string, message: string, requestId: string, status: number) {
    super(message);
    this.name = 'ManagementApiError';
    this.code = code;
    this.requestId = requestId;
    this.status = status;
  }
}

function withTimeout(signal: AbortSignal | undefined, timeoutMs: number): AbortSignal {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort('client_timeout'), timeoutMs);
  const abort = () => controller.abort(signal?.reason);
  signal?.addEventListener('abort', abort, { once: true });
  controller.signal.addEventListener('abort', () => {
    window.clearTimeout(timer);
    signal?.removeEventListener('abort', abort);
  }, { once: true });
  return controller.signal;
}

async function request<T extends object>(path: string, signal?: AbortSignal, timeoutMs = CORE_TIMEOUT_MS): Promise<T> {
  const requestSignal = withTimeout(signal, timeoutMs);
  let response: Response;
  try {
    response = await fetch(`${API_ROOT}${path}`, {
      method: 'GET',
      headers: { Accept: 'application/json' },
      signal: requestSignal,
      credentials: 'same-origin',
    });
  } catch (error) {
    if (requestSignal.aborted && requestSignal.reason === 'client_timeout') {
      throw new ManagementApiError('client_timeout', 'The request timed out.', '', 0);
    }
    throw error;
  }

  const requestId = response.headers.get('X-Request-Id') ?? '';
  const contentType = response.headers.get('Content-Type') ?? '';
  if (!contentType.includes('application/json')) {
    throw new ManagementApiError('invalid_content_type', 'The server returned an unexpected response.', requestId, response.status);
  }

  const body = await response.json() as T | { error?: { code?: string; message?: string; requestId?: string } };
  if (!response.ok) {
    const error = 'error' in body ? body.error : undefined;
    throw new ManagementApiError(
      error?.code ?? 'request_failed',
      error?.message ?? `Request failed with HTTP ${response.status}.`,
      error?.requestId ?? requestId,
      response.status,
    );
  }
  return body as T;
}

export const managementApi = {
  getOverview: (signal?: AbortSignal) => request<Overview>('/overview', signal),
  getFilesystems: (signal?: AbortSignal, cursor?: string | null) =>
    request<ListResponse<Filesystem>>(`/filesystems?limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, signal),
  getMdsNodes: (signal?: AbortSignal, cursor?: string | null) =>
    request<ListResponse<MdsNode>>(`/mds-nodes?limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, signal),
  getClients: (signal?: AbortSignal, cursor?: string | null) =>
    request<ListResponse<Client>>(`/clients?limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, signal),
  getCacheMembers: (signal?: AbortSignal, cursor?: string | null) =>
    request<ListResponse<CacheMember>>(`/cache-members?limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, signal),
  getDiagnostic: <T extends object>(path: string, signal?: AbortSignal) => request<T>(path, signal, 15_000),
  getRemaining: <T extends object>(path: string, signal?: AbortSignal) => request<T>(path, signal, 20_000),
};
