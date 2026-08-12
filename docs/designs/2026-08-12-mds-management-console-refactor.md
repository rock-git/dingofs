# MDS Management Console Refactor

## Status

Design agreed on 2026-08-12. This document describes the target and delivery sequence; it does not authorize implementation by itself.

## Context

`FsStatServiceImpl` is declared in `src/mds/service/fsstat_service.h`, but nearly all HTTP behavior lives in the roughly 2,100-line `src/mds/service/fsstat_service.cc`. That file currently mixes bRPC routing, MDS data access, HTML/CSS/JavaScript generation, JSON endpoints, status interpretation, and more than ten diagnostic pages.

The goal is to establish a maintainable Management Console and Management API that can absorb more complex read-only diagnostic features without changing the existing single-binary, offline-capable deployment model.

## Scope

The first console increment includes:

- an Overview;
- File Systems resource list;
- MDS Nodes resource list;
- Clients resource list;
- Cache Members resource list;
- explicit links to bRPC Server Tools and unmigrated legacy diagnostics.

The Overview shows cluster identity, build information, resource summaries, failures, and bounded previews. It is not a complete resource dump and does not claim a unified cluster-health verdict.

The initial API and console are strictly read-only.

## Non-goals

The first increment will not:

- migrate quota, directory tree, chunk, shard, deleted-file, deleted-slice, slice-reference, op-log, or other detail pages;
- add management mutations;
- add authentication or authorization;
- add polling, SSE, or WebSocket updates;
- add a dedicated Management API WorkerSet or concurrency limiter;
- provide snapshot-consistent pagination;
- add Playwright end-to-end tests;
- provide formal accessibility acceptance;
- systematically remediate HTML injection throughout legacy rendering;
- make the Management API a core client protocol.

## Terminology

The canonical terms are recorded in `CONTEXT.md`:

- **Management Console**: the browser interface used to inspect and diagnose MDS and related resources.
- **Management API**: the structured read-only operations interface used by the console, separate from client-facing MDS RPCs.
- **Overview**: a bounded summary view for rapid situational awareness.
- **Resource List**: the complete browsing surface for one resource category.
- **Operational Health**: an MDS-derived operational assessment based on facts such as heartbeat timestamps. It is distinct from lifecycle state.

Resource categories retain their native status models. MDS and Client use online/offline semantics, Cache Member retains unknown/online/unstable/offline, and File System retains its lifecycle state. The console does not map these into a synthetic common health enum or a single cluster-health badge.

## Architecture

### C++ modules

The existing implementation will be separated into four modules:

```text
fsstat_service.{h,cc}
  bRPC adapter, request/response conversion, top-level dispatch, GetTabInfo

fsstat_api.{h,cc}
  /api/v1 routing, DTO construction, status interpretation,
  cursor handling, JSON success/error responses

fsstat_assets.{h,cc}
  embedded resource lookup, MIME, ETag, cache and security headers,
  React application entry

fsstat_legacy.{h,cc}
  unmigrated HTML renderers; deleted as migration completes
```

`FsStatServiceImpl` keeps a small public interface. The implementation must not create a public Handler class for every endpoint.

The Management API reads raw facts through an internal `ManagementDataSource` seam. A production adapter reads `Server`, `FileSystemSet`, heartbeat data, and server metadata. An in-memory adapter drives interface tests. Data-source results contain raw facts, not JSON or display formatting; the API module owns allowlisted DTOs, status semantics, cursors, and error mapping.

### Frontend modules

Frontend sources live under a dedicated directory such as:

```text
src/mds/service/fsstat_ui/
  components/
  hooks/
  pages/
  services/
  types/
  generated/
```

The project uses:

- React;
- TypeScript;
- Vite;
- shadcn/ui components copied selectively into the repository;
- Tailwind CSS with CSS-variable themes;
- TanStack Table;
- TanStack Query;
- React Router;
- Vitest and React Testing Library;
- ESLint;
- npm with `package-lock.json`.

No Ant Design, MUI, Redux, CSS-in-JS runtime, CDN dependency, or second component system is introduced.

A shared lightweight `managementApi` module uses OpenAPI-generated TypeScript types and provides a small method interface for Overview and the four resource categories. It owns same-origin URL construction, JSON/content-type checks, structured errors, request IDs, AbortSignal handling, and timeouts. Pages do not call `fetch` directly.

## HTTP routing

### Console routes

The React shell is served only for explicitly registered browser-history routes:

```text
/FsStatService/
/FsStatService/filesystems
/FsStatService/mds
/FsStatService/clients
/FsStatService/cache-members
```

Direct navigation and browser refresh must work. Unknown paths return 404; there is no catch-all SPA fallback that could swallow API or legacy routes.

The React application owns its navigation. It does not embed `brpc::Server::PrintTabsBody`; instead, it exposes a clearly marked Server Tools link to bRPC-native diagnostics.

### Management API routes

```text
/FsStatService/api/v1/overview
/FsStatService/api/v1/filesystems
/FsStatService/api/v1/mds-nodes
/FsStatService/api/v1/clients
/FsStatService/api/v1/cache-members
```

The API is same-origin and does not enable CORS. The browser does not fetch another MDS node directly. Cluster-wide data is gathered by the serving MDS; another node's native page may be opened through an ordinary link.

The API is primarily for the embedded console, but released v1 fields follow minimum compatibility discipline. Optional fields and endpoints may be added within v1. Removing or renaming fields, changing their types, or changing existing enum meanings requires a new API version or an explicit migration.

### Legacy routes

The complete old dashboard remains at:

```text
/FsStatService/legacy
```

The console links to it as Legacy Diagnostics. Existing detail routes continue to work while unmigrated. A request to `/FsStatService?key=...` redirects to `/FsStatService/legacy?key=...`, preserving and correctly encoding the parameter.

`/legacy` can be removed only after every capability it contains has a verified console replacement for at least one release. No runtime feature flag is added. Before the root switch, an explicit temporary preview route may be used and must be removed at final cutover.

## Management API contract

### OpenAPI

A checked-in OpenAPI 3.1 document is the source of truth for routes, parameters, status codes, errors, enums, list envelopes, and exact-integer encoding. It generates TypeScript types but not a C++ server framework or a generated frontend runtime client.

Representative C++ response fixtures are validated against the OpenAPI schema in CI. Production browser code does not ship a runtime schema validator.

### Resource envelope

Core resource endpoints return their summary and bounded items together so Overview does not query the same source twice:

```json
{
  "summary": {},
  "items": [],
  "nextCursor": null,
  "generatedAt": "2026-08-12T08:30:00.123Z"
}
```

`/overview` contains only independent lightweight data such as cluster ID, serving MDS identity, storage engine, build/Git information, and Management API version. Resource counts and previews come from the corresponding resource responses.

### Pagination

The UI defaults to 25 rows and offers 25, 50, and 100. API pages default to 1,000 items and allow at most 5,000, subject to a separately measured serialized-byte ceiling.

Continuation uses an opaque, stateless keyset cursor. Results are stably ordered with the resource ID as the final tie-breaker. The current implementation may still fetch the complete backend collection and only bound serialization; future storage-native pagination must preserve the HTTP contract.

Cursors provide best-effort traversal, not a transaction snapshot. Every page has its own `generatedAt`. Invalid or expired cursors produce a structured `invalid_cursor` error. The console provides a full reload when data changes between pages.

When a resource collection fits in one API page, the browser performs search, sorting, and UI pagination locally. If `nextCursor` is present, the UI clearly indicates that more records exist and permits continuation loading.

### Exact values and formatting

- Identifiers, inode values, and other exact 64-bit integers are decimal JSON strings.
- Small bounded counts, pagination values, and enums may be JSON numbers.
- Timestamps are ISO 8601 UTC strings.
- Capacities and durations use exact base units, not display strings.
- The UI formats bytes with IEC units (KiB/MiB/GiB) and exposes exact bytes.
- Time defaults to browser-local display with an explicit timezone; UTC is available in detail/tooltip form.
- Relative time is calculated on load or manual refresh and does not start a timer.

### Status and health

The API returns both raw facts, such as last-seen timestamps, and MDS-derived native status/health results with stable machine values and reasons. React presents these values rather than duplicating heartbeat thresholds. File-system lifecycle state remains separate from operational health.

### Allowlisted DTOs

Management responses are explicit DTOs, never transparent Protobuf serialization. Storage diagnostic data may include type, endpoint, bucket, pool, and similar metadata. It must never include:

- S3 access key;
- S3 secret key;
- Rados key;
- future Protobuf fields that have not been explicitly reviewed.

The existing legacy file-system details response must also omit or redact credentials. Tests use sentinel secret values and assert that they never appear in responses.

### Errors

Failures use meaningful HTTP status codes and a structured body:

```json
{
  "error": {
    "code": "filesystem_not_found",
    "message": "File system 12 was not found",
    "requestId": "..."
  }
}
```

Expected mappings include 400 for invalid parameters, 404 for missing resources, 409 for conflicts where applicable, 429 for explicit request limiting if later introduced, 500 for internal failures, and 503 for unavailable dependencies. Success responses do not use a generic `success: true` wrapper. A failed module is shown as failed, not as an empty collection.

## Console behavior

### Overview

Overview loads the lightweight overview endpoint and four resource endpoints independently and in parallel. Each module owns its loading, empty, error, success, and last-successful-update state. Failure in one module does not suppress the others.

Overview shows category-specific counts and bounded previews, for example MDS online/offline counts, Cache Member native state counts, and File System lifecycle counts. It does not show a unified cluster-health verdict.

### Refresh

There is no automatic refresh, focus refresh, retry loop, polling, SSE, or WebSocket in the first increment.

The header provides Refresh All, which refetches modules independently in parallel. Every module also provides refresh/retry. During refresh, old successful data remains visible and is marked as refreshing. Data remains in TanStack Query memory only and is not persisted.

Core resource requests time out after 10 seconds, ordinary detail requests after 15 seconds, and future expensive diagnostics may explicitly opt into at most 30 seconds. Navigation or replacement requests abort obsolete work. Timeouts produce a stable `client_timeout` error and are not automatically retried.

### Resource lists

All four resource lists provide search, sorting, local pagination when possible, loading/empty/error states, and continuation loading when required.

File Systems defaults to the following columns:

```text
ID, Name, Lifecycle State, Type, Partition Type, Owner,
Capacity, Mount Points, Updated At, Actions
```

Additional allowlisted information appears in a detail side panel: UUID/version, block/chunk sizes, partition policy, storage metadata, UID/GID map, dir-stat and trash configuration, and timestamps. The list endpoint returns the allowlisted detail data up front; opening the panel does not issue another request.

Each file-system row uses a grouped Actions menu rather than a wall of links. React routes open in the current tab. Legacy diagnostics and external MDS/bRPC pages open in a new tab with `noopener noreferrer`. Unavailable actions are disabled with a reason rather than silently hidden.

### URL and local preferences

Shareable list state—filters, sort, and page—is encoded in URL query parameters. Personal presentation preferences—Light/Dark/System theme, page size, visible columns, and navigation collapse—may be stored in localStorage. API responses, errors, storage metadata, and resource details are never stored there. Invalid or obsolete saved values fall back safely.

The console language is English. Machine fields, enums, and error codes remain stable English values. Internationalization is not introduced in the first increment.

### Theme and browsers

The console supports Light, Dark, and System themes using CSS variables; System is the default. Legacy and bRPC-native pages do not inherit the console theme.

Supported targets are the most recent two major releases of Chrome/Edge, Firefox, and Safari. Internet Explorer and large legacy-browser polyfill bundles are not supported. The first increment has no dedicated accessibility acceptance beyond basic desktop usability and textual status labels.

## Asset build and serving

Vite produces hashed HTML/CSS/JavaScript assets. A deterministic repository script converts the build output into checked-in generated C++ resource-table files containing URL paths, byte arrays, MIME types, content hashes/ETags, lengths, and entry metadata. Generation uses fixed ordering and no timestamps; generated files are marked `DO NOT EDIT`.

Normal C++ builds compile the checked-in resource table and do not require Node.js. A dedicated frontend CI path uses a pinned Node.js LTS version and:

```bash
npm ci
npm run check
npm run test
npm run build
```

CI rebuilds generated assets and fails on drift. The dependency policy for the first increment is limited to npm's lockfile and clean install; no dedicated npm vulnerability or license gate is added.

bRPC performs HTTP compression; raw and gzip copies are not both embedded initially. Route-level lazy loading splits File Systems, MDS, Clients, and Cache Members from the initial shell. More granular component splitting is avoided.

Production source maps are not embedded or served. CI/release may retain them as separate diagnostic artifacts, and emitted production assets must not reference a publicly served map.

Initial compressed-size budgets are approximately:

- first-load JavaScript: 300 KiB gzip;
- CSS: 100 KiB gzip;
- individual asynchronous chunk: 200 KiB gzip.

CI reports raw and gzip sizes and fails on an absolute-budget violation or an unexplained increase greater than 20% from the calibrated baseline.

## HTTP caching and security headers

Caching is response-specific:

- SPA entry and browser routes: `Cache-Control: no-cache`;
- hashed JS/CSS/font assets: `public, max-age=31536000, immutable`;
- Management API: `no-store`;
- legacy HTML: `no-store`.

Embedded assets support ETag and `If-None-Match`.

The React console has a security policy independent of legacy pages. Its response includes a restrictive CSP with same-origin scripts and connections, no objects, no base URI, and no framing. Inline scripts and eval are prohibited. Inline styles remain permitted where Radix/shadcn positioning requires them. The console also sends `X-Content-Type-Options: nosniff` and `Referrer-Policy: no-referrer`.

Legacy pages retain a separate, weaker policy while their inline scripts remain. The first increment fixes credential exposure but deliberately does not systematically escape all legacy HTML. New React code must not use `dangerouslySetInnerHTML` for server data, and new functionality must not be added to the legacy renderer.

The read-only console adds no application-layer authentication. It relies on the existing network trust boundary around the MDS HTTP port, which deployment documentation must state explicitly. This decision must be revisited before any mutation is introduced.

## Logging and operational protection

Successful Management API requests log at DEBUG/VLOG. Slow requests and failures use higher levels. Structured fields include request ID, normalized route, HTTP status, duration, and item count. Logs do not include cursor values, raw Parse Key input, complete query strings, response bodies, credentials, or storage configurations. The initial slow-request threshold is one second and may be calibrated from observation.

The first increment does not add a concurrency limiter or dedicated WorkerSet. It does bound serialized pages and response bytes, records slow requests, and avoids automatic retries. This is an explicit simplification rather than a claim that management reads are cost-free.

## Testing

### C++

Interface tests cover:

- Management API routes and strict parameter parsing;
- successful, empty, failure, and continuation responses;
- exact 64-bit encoding;
- resource-native status and reason fields;
- credential exclusion;
- cursor validation and stable ordering;
- structured HTTP errors and request IDs;
- SPA/asset/legacy dispatch and unknown-route 404;
- MIME, ETag, CSP, and caching headers;
- `?key=` compatibility redirect.

Tests use the in-memory `ManagementDataSource` adapter rather than the global Server singleton.

### Frontend

Vitest and React Testing Library cover:

- loading, empty, error, success, and refreshing states;
- independent refresh and Refresh All;
- search, sort, UI pagination, and continuation;
- preservation of exact 64-bit string IDs;
- URL state and safe localStorage fallback;
- Actions, legacy links, and Server Tools links;
- themes and route-level error fallback.

### Contract and build

CI validates representative C++ JSON fixtures against OpenAPI, checks generated TypeScript and embedded assets for drift, applies bundle budgets, and verifies that every emitted hashed chunk is served by the embedded resource table.

Playwright is deferred. Before release, the latest supported desktop browsers receive a manual smoke test.

## Delivery plan

### Phase 1: security and legacy extraction

- redact S3/Rados credentials from legacy details;
- extract existing HTML rendering into `fsstat_legacy` without intentional behavior changes;
- add regression tests for credential exclusion;
- keep current root and detail URLs working.

### Phase 2: Management API

- introduce the internal data-source seam and adapters;
- add OpenAPI 3.1;
- implement overview and four core resource endpoints;
- implement DTOs, native statuses, cursors, errors, logs, and limits;
- add C++ and contract tests.

### Phase 3: frontend build and embedding

- add React/Vite/npm and the agreed UI/data stack;
- implement deterministic asset generation and CMake integration;
- add cache/security headers, CI drift checks, source-map handling, and size budgets;
- expose an explicit temporary preview entry.

### Phase 4: console and cutover

- implement the shell, Overview, four resource lists, details, Actions, manual refresh, themes, and tests;
- switch `/FsStatService/` to React;
- move the old dashboard to `/FsStatService/legacy`;
- preserve `?key=` through redirect;
- add `<noscript>` and React Error Boundary links to legacy;
- remove the temporary preview route.

Every phase must build and pass its own tests independently. Refactoring and user-visible cutover must not be hidden in one indivisible change.

## Acceptance criteria

The first increment is complete only when:

1. Overview and all four core resource lists support their agreed display, search, sorting, pagination/continuation, Actions, detail, refresh, theme, and independent state behavior.
2. `/api/v1` conforms to OpenAPI, preserves exact values, returns structured errors, and excludes all storage credentials from new and legacy responses.
3. A normal C++ build succeeds without Node.js; clean npm checks/tests/build pass; generated output is current; bundle budgets pass; every chunk is embedded and served.
4. Legacy detail URLs, `/legacy`, and `?key=` compatibility work; direct SPA navigation works; unknown paths return 404; no-script and runtime failures provide a legacy fallback.
5. C++ interface tests, frontend behavior tests, OpenAPI fixture validation, asset checks, and manual supported-browser smoke tests pass.

## Accepted residual risks

- Access depends on network isolation rather than application authentication.
- Legacy HTML remains broadly unescaped and retains a weaker CSP until deletion.
- Management reads have bounded responses but no concurrency isolation.
- The first increment has no formal accessibility program.
- npm dependencies have a lockfile but no dedicated vulnerability/license CI gate.
- Cursor traversal is best-effort rather than snapshot-consistent.
