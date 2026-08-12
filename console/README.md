# DingoFS MDS Management Console

This directory contains the React/TypeScript source for the embedded MDS management console.

## Development

```bash
npm ci --ignore-scripts
npm run dev
```

Vite serves the app under `/FsStatService/`. The development server needs a same-origin proxy to a running MDS for API requests.

## Generate embedded assets

The normal C++ build consumes the checked-in generated resource table and does not require Node.js. To rebuild the frontend and refresh the C++ asset table:

```bash
./scripts/build.sh
```

`src/types/generated.ts` is generated from `openapi.yaml`, and
`src/mds/service/fsstat_assets_generated.cc` is generated from `dist/` by
`console/scripts/generate_assets.py`. Do not edit either generated file by hand.

`npm run check:contracts` validates representative JSON fixtures against the
OpenAPI schemas. `npm run check:bundle` enforces the initial gzip budgets.
`./scripts/check-generated.sh` is the clean-tree drift check used by CI.

The production build intentionally does not emit source maps. The generated
resource table excludes `.map` files, uses content hashes for ETags, and is
compiled into `dingo-mds`.

## Migrated diagnostic routes

The console now includes the former legacy diagnostics as read-only pages:

- File-system details and directory browsing;
- deleted files, deleted slices, slice references, and file-system OpLog;
- inode details for live and deleted inodes;
- MDS server details, version information, distributed locks, ID generators,
  cache summary, and metadata key parsing.

Their versioned API resources live below `/FsStatService/api/v1`. The original
HTML routes remain available for compatibility through the Legacy Diagnostics
link.
