#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
npm run generate:types
npm run check:contracts
npm run build
npm run check:bundle
python3 scripts/generate_assets.py

cd ..
git diff --exit-code -- src/mds/service/fsstat_assets_generated.cc console/src/types/generated.ts
