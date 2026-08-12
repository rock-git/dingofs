#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
npm ci --ignore-scripts
npm run generate:types
npm run check
npm run test
npm run build
npm run check:bundle
npm run check:contracts
python3 scripts/generate_assets.py
