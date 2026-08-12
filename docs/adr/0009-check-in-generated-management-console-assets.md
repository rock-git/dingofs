# Check in generated management console assets

The repository will store the React and TypeScript sources, a pinned dependency lockfile, and the generated assets embedded by `dingo-mds`. Normal C++ builds will consume the checked-in assets without requiring Node.js, while a dedicated generation command and CI drift check will rebuild them and fail when committed output is stale; this preserves the existing C++ build environment without allowing frontend source and shipped output to diverge silently.
