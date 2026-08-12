# Split FsStatService into API, asset, and legacy modules

`FsStatServiceImpl` will remain a small bRPC adapter and top-level dispatcher, while internal modules own the versioned management API, embedded-asset serving, and still-unmigrated legacy HTML respectively. This concentrates routing, serialization and health behavior behind testable interfaces without creating a shallow public handler class per endpoint, and makes the legacy renderer removable as migration completes.
