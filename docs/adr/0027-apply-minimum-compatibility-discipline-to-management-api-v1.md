# Apply minimum compatibility discipline to Management API v1

`/FsStatService/api/v1/*` is a read-only operations interface primarily for the embedded console, not a core client protocol, but released v1 schemas will still follow minimum compatibility discipline: optional fields and endpoints may be added, while field removal, renaming, type changes, or changed enum meanings require a new API version or an explicit migration. OpenAPI will describe this scope without promising the long-term stability level of MDS client RPCs.
