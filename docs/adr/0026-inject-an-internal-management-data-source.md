# Inject an internal management data source

The Management API module will obtain raw resource facts through an internal `ManagementDataSource` seam rather than reach directly into the global `Server` singleton. A production adapter will read `FileSystemSet`, heartbeat, and server metadata, while an in-memory adapter will drive interface tests; routing, allowlisted DTO construction, status interpretation, pagination, and error serialization remain inside the deeper API module rather than leaking into the data-source interface.
