# Keep the initial management API read-only

The first management-console migration will expose only read operations, while preserving a future boundary for explicit management actions. No mutating endpoint will be added until the project defines authentication, authorization, CSRF protection, idempotency, target-node semantics, and audit logging; future operations must use explicit action endpoints rather than side-effecting GET requests.
