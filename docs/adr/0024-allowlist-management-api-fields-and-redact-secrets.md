# Allowlist management API fields and redact secrets

Management API responses will use explicit DTOs rather than serialize internal Protobuf messages wholesale. Storage type, endpoint, bucket, pool, and similar diagnostic metadata may be exposed, but S3 access keys, S3 secret keys, Rados keys, and future unreviewed fields must never enter a response; legacy file-system details will also omit or redact these credentials, with regression tests using sentinel secret values.
