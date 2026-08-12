# Describe the management API with OpenAPI

An OpenAPI 3.1 document will be the checked-in contract for `/FsStatService/api/v1/*`, including routes, status codes, structured errors, health enums, list envelopes, and exact-integer string encoding. TypeScript types will be generated from this document and checked for drift in CI, while the C++ implementation remains explicit rather than introducing a C++ OpenAPI server-generation framework.
