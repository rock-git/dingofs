# Retain the old dashboard as legacy diagnostics

Replacing `/FsStatService/` with the React overview must not remove the distributed-lock, ID-generator, cache-summary, key-parser, or version diagnostics still embedded in the old page. The complete old dashboard will therefore remain at `/FsStatService/legacy` and be linked as Legacy Diagnostics until every capability has a verified console replacement for at least one release, after which the legacy route and renderer can be deleted together.
