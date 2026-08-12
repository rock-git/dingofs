# Embed MDS management console assets in the server binary

The MDS management console will use a modern frontend build toolchain, but its compiled HTML, CSS, JavaScript, and other assets will be embedded in `dingo-mds`. This preserves the existing single-binary, offline-capable deployment model and avoids runtime dependencies on Node.js, a CDN, or a separately installed static-resource directory; updating the console therefore requires rebuilding and releasing MDS.
