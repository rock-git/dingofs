# Keep management API access same-origin

The browser console will call only the Management API of the MDS node that served it, and the API will not enable CORS. Cluster-wide information must be gathered by that node, while links to another MDS may navigate to its native page without cross-origin fetching; any future node aggregation belongs in a controlled server-side capability rather than a browser proxy or arbitrary remote request.
