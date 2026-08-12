# Evaluate operational health on the server

Management API resources will expose both the raw operational facts, such as the last heartbeat time, and the MDS-derived health assessment with a stable state and reason. The React application will present that assessment rather than duplicate timeout flags and classification rules, while the raw facts remain available so operators can understand and diagnose the result.
