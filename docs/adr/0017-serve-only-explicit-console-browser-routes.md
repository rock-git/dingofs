# Serve only explicit console browser routes

The React console will use readable browser-history routes, and `FsStatServiceImpl` will serve the application shell only for an explicit set of console routes. Legacy HTML paths and `/api/v1/*` retain their own handlers, while unknown paths return 404 rather than falling back indiscriminately to React; route-contract tests will protect direct navigation and prevent new SPA routes from colliding with legacy endpoints.
