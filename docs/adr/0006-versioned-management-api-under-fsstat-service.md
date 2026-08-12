# Introduce a versioned management API under FsStatService

The management console will be served from `/FsStatService/`, with embedded assets under `/FsStatService/assets/*` and structured data under `/FsStatService/api/v1/*`. Existing HTML endpoints will remain available while their pages are migrated, allowing the console to move one capability at a time without breaking operational bookmarks or workflows; after migration, an old page URL may route into the React application.
