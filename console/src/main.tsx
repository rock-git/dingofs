import { lazy, StrictMode, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { ConsoleLayout } from './components/ConsoleLayout';
import { ErrorBoundary } from './components/ErrorBoundary';
import { OverviewPage } from './pages/OverviewPage';
const FilesystemsPage = lazy(() => import('./pages/ResourcePages').then((module) => ({ default: module.FilesystemsPage })));
const MdsPage = lazy(() => import('./pages/ResourcePages').then((module) => ({ default: module.MdsPage })));
const ClientsPage = lazy(() => import('./pages/ResourcePages').then((module) => ({ default: module.ClientsPage })));
const CacheMembersPage = lazy(() => import('./pages/ResourcePages').then((module) => ({ default: module.CacheMembersPage })));
const DiagnosticsPage = lazy(() => import('./pages/DiagnosticsPage').then((module) => ({ default: module.DiagnosticsPage })));
const ServerPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.ServerPage })));
const VersionPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.VersionPage })));
const FilesystemDetailsPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.FilesystemDetailsPage })));
const DirectoryTreePage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.DirectoryTreePage })));
const InodeDetailsPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.InodeDetailsPage })));
const DeletedFilesPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.DeletedFilesPage })));
const DeletedSlicesPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.DeletedSlicesPage })));
const SliceReferencesPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.SliceReferencesPage })));
const OpLogPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.OpLogPage })));
const LocksPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.LocksPage })));
const IdGeneratorsPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.IdGeneratorsPage })));
const CacheSummaryPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.CacheSummaryPage })));
const ParseKeyPage = lazy(() => import('./pages/RemainingPages').then((module) => ({ default: module.ParseKeyPage })));
import './styles.css';

const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: Infinity, refetchOnWindowFocus: false, retry: false } } });

function NotFound() { return <div className="state-panel"><h2>Page not found</h2><a href="/FsStatService/legacy">Open Legacy Diagnostics</a></div>; }

createRoot(document.getElementById('root')!).render(<StrictMode><ErrorBoundary><QueryClientProvider client={queryClient}><BrowserRouter basename="/FsStatService"><Suspense fallback={<div className="state-panel"><span className="spinner" /> Loading page…</div>}><Routes><Route element={<ConsoleLayout />}><Route index element={<OverviewPage />} /><Route path="filesystems" element={<FilesystemsPage />} /><Route path="filesystems/:fsId" element={<FilesystemsPage />} /><Route path="filesystems/:fsId/details" element={<FilesystemDetailsPage />} /><Route path="filesystems/:fsId/tree" element={<DirectoryTreePage />} /><Route path="filesystems/:fsId/deleted-files" element={<DeletedFilesPage />} /><Route path="filesystems/:fsId/deleted-files/:ino" element={<InodeDetailsPage deleted />} /><Route path="filesystems/:fsId/deleted-slices" element={<DeletedSlicesPage />} /><Route path="filesystems/:fsId/slice-references" element={<SliceReferencesPage />} /><Route path="filesystems/:fsId/oplog" element={<OpLogPage />} /><Route path="filesystems/:fsId/inodes/:ino" element={<InodeDetailsPage />} /><Route path="filesystems/:fsId/quota" element={<DiagnosticsPage kind="quota" />} /><Route path="filesystems/:fsId/dir-stats" element={<DiagnosticsPage kind="dir-stats" />} /><Route path="filesystems/:fsId/mountpoints" element={<DiagnosticsPage kind="mountpoints" />} /><Route path="filesystems/:fsId/file-sessions" element={<DiagnosticsPage kind="file-sessions" />} /><Route path="filesystems/:fsId/files/:ino/chunks" element={<DiagnosticsPage kind="chunks" />} /><Route path="filesystems/:fsId/files/:ino/shard" element={<DiagnosticsPage kind="shard" />} /><Route path="server-details" element={<ServerPage />} /><Route path="version" element={<VersionPage />} /><Route path="locks" element={<LocksPage />} /><Route path="id-generators" element={<IdGeneratorsPage />} /><Route path="cache-summary" element={<CacheSummaryPage />} /><Route path="tools/parse-key" element={<ParseKeyPage />} /><Route path="mds" element={<MdsPage />} /><Route path="clients" element={<ClientsPage />} /><Route path="cache-members" element={<CacheMembersPage />} /><Route path="*" element={<NotFound />} /></Route></Routes></Suspense></BrowserRouter></QueryClientProvider></ErrorBoundary></StrictMode>);
