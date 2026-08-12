import { gzipSync } from 'node:zlib';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const assetsDir = path.join(root, 'dist', 'assets');
const files = (await readdir(assetsDir)).filter((file) => !file.endsWith('.map'));
const sizes = await Promise.all(files.map(async (file) => {
  const data = await readFile(path.join(assetsDir, file));
  return { file, raw: data.length, gzip: gzipSync(data, { level: 9 }).length };
}));
const js = sizes.filter(({ file }) => file.endsWith('.js'));
const css = sizes.filter(({ file }) => file.endsWith('.css'));
const initialJs = js.filter(({ file }) => file.startsWith('index-')).reduce((sum, item) => sum + item.gzip, 0);
const totalCss = css.reduce((sum, item) => sum + item.gzip, 0);
const oversizedChunks = sizes.filter(({ file, gzip }) => file !== 'index.html' && file !== 'index.css' && file.endsWith('.js') && gzip > 200 * 1024);

for (const item of sizes) console.log(`${item.file}: ${item.raw} bytes, ${item.gzip} bytes gzip`);
if (initialJs > 300 * 1024) throw new Error(`initial JavaScript exceeds 300 KiB gzip: ${initialJs}`);
if (totalCss > 100 * 1024) throw new Error(`CSS exceeds 100 KiB gzip: ${totalCss}`);
if (oversizedChunks.length > 0) throw new Error(`async chunk exceeds 200 KiB gzip: ${oversizedChunks.map(({ file }) => file).join(', ')}`);
