import { readFile } from 'node:fs/promises';
import { parseChunkManifest } from '../src/manifest.js';

// Registration reads the existing index; it never uploads or re-indexes video.
const values = new Map<string, string>();
const allowed = new Set(['--manifest', '--index-id', '--title', '--source-recording-id', '--source-offset-seconds']);
const args = process.argv.slice(2);
for (let i = 0; i < args.length; i += 2) {
  const key = args[i]!;
  const value = args[i + 1];
  if (!allowed.has(key) || value === undefined || values.has(key) || value.startsWith('--')) throw new Error('Invalid or duplicate argument. See docs/chunk-backend.md.');
  values.set(key, value);
}
const indexId = values.get('--index-id') || process.env.TWELVELABS_INDEX_ID;
const manifest = values.get('--manifest');
if (!indexId || !manifest) throw new Error('Provide --manifest and --index-id (or TWELVELABS_INDEX_ID).');
const offset = Number(values.get('--source-offset-seconds') ?? 0);
if (!Number.isFinite(offset) || offset < 0) throw new Error('Source offset must be a nonnegative number of seconds.');
const port = Number(process.env.PORT || 3000);
if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('Invalid PORT.');
const response = await fetch(`http://127.0.0.1:${port}/broadcasts/import`, {
  method: 'POST', headers: { 'content-type': 'application/json', 'x-creator-authorized': 'true' },
  body: JSON.stringify({ title: values.get('--title') || 'Rewindr straight-cut test', indexId,
    sourceOffsetSeconds: offset, sourceRecordingId: values.get('--source-recording-id'),
    chunks: parseChunkManifest(await readFile(manifest, 'utf8')) }),
  signal: AbortSignal.timeout(180_000)
});
const body = await response.json() as { error?: string };
if (!response.ok) throw new Error(body.error || `Registration failed (${response.status}).`);
console.log(JSON.stringify(body, null, 2));
