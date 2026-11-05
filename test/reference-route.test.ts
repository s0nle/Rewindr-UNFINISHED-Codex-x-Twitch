import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildApp } from '../src/app.js';
import { LocalStore } from '../src/store.js';
import type { SearchSettings, VideoSearchProvider } from '../src/types.js';

const image = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jB1kAAAAASUVORK5CYII=', 'base64');
const recordingId = '5fdbb533-04d3-4e3c-9e15-69e440bd5681';
type Part = { field: string; value: string } | { field: string; filename: string; bytes: Buffer };
function multipart(parts: Part[]) {
  const boundary = 'rewindr-reference-route-test';
  const buffers: Buffer[] = [];
  for (const part of parts) {
    buffers.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${part.field}"`));
    if ('filename' in part) {
      buffers.push(Buffer.from(`; filename="${part.filename}"\r\nContent-Type: image/png\r\n\r\n`), part.bytes);
    } else buffers.push(Buffer.from(`\r\n\r\n${part.value}`));
    buffers.push(Buffer.from('\r\n'));
  }
  buffers.push(Buffer.from(`--${boundary}--\r\n`));
  return { headers: { 'content-type': `multipart/form-data; boundary=${boundary}` }, payload: Buffer.concat(buffers) };
}
class ReferenceProvider implements VideoSearchProvider {
  readonly name = 'twelvelabs' as const;
  fail = false;
  calls: { index: string; source: string; question: string; limit: number; path: string; bytes: Buffer; settings?: SearchSettings }[] = [];
  async ensureIndex(): Promise<never> { throw new Error('Unexpected indexing.'); }
  async createUpload(): Promise<never> { throw new Error('Unexpected upload.'); }
  async transfer(): Promise<never> { throw new Error('Unexpected transfer.'); }
  async assetStatus(): Promise<never> { throw new Error('Unexpected status polling.'); }
  async indexAsset(): Promise<never> { throw new Error('Unexpected indexing.'); }
  async indexStatus(): Promise<never> { throw new Error('Unexpected status polling.'); }
  async search(index: string, source: string, question: string, limit: number, path?: string, settings?: SearchSettings) {
    assert.ok(path, 'reference must exist while the provider runs');
    this.calls.push({ index, source, question, limit, path, bytes: await readFile(path), settings });
    if (this.fail) throw new Error('PRIVATE_PROVIDER_ERROR');
    return [{ videoId: 'indexed-source', startSeconds: 20, endSeconds: 30, rank: 1 }];
  }
}
async function setup(t: { after: (fn: () => Promise<void>) => void }, phase: 'ready' | 'indexing' = 'ready', configured = true) {
  const directory = await mkdtemp(join(tmpdir(), 'rewindr-reference-'));
  const store = new LocalStore(directory); await store.init();
  const now = new Date().toISOString();
  await store.put({ id: recordingId, filename: 'source.mp4', bytes: 1, sha256: '0'.repeat(64),
    creatorAuthorized: true, authorizedAt: now, createdAt: now, updatedAt: now, phase,
    indexId: 'index', assetId: 'asset', indexedAssetId: 'indexed-source',
    media: { durationSeconds: 1200, width: 1280, height: 720, videoCodec: 'h264' } });
  const provider = new ReferenceProvider();
  const app = await buildApp({ dataDir: directory, provider: configured ? provider : undefined });
  t.after(async () => {
    await app.close();
    if (!directory.startsWith(join(tmpdir(), 'rewindr-reference-'))) throw new Error('Unexpected cleanup directory.');
    await rm(directory, { recursive: true, force: true });
  });
  const send = (parts: Part[]) => app.inject({ method: 'POST', url: `/recordings/${recordingId}/search/reference`, ...multipart(parts) });
  const temporaryFiles = async () => (await readdir(directory)).filter(file => file.includes('.reference.'));
  return { app, provider, send, temporaryFiles };
}

test('reference route forwards actual PNG bytes and question in either multipart order, then removes its temporary file', async t => {
  const { provider, send, temporaryFiles } = await setup(t);
  const file = { field: 'reference', filename: 'reference.png', bytes: image };
  const question = { field: 'question', value: 'Woman wearing a floral dress' };
  for (const parts of [[file, question], [question, file]]) {
    const result = await send(parts);
    assert.equal(result.statusCode, 200, result.body);
    assert.equal(result.json().queryMode, 'text-and-image');
    assert.deepEqual(result.json().modalities, ['visual']);
    const call = provider.calls.at(-1)!;
    assert.deepEqual(call.bytes, image);
    assert.equal(call.question, question.value);
    assert.equal(call.index, 'index');
    assert.equal(call.source, 'indexed-source');
    assert.equal(call.limit, 30);
    assert.match(call.path, /\.reference\.png$/);
    await assert.rejects(readFile(call.path), { code: 'ENOENT' });
    assert.deepEqual(await temporaryFiles(), []);
  }
});

test('reference route supports image-only search without inventing a text query', async t => {
  const { provider, send } = await setup(t);
  const response = await send([{ field: 'reference', filename: 'reference.png', bytes: image }]);
  assert.equal(response.statusCode, 200, response.body);
  assert.equal(response.json().queryMode, 'image');
  assert.equal(provider.calls[0]!.question, '');
});

test('reference route removes the temporary image and sanitizes provider failure', async t => {
  const { provider, send, temporaryFiles } = await setup(t); provider.fail = true;
  const response = await send([{ field: 'reference', filename: 'reference.png', bytes: image }]);
  assert.equal(response.statusCode, 502, response.body);
  assert.doesNotMatch(response.body, /PRIVATE_PROVIDER_ERROR|\.reference\./);
  assert.equal(provider.calls.length, 1);
  assert.deepEqual(await temporaryFiles(), []);
});

test('reference route rejects missing images, wrong fields and non-image bytes without calling the provider', async t => {
  const { provider, send, temporaryFiles } = await setup(t);
  const cases: [Part[], number][] = [
    [[{ field: 'question', value: 'woman' }], 400],
    [[{ field: 'unexpected', filename: 'reference.png', bytes: image }], 400],
    [[{ field: 'unexpected', value: 'woman' }], 400],
    [[{ field: 'reference', filename: 'fake.png', bytes: Buffer.from('not a PNG') }], 415],
    [[{ field: 'reference', filename: 'reference.png', bytes: image }, { field: 'question', value: 'x'.repeat(1001) }], 400]
  ];
  for (const [parts, status] of cases) {
    const response = await send(parts);
    assert.equal(response.statusCode, status, response.body);
    assert.deepEqual(await temporaryFiles(), []);
  }
  assert.equal(provider.calls.length, 0);
});

test('reference route enforces file count and five-MiB limits and cleans partial work', async t => {
  const { provider, send, temporaryFiles } = await setup(t);
  const file = { field: 'reference', filename: 'reference.png', bytes: image };
  const duplicate = await send([file, file]);
  assert.equal(duplicate.statusCode, 413, duplicate.body);
  assert.deepEqual(await temporaryFiles(), []);
  const oversized = Buffer.alloc(5 * 1024 * 1024 + 1); image.copy(oversized);
  const tooLarge = await send([{ ...file, bytes: oversized }]);
  assert.equal(tooLarge.statusCode, 413, tooLarge.body);
  assert.deepEqual(await temporaryFiles(), []);
  assert.equal(provider.calls.length, 0);
});

test('reference route rejects JSON before reading an upload', async t => {
  const { app, provider } = await setup(t);
  const response = await app.inject({ method: 'POST', url: `/recordings/${recordingId}/search/reference`, payload: { question: 'woman' } });
  assert.equal(response.statusCode, 415, response.body);
  assert.equal(provider.calls.length, 0);
});

test('reference route requires an existing ready recording and configured provider', async t => {
  const { app, provider, send, temporaryFiles } = await setup(t, 'indexing', false);
  const parts = [{ field: 'reference', filename: 'reference.png', bytes: image }];
  assert.equal((await send(parts)).statusCode, 409);
  const missing = await app.inject({ method: 'POST', url: '/recordings/missing/search/reference', ...multipart(parts) });
  assert.equal(missing.statusCode, 404);
  assert.deepEqual(await temporaryFiles(), []);
  assert.equal(provider.calls.length, 0);
  const ready = await setup(t, 'ready', false);
  assert.equal((await ready.send(parts)).statusCode, 503);
  assert.deepEqual(await ready.temporaryFiles(), []);
});
