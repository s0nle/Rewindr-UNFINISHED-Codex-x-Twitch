import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildApp } from '../src/app.js';
import { LocalStore } from '../src/store.js';
import { parseChunkManifest } from '../src/manifest.js';
import { mapChunkMoments, chunkEncounters } from '../src/chunk-search.js';
import { importBroadcastSchema } from '../src/broadcasts.js';
import type { Broadcast, IndexedChunk, ProviderMoment, SearchSettings, VideoSearchProvider } from '../src/types.js';

const boundaries = [0, 60.043317, 120.036583, 180.046533, 240.0398, 300.03];
const chunks = boundaries.slice(0, -1).map((startSeconds, i) => ({
  filename: `chunk_${String(i).padStart(3, '0')}.mp4`, startSeconds, endSeconds: boundaries[i + 1]!
}));
class ChunkProvider implements VideoSearchProvider {
  readonly name = 'twelvelabs' as const;
  catalog: IndexedChunk[] = chunks.map((chunk, i) => ({ indexedAssetId: `indexed-${i}`, assetId: `asset-${i}`,
    filename: chunk.filename, durationSeconds: chunk.endSeconds - chunk.startSeconds, status: 'ready' }));
  rows: ProviderMoment[] = [
    { videoId: 'indexed-0', startSeconds: 59, endSeconds: 60, rank: 5 },
    { videoId: 'indexed-1', startSeconds: 0, endSeconds: 0.9, rank: 2 },
    { videoId: 'indexed-2', startSeconds: 30, endSeconds: 35, rank: 1 },
    { videoId: 'indexed-3', startSeconds: 30, endSeconds: 35, rank: 3 },
    { videoId: 'indexed-4', startSeconds: 30, endSeconds: 35, rank: 4 }
  ];
  calls: { ids: string[]; question: string; limit: number; settings: SearchSettings }[] = [];
  async ensureIndex(): Promise<never> { throw new Error('Import must not create indexes.'); }
  async createUpload(): Promise<never> { throw new Error('Import must not upload video.'); }
  async transfer(): Promise<never> { throw new Error('Import must not transfer video.'); }
  async assetStatus(): Promise<never> { throw new Error('Unexpected asset polling.'); }
  async indexAsset(): Promise<never> { throw new Error('Import must not index video.'); }
  async indexStatus(): Promise<never> { throw new Error('Unexpected legacy indexing.'); }
  async search(): Promise<never> { throw new Error('Expected one multi-chunk search.'); }
  async listIndexedChunks() { return this.catalog; }
  async searchMany(_index: string, ids: string[], question: string, limit: number, settings: SearchSettings) {
    this.calls.push({ ids, question, limit, settings }); return this.rows;
  }
}
const manifest = { title: 'Recorded test', indexId: 'test-index', sourceOffsetSeconds: 3600, chunks };
async function setup(t: { after: (fn: () => Promise<void>) => void }) {
  const dir = await mkdtemp(join(tmpdir(), 'rewindr-chunks-'));
  const provider = new ChunkProvider();
  const app = await buildApp({ dataDir: dir, provider });
  t.after(async () => {
    await app.close();
    if (!dir.startsWith(join(tmpdir(), 'rewindr-chunks-'))) throw new Error('Unexpected cleanup target.');
    await rm(dir, { recursive: true, force: true });
  });
  const register = (payload: Record<string, unknown> = manifest) => app.inject({ method: 'POST', url: '/broadcasts/import',
    headers: { 'x-creator-authorized': 'true' }, payload });
  return { dir, provider, app, register };
}

test('search uses all registered chunk IDs, maps measured offsets, merges a boundary encounter and displays three candidates', async t => {
  const { app, provider, register } = await setup(t);
  const imported = await register();
  assert.equal(imported.statusCode, 201, imported.body);
  const id = imported.json().broadcastId;
  const result = await app.inject({ method: 'POST', url: `/broadcasts/${id}/search`, payload: {
    question: 'Guy in a hoodie asks for a photo', candidateLimit: 50, modalities: ['visual', 'transcription'],
    operator: 'or', transcriptionOptions: ['semantic']
  } });
  assert.equal(result.statusCode, 200, result.body);
  const body = result.json();
  assert.deepEqual(provider.calls[0]!.ids, ['indexed-0', 'indexed-1', 'indexed-2', 'indexed-3', 'indexed-4']);
  assert.equal(provider.calls[0]!.question, 'Guy in a hoodie asks for a photo');
  assert.equal(provider.calls[0]!.limit, 50);
  assert.deepEqual(provider.calls[0]!.settings, { modalities: ['visual', 'transcription'], operator: 'or', transcriptionOptions: ['semantic'] });
  assert.equal(body.encounters.length, 3);
  assert.equal(body.moreCandidatesAvailable, true);
  assert.equal(body.encounters[1].providerMatches.length, 2);
  assert.equal(body.encounters[1].matchStartSeconds, 59);
  assert.ok(Math.abs(body.encounters[1].matchEndSeconds - 60.943317) < 0.000001);
  assert.ok(Math.abs(body.moments[0].recordingStartSeconds - 150.036583) < 0.000001);
  assert.ok(Math.abs(body.moments[0].broadcastStartSeconds - 3750.036583) < 0.000001);
  assert.equal(body.encounters[0].playbackUrl, null);
  assert.equal(body.encounters[0].replayAvailable, false);
  assert.equal(body.encounters[0].analysisStatus, 'not_implemented');
  assert.equal(body.moments[0].score, undefined);
  const status = await app.inject(`/broadcasts/${id}/status`);
  assert.equal(status.json().searchableThroughSeconds, 300.03);
});

test('import rejects overlaps, unready chunks, ambiguous filenames and duration mismatches without uploading', async t => {
  const { app, provider, register } = await setup(t);
  const missingAuthorization = await app.inject({ method: 'POST', url: '/broadcasts/import', payload: manifest });
  assert.equal(missingAuthorization.statusCode, 400);
  assert.equal((await register({ ...manifest, chunks: [chunks[0], { ...chunks[1], startSeconds: 55 }] })).statusCode, 400);
  provider.catalog[0]!.status = 'indexing';
  assert.equal((await register()).statusCode, 409);
  provider.catalog[0]!.status = 'ready';
  provider.catalog.push({ ...provider.catalog[0]!, indexedAssetId: 'duplicate' });
  assert.equal((await register()).statusCode, 409);
  provider.catalog.pop();
  provider.catalog[0]!.durationSeconds = 90;
  assert.equal((await register()).statusCode, 409);
  assert.equal(provider.calls.length, 0);
});

test('search validates settings, rejects unregistered sources and invalid times, and never pads empty results', async t => {
  const { app, provider, register } = await setup(t);
  const id = (await register()).json().broadcastId;
  const search = (payload: Record<string, unknown>) => app.inject({ method: 'POST', url: `/broadcasts/${id}/search`, payload });
  for (const payload of [{ question: '' }, { question: 'hoodie', candidateLimit: 51 },
    { question: 'hoodie', modalities: [] }, { question: 'hoodie', modalities: ['visual', 'visual'] },
    { question: 'hoodie', operator: 'invalid' }, { question: 'hoodie', rewrite: true }]) {
    assert.equal((await search(payload)).statusCode, 400);
  }
  assert.equal(provider.calls.length, 0);
  provider.rows = [{ videoId: 'unregistered', startSeconds: 0, endSeconds: 5, rank: 1 }];
  assert.equal((await search({ question: 'hoodie' })).statusCode, 502);
  provider.rows = [{ videoId: 'indexed-0', startSeconds: 59, endSeconds: 90, rank: 1 }];
  assert.equal((await search({ question: 'hoodie' })).statusCode, 502);
  provider.rows = [];
  const empty = await search({ question: 'absent event' });
  assert.equal(empty.statusCode, 200);
  assert.deepEqual(empty.json().encounters, []);
  assert.deepEqual(provider.calls.at(-1)!.settings.modalities, ['visual']);
  assert.equal((await app.inject({ method: 'POST', url: '/broadcasts/missing/search', payload: { question: 'test' } })).statusCode, 404);
});

test('broadcast registration survives restart and source replay seeks recording time rather than full-stream offset', async t => {
  const { app, dir, provider } = await setup(t);
  const store = new LocalStore(dir); await store.init();
  const sourceId = '5fdbb533-04d3-4e3c-9e15-69e440bd5681';
  await store.put({ id: sourceId, filename: 'source.mp4', bytes: 1, sha256: '0'.repeat(64), creatorAuthorized: true,
    authorizedAt: new Date().toISOString(), createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    phase: 'ready', media: { durationSeconds: 1800, width: 1280, height: 720, videoCodec: 'h264' } });
  // Restart so the application loads the existing source metadata, as the real local app does.
  await app.close();
  const restarted = await buildApp({ dataDir: dir, provider }); t.after(() => restarted.close());
  const response = await restarted.inject({ method: 'POST', url: '/broadcasts/import', headers: { 'x-creator-authorized': 'true' },
    payload: { ...manifest, sourceRecordingId: sourceId } });
  assert.equal(response.statusCode, 201, response.body);
  const id = response.json().broadcastId;
  await restarted.close();
  const again = await buildApp({ dataDir: dir, provider }); t.after(() => again.close());
  const result = await again.inject({ method: 'POST', url: `/broadcasts/${id}/search`, payload: { question: 'hoodie' } });
  assert.equal(result.statusCode, 200, result.body);
  assert.equal(result.json().encounters[0].replayAvailable, true);
  assert.equal(result.json().moments[0].playbackUrl, `/recordings/${sourceId}/source#t=150.036583`);
  const missingSource = await again.inject({ method: 'POST', url: '/broadcasts/import', headers: { 'x-creator-authorized': 'true' },
    payload: { ...manifest, sourceRecordingId: 'c0d3ef27-e66b-4ba2-8c99-f5c1ec69826c' } });
  assert.equal(missingSource.statusCode, 400);
});

test('manifest parser retains measured offsets, accepts both existing CSV formats and rejects malformed rows', () => {
  assert.deepEqual(parseChunkManifest('chunk_000.mp4,0.000000,60.043317\r\nchunk_001.mp4,60.043317,120.036583\r\n'), chunks.slice(0, 2));
  assert.deepEqual(parseChunkManifest('\uFEFF"filename","start_seconds","end_seconds"\n"chunk_000.mp4","0","60.043317"'), chunks.slice(0, 1));
  for (const input of ['', 'filename,start,end', 'chunk.mp4,0,NaN', 'chunk.mp4,60,0', 'chunk.mp4,0', 'chunk.mp4, ,60', 'chunk.mp4,0, ']) {
    assert.throws(() => parseChunkManifest(input));
  }
});

test('filename validation accepts ordinary MP4 names and Unicode punctuation while rejecting paths and control characters', () => {
  const valid = ['chunk_019.mp4', 'chunk_019.MP4', "IRL (019) — café's.mp4", 'part, one.mp4'];
  for (const name of valid) {
    assert.equal(importBroadcastSchema.safeParse({ ...manifest, chunks: [{ ...chunks[0], filename: name }] }).success, true, name);
  }
  const invalid = ['../clip.mp4', '..\\clip.mp4', 'C:\\clip.mp4', '/clip.mp4', 'clip.mp4\n', 'clip\r.mp4',
    'clip\0.mp4', 'clip\u007f.mp4', 'clip?.mp4', 'clip*.mp4', 'clip:stream.mp4', 'clip.mp4.exe',
    'clipXmp4', 'clip.mp4 ', '.mp4', 'CON.mp4', 'COM1.backup.mp4', 'clip.mp4\u2028', 'clip\u0085.mp4'];
  for (const name of invalid) {
    assert.equal(importBroadcastSchema.safeParse({ ...manifest, chunks: [{ ...chunks[0], filename: name }] }).success, false, JSON.stringify(name));
  }
});

test('manifest parser preserves quoted comma/Unicode filenames and rejects broken CSV quoting', () => {
  const name = "IRL, part (019) — café's.mp4";
  assert.deepEqual(parseChunkManifest(`"${name}",60.043317,120.036583`), [
    { filename: name, startSeconds: 60.043317, endSeconds: 120.036583 }
  ]);
  for (const text of ['"unfinished.mp4,0,60', '"chunk.mp4"junk,0,60', 'chunk"oops.mp4,0,60',
    '"../chunk.mp4",0,60', '"chunk.mp4\n",0,60']) assert.throws(() => parseChunkManifest(text), text);
});

test('import preserves a Unicode filename exactly when matching and persisting the indexed asset', async t => {
  const { app, provider, register } = await setup(t);
  const name = "IRL, part (019) — café's.MP4";
  provider.catalog[0]!.filename = name;
  const response = await register({ ...manifest, chunks: [{ ...chunks[0], filename: name }] });
  assert.equal(response.statusCode, 201, response.body);
  const status = await app.inject(response.json().statusUrl);
  assert.equal(status.json().chunks[0].filename, name);
  assert.equal(status.json().chunks[0].indexedAssetId, 'indexed-0');
});

test('grouping does not merge matches across a missing stretch of footage', () => {
  const broadcast: Broadcast = { id: 'test', title: 'Gap', indexId: 'index', createdAt: '', sourceOffsetSeconds: 0,
    chunks: [{ filename: 'a.mp4', indexedAssetId: 'a', startSeconds: 0, endSeconds: 60 },
      { filename: 'b.mp4', indexedAssetId: 'b', startSeconds: 62, endSeconds: 122 }] };
  const mapped = mapChunkMoments(broadcast, [{ videoId: 'a', startSeconds: 59, endSeconds: 60, rank: 1 },
    { videoId: 'b', startSeconds: 0, endSeconds: 1, rank: 2 }]);
  assert.equal(chunkEncounters(broadcast, mapped).length, 2);
});
