import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createReadStream } from 'node:fs';
import { mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { setTimeout as delay } from 'node:timers/promises';
import ffmpegPackage from 'ffmpeg-static';
import { buildApp } from '../src/app.js';
import { probeMedia } from '../src/media.js';
import { LocalStore } from '../src/store.js';
import type { Recording, VideoSearchProvider } from '../src/types.js';

let root: string, fixture: string;
const ffmpeg = ffmpegPackage as unknown as string;
before(async () => {
  root = await mkdtemp(join(tmpdir(), 'rewindr-tests-'));
  fixture = join(root, 'synthetic-20min.mp4');
  assert.ok(ffmpeg);
  await promisify(execFile)(ffmpeg, ['-v','error','-f','lavfi','-i','color=c=blue:s=360x360:r=1','-t','1200','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-movflags','+faststart',fixture], { timeout: 60_000, windowsHide: true });
});
after(async () => { await rm(root, { recursive: true, force: true }); });

class MockProvider implements VideoSearchProvider {
  readonly name = 'twelvelabs' as const;
  state: 'ready' | 'failed' | 'indexing' = 'ready';
  asset: 'ready' | 'failed' | 'processing' = 'ready';
  submissions = 0;
  searchCalls = 0;
  pollFailures = 0;
  async ensureIndex() { return 'mock-index'; }
  async createUpload(_path: string, _filename: string, bytes: number) { return { uploadId: 'mock-upload', assetId: 'mock-asset', chunkSize: bytes, totalChunks: 1, completedChunks: 0, uploadHeaders: {} }; }
  async transfer(_path: string, _bytes: number, _session: unknown, checkpoint: (n: number) => Promise<void>) { await checkpoint(1); }
  async assetStatus() { return this.asset; }
  async indexAsset() { this.submissions++; return 'mock-indexed'; }
  async indexStatus() { if (this.pollFailures-- > 0) throw new Error('temporary outage'); return this.state; }
  async search() { this.searchCalls++; return [{ videoId: 'mock-indexed', startSeconds: 100, endSeconds: 112, rank: 1 }]; }
}
async function setup(provider: MockProvider | undefined = new MockProvider(), maxUploadBytes?: number) {
  const dir = await mkdtemp(join(root, 'api-'));
  const app = await buildApp({ dataDir: dir, provider, intervalMs: 5, maxUploadBytes });
  return { app, dir, provider };
}
async function upload(app: Awaited<ReturnType<typeof buildApp>>, path = fixture, authorized = true, field = 'recording') {
  const boundary = 'rewindr-test-boundary';
  async function* body() {
    yield Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${field}"; filename="source.mp4"\r\nContent-Type: video/mp4\r\n\r\n`);
    yield* createReadStream(path);
    yield Buffer.from(`\r\n--${boundary}--\r\n`);
  }
  return app.inject({ method: 'POST', url: '/recordings', headers: { 'content-type': `multipart/form-data; boundary=${boundary}`, ...(authorized ? { 'x-creator-authorized': 'true' } : {}) }, payload: Readable.from(body()) });
}
async function statusUntil(app: Awaited<ReturnType<typeof buildApp>>, id: string, phase: string) {
  for (let i = 0; i < 300; i++) {
    const result = await app.inject(`/recordings/${id}/status`);
    if (result.json().phase === phase) return result.json();
    await delay(10);
  }
  assert.fail(`Did not reach ${phase}: ${(await app.inject(`/recordings/${id}/status`)).body}`);
}

test('real ffprobe validates a synthetic 20-minute MP4 (not retrieval evidence)', async () => {
  const media = await probeMedia(fixture);
  assert.equal(media.durationSeconds, 1200);
  assert.equal(media.videoCodec, 'h264');
});
test('streamed upload returns 202, indexes in background, searches and seeks original bytes with mocked provider', async t => {
  const { app, provider } = await setup(); t.after(() => app.close());
  const response = await upload(app);
  assert.equal(response.statusCode, 202, response.body);
  assert.equal(response.json().phase, 'queued');
  const id = response.json().recordingId;
  const ready = await statusUntil(app, id, 'ready');
  assert.equal(ready.uploadProgress.completedChunks, 1);
  assert.equal(ready.upload, undefined);
  assert.match(ready.sha256, /^[0-9a-f]{64}$/);
  assert.equal(provider!.submissions, 1);
  const search = await app.inject({ method: 'POST', url: `/recordings/${id}/search`, payload: { question: 'guy wearing a Barcelona jersey' } });
  assert.equal(search.statusCode, 200, search.body);
  assert.equal(search.json().moments[0].rank, 1);
  assert.equal(search.json().moments[0].playbackUrl, `/recordings/${id}/source#t=100`);
  assert.equal(search.json().moments[0].score, undefined);
  const bytes = await readFile(fixture);
  const part = await app.inject({ url: `/recordings/${id}/source`, headers: { range: 'bytes=10-99' } });
  assert.equal(part.statusCode, 206);
  assert.equal(part.headers['content-range'], `bytes 10-99/${bytes.length}`);
  assert.deepEqual(part.rawPayload, bytes.subarray(10,100));
  const suffix = await app.inject({ url: `/recordings/${id}/source`, headers: { range: 'bytes=-10' } });
  assert.deepEqual(suffix.rawPayload, bytes.subarray(-10));
  const full = await app.inject(`/recordings/${id}/source`);
  assert.equal(full.statusCode,200); assert.deepEqual(full.rawPayload,bytes);
  for (const range of ['bytes=999999999-','bytes=0-1,3-5','bytes=-0','bytes=2-1','bytes=-']) {
    const invalid = await app.inject({ url: `/recordings/${id}/source`, headers: { range } });
    assert.equal(invalid.statusCode,416,range);
  }
});
test('requires creator authorization; rejects cross-site and rebinding requests', async t => {
  const { app } = await setup(); t.after(() => app.close());
  assert.equal((await upload(app, fixture, false)).statusCode,400);
  for (const headers of [{host:'evil.example'}, {origin:'https://evil.example'}, {'sec-fetch-site':'cross-site'}]) {
    assert.equal((await app.inject({url:'/health',headers})).statusCode,403);
  }
  assert.equal((await app.inject('/health')).json().localOnly,true);
});
test('missing credentials reports 503 without storing a recording', async t => {
  const dir = await mkdtemp(join(root,'no-key-'));
  const app = await buildApp({dataDir:dir}); t.after(() => app.close());
  assert.equal((await upload(app)).statusCode,503);
  assert.equal((await app.inject('/health')).json().providerConfigured,false);
  assert.deepEqual(await readdir(join(dir,'media')),[]);
});
test('oversize and incorrect field uploads clean partial files', async t => {
  const { app, dir } = await setup(new MockProvider(),64); t.after(() => app.close());
  assert.equal((await upload(app)).statusCode,413);
  assert.equal((await upload(app,fixture,true,'wrong')).statusCode,400);
  assert.deepEqual(await readdir(join(dir,'media')),[]);
  assert.deepEqual(await readdir(join(dir,'records')),[]);
});
test('invalid media fails before provider submission and before playback', async t => {
  const invalidPath=join(root,'invalid.mp4'); await import('node:fs/promises').then(fs=>fs.writeFile(invalidPath,'not video'));
  const { app, provider } = await setup(); t.after(() => app.close());
  const response = await upload(app,invalidPath);
  const id = response.json().recordingId;
  const failed = await statusUntil(app,id,'failed');
  assert.match(failed.error,/probe/);
  assert.equal(provider!.submissions,0);
  assert.equal((await app.inject(`/recordings/${id}/source`)).statusCode,409);
});
test('search validates input, rejects missing and processing recordings', async t => {
  const provider = new MockProvider(); provider.asset='processing';
  const { app } = await setup(provider); t.after(() => app.close());
  const id = (await upload(app)).json().recordingId;
  for (const payload of [{question:''},{question:'a',limit:2},{question:'a',unexpected:true}]) {
    assert.equal((await app.inject({method:'POST',url:`/recordings/${id}/search`,payload})).statusCode,400);
  }
  assert.equal((await app.inject({method:'POST',url:`/recordings/${id}/search`,payload:{question:'jersey'}})).statusCode,409);
  assert.equal((await app.inject('/recordings/unknown/status')).statusCode,404);
});
test('provider indexing failures become terminal failures', async t => {
  const provider = new MockProvider(); provider.state='failed';
  const { app } = await setup(provider); t.after(() => app.close());
  const id=(await upload(app)).json().recordingId;
  assert.match((await statusUntil(app,id,'failed')).error,/indexing failed/);
});
test('transient polling errors retry; persistent failures terminate', async t => {
  const provider = new MockProvider(); provider.pollFailures=2;
  const { app } = await setup(provider); t.after(() => app.close());
  const id=(await upload(app)).json().recordingId;
  assert.equal((await statusUntil(app,id,'ready')).pollErrors,0);
  provider.pollFailures=10;
  const second=(await upload(app)).json().recordingId;
  assert.match((await statusUntil(app,second,'failed')).error,/TwelveLabs operation failed/);
});
test('restart resumes known indexing job without duplicate submission', async () => {
  const provider = new MockProvider(); provider.state='indexing';
  const { app, dir } = await setup(provider);
  const id=(await upload(app)).json().recordingId;
  await statusUntil(app,id,'indexing');
  await app.close();
  provider.state='ready';
  const restarted=await buildApp({dataDir:dir,provider,intervalMs:5});
  try { await statusUntil(restarted,id,'ready'); assert.equal(provider.submissions,1); }
  finally { await restarted.close(); }
});
test('restart marks ambiguous submissions failed without calling provider', async () => {
  const dir=await mkdtemp(join(root,'interrupted-'));
  const store=new LocalStore(dir); await store.init();
  const id='11111111-1111-4111-8111-111111111111'; const now=new Date().toISOString();
  await store.put({id,filename:'source.mp4',bytes:1,sha256:'a'.repeat(64),creatorAuthorized:true,authorizedAt:now,createdAt:now,updatedAt:now,phase:'submitting_index'});
  const provider=new MockProvider();
  const app=await buildApp({dataDir:dir,provider,intervalMs:5});
  try { assert.match((await statusUntil(app,id,'failed')).error,/interrupted/); assert.equal(provider.submissions,0); }
  finally { await app.close(); }
});
test('duration validation rejects short source before indexing', async t => {
  const short=join(root,'short.mp4');
  await promisify(execFile)(ffmpeg!,['-v','error','-i',fixture,'-t','5','-c','copy',short],{windowsHide:true});
  await assert.rejects(probeMedia(short),/20–30 minute/);
});
