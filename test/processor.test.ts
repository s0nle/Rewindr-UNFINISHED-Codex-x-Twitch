import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as wait } from 'node:timers/promises';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Processor } from '../src/processor.js';
import { HttpError } from '../src/http-errors.js';
import { buildApp } from '../src/app.js';
import type { RecordingRepository, Persistence } from '../src/persistence.js';
import type { Recording, VideoSearchProvider } from '../src/types.js';

class MemoryStore implements RecordingRepository {
  readonly root = '.';
  row: Recording = { id: '55555555-5555-4555-8555-555555555555', filename: 'source.mp4', bytes: 1, sha256: 'a'.repeat(64),
    creatorAuthorized: true, authorizedAt: new Date().toISOString(), createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    phase: 'indexing', indexId: 'index', assetId: 'asset', indexedAssetId: 'indexed' };
  allCalls = 0; putCalls = 0;
  onAll?: () => void;
  onPut?: (record: Recording) => void;
  async init() {}
  mediaPath(): string { throw new Error('This fixture must not access media.'); }
  async get() { return structuredClone(this.row); }
  async all() { this.allCalls++; this.onAll?.(); return [structuredClone(this.row)]; }
  async put(record: Recording) { this.putCalls++; this.onPut?.(record); this.row = structuredClone(record); }
}
class Provider implements VideoSearchProvider {
  readonly name = 'twelvelabs' as const;
  indexSubmissions = 0; statusCalls = 0;
  async ensureIndex(): Promise<never> { throw new Error('Must not create another index.'); }
  async createUpload(): Promise<never> { throw new Error('Must not upload again.'); }
  async transfer(): Promise<void> { throw new Error('Must not transfer again.'); }
  async assetStatus() { return 'ready' as const; }
  async indexAsset() { this.indexSubmissions++; return 'new-indexed-id'; }
  async indexStatus() { this.statusCalls++; return 'ready' as const; }
  async search() { return []; }
}

test('recording uploads retry transient failures with the same session, survive restart and stop after a bounded budget',async()=>{
  const store=new MemoryStore(),provider=new Provider();store.mediaPath=()=> 'synthetic-source.mp4';store.row.phase='uploading';store.row.upload={uploadId:'existing',assetId:'asset',chunkSize:1,totalChunks:1,completedChunks:0,uploadHeaders:{}};
  let transfers=0;provider.transfer=async()=>{transfers++;if(transfers<3)throw Object.assign(Error('temporary'),{statusCode:500});};
  const options={maxFailures:3,initialDelayMs:1,maxDelayMs:2};
  for(let i=0;i<3;i++){const processor=new Processor(store,provider,1,options);await processor.processOne(store.row.id);await wait(3);}
  assert.equal(transfers,3);assert.equal(store.row.phase,'asset_processing');assert.equal(store.row.upload.uploadId,'existing');assert.equal(store.row.pollErrors,0);assert.equal(provider.indexSubmissions,0);
  store.row.phase='uploading';provider.transfer=async()=>{throw Object.assign(Error('temporary'),{statusCode:503});};
  for(let i=0;i<3;i++){await new Processor(store,provider,1,options).processOne(store.row.id);await wait(3);}assert.equal(store.row.phase,'failed');
});
test('permanent recording upload errors fail without automatically creating another provider resource',async()=>{
  const store=new MemoryStore(),provider=new Provider();store.mediaPath=()=> 'synthetic-source.mp4';store.row.phase='uploading';store.row.upload={uploadId:'existing',assetId:'asset',chunkSize:1,totalChunks:1,completedChunks:0,uploadHeaders:{}};
  provider.transfer=async()=>{throw Object.assign(Error('denied'),{statusCode:403});};await new Processor(store,provider).processOne(store.row.id);assert.equal(store.row.phase,'failed');assert.equal(provider.indexSubmissions,0);
});
const retry = { maxFailures: 3, initialDelayMs: 2, maxDelayMs: 4 };
async function until(predicate: () => boolean | Promise<boolean>) {
  for (let i = 0; i < 200; i++) { if (await predicate()) return; await wait(5); }
  assert.fail('Background operation did not reach the expected state.');
}

test('a transient database read failure retries with exponential backoff and resumes indexing', async t => {
  const store = new MemoryStore(); const provider = new Provider();
  const warning = t.mock.method(console, 'warn', () => {});
  store.onAll = () => { if (store.allCalls >= 2 && store.allCalls <= 4) throw new HttpError(503, 'Temporary database outage'); };
  const processor = new Processor(store, provider, 1, { ...retry, maxFailures: 4 }); t.after(() => processor.stop());
  await processor.start(); await until(() => store.row.phase === 'ready');
  assert.deepEqual(warning.mock.calls.map(call => (call.arguments[1] as { nextRetryMs: number }).nextRetryMs), [2, 4, 4]);
  assert.equal(provider.statusCalls, 1);
  assert.equal(processor.status().consecutiveMetadataFailures, 0);
  assert.notEqual(processor.status().state, 'halted');
});

test('failed metadata writes after provider submission keep its returned ID without submitting again', async t => {
  const store = new MemoryStore(); store.row.phase = 'asset_processing';
  const provider = new Provider(); let failures = 0;
  t.mock.method(console, 'warn', () => {});
  store.onPut = record => { if (record.phase === 'indexing' && failures++ < 2) throw new HttpError(503, 'Temporary checkpoint failure'); };
  const processor = new Processor(store, provider, 1, retry); t.after(() => processor.stop());
  await processor.start(); await until(() => store.row.phase === 'ready');
  assert.equal(provider.indexSubmissions, 1);
  assert.equal(store.row.indexedAssetId, 'new-indexed-id');
  assert.equal(store.row.providerError, undefined);
});

test('successful metadata operations reset the consecutive failure budget', async t => {
  const store = new MemoryStore(); const provider = new Provider(); let writeFailed = false;
  const warning = t.mock.method(console, 'warn', () => {});
  store.onAll = () => { if (store.allCalls === 2) throw new HttpError(503, 'Read blip'); };
  store.onPut = () => { if (!writeFailed) { writeFailed = true; throw new HttpError(503, 'Write blip'); } };
  const processor = new Processor(store, provider, 1, { ...retry, maxFailures: 2 }); t.after(() => processor.stop());
  await processor.start(); await until(() => store.row.phase === 'ready');
  assert.deepEqual(warning.mock.calls.map(call => (call.arguments[1] as { consecutiveFailures: number }).consecutiveFailures), [1, 1]);
  assert.notEqual(processor.status().state, 'halted');
});

test('persistent database errors stop after the configured attempt limit without further polling', async t => {
  const store = new MemoryStore(); const provider = new Provider();
  t.mock.method(console, 'warn', () => {}); t.mock.method(console, 'error', () => {});
  store.onAll = () => { if (store.allCalls > 1) throw new HttpError(503, 'Database unavailable'); };
  const processor = new Processor(store, provider, 1, retry); t.after(() => processor.stop());
  await processor.start(); await until(() => processor.status().state === 'halted');
  assert.equal(store.allCalls, 4); // one startup read plus three failed attempts
  assert.equal(processor.status().consecutiveMetadataFailures, 3);
  assert.equal(processor.status().haltReason, 'metadata_unavailable');
  await wait(15); assert.equal(store.allCalls, 4);
  assert.equal(provider.statusCalls, 0);
});

test('local storage errors halt immediately instead of being retried', async t => {
  const store = new MemoryStore(); const provider = new Provider();
  const warning = t.mock.method(console, 'warn', () => {}); t.mock.method(console, 'error', () => {});
  store.onAll = () => { if (store.allCalls > 1) throw Object.assign(new Error('Disk failure'), { code: 'ENOSPC' }); };
  const processor = new Processor(store, provider, 1, retry); t.after(() => processor.stop());
  await processor.start(); await until(() => processor.status().state === 'halted');
  assert.equal(store.allCalls, 2); assert.equal(warning.mock.calls.length, 0);
  assert.equal(processor.status().haltReason, 'storage_or_processing_error');
});

test('a local checkpoint write failure is not mislabeled or persisted as a provider failure', async t => {
  const store = new MemoryStore(); const provider = new Provider();
  t.mock.method(console, 'error', () => {});
  store.onPut = () => { throw Object.assign(new Error('Disk failure'), { code: 'ENOSPC' }); };
  const processor = new Processor(store, provider, 1, retry); t.after(() => processor.stop());
  await processor.start(); await until(() => processor.status().state === 'halted');
  assert.equal(store.putCalls, 1);
  assert.equal(store.row.phase, 'indexing');
  assert.equal(store.row.providerError, undefined);
});

test('stop cancels a long retry delay promptly without reporting a fatal halt', { timeout: 2000 }, async t => {
  const store = new MemoryStore(); const provider = new Provider();
  t.mock.method(console, 'warn', () => {});
  store.onAll = () => { if (store.allCalls > 1) throw new HttpError(503, 'Database unavailable'); };
  const processor = new Processor(store, provider, 1, { maxFailures: 3, initialDelayMs: 10000, maxDelayMs: 10000 }); t.after(() => processor.stop());
  await processor.start(); await until(() => processor.status().state === 'retrying');
  await processor.stop();
  assert.equal(processor.status().state, 'stopped');
  assert.equal(processor.status().haltReason, undefined);
  assert.equal(provider.statusCalls, 0);
});

test('startup database reads also recover from a transient failure', async t => {
  const store = new MemoryStore(); const provider = new Provider();
  t.mock.method(console, 'warn', () => {});
  store.onAll = () => { if (store.allCalls < 3) throw new HttpError(503, 'Startup database unavailable'); };
  const processor = new Processor(store, provider, 1, retry); t.after(() => processor.stop());
  await processor.start(); await until(() => store.row.phase === 'ready');
  assert.equal(processor.status().consecutiveMetadataFailures, 0);
});

test('health reports a halted processor even when the database health probe has recovered', async t => {
  const root = await mkdtemp(join(tmpdir(), 'rewindr-processor-health-'));
  const store = new MemoryStore(); const provider = new Provider();
  store.onAll = () => { if (store.allCalls > 1) throw new HttpError(503, 'Database unavailable'); };
  t.mock.method(console, 'warn', () => {}); t.mock.method(console, 'error', () => {});
  const persistence: Persistence = { kind: 'supabase', recordings: store,
    broadcasts: { async init() {}, async get() { return undefined; }, async all() { return []; }, async put() {} },
    async init() {}, async health() {} };
  const app = await buildApp({ dataDir: root, persistence, provider, intervalMs: 1, processorRetry: retry });
  try {
    await until(async () => (await app.inject('/health')).statusCode === 503);
    const health = await app.inject('/health');
    assert.equal(health.json().status, 'degraded');
    assert.equal(health.json().processing.state, 'halted');
    assert.equal(health.json().processing.haltReason, 'metadata_unavailable');
  } finally {
    await app.close();
    if (!root.startsWith(join(tmpdir(), 'rewindr-processor-health-'))) throw new Error('Unexpected cleanup path.');
    await rm(root, { recursive: true, force: true });
  }
});
