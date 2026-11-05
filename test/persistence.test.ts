import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Fastify from 'fastify';
import { createClient } from '@supabase/supabase-js';
import { SupabasePersistence, LOCAL_OWNER_ID, createSupabasePersistence } from '../src/supabase-persistence.js';
import { createLocalPersistence } from '../src/local-persistence.js';
import { importLocalMetadata } from '../src/import-local.js';
import { buildApp } from '../src/app.js';
import { HttpError } from '../src/http-errors.js';
import { ImportValidationError, describeImportError } from '../src/import-errors.js';
import type { Recording, Broadcast } from '../src/types.js';

const id = '55555555-5555-4555-8555-555555555555';
const record: Recording = { id, filename: 'source.mp4', bytes: 3, sha256: 'a'.repeat(64), creatorAuthorized: true,
  authorizedAt: '2026-10-30T12:00:00.000Z', createdAt: '2026-10-30T12:00:00.000Z', updatedAt: '2026-10-30T12:00:00.000Z', phase: 'ready',
  media: { durationSeconds: 1200, width: 1280, height: 720, videoCodec: 'h264' }, indexId: 'index', indexedAssetId: 'indexed' };
const broadcast: Broadcast = { id: '66666666-6666-4666-8666-666666666666', title: 'Test', indexId: 'index',
  createdAt: record.createdAt, sourceOffsetSeconds: 0, sourceRecordingId: id,
  chunks: [{ filename: 'chunk_000.mp4', indexedAssetId: 'indexed', startSeconds: 0, endSeconds: 60 }] };
async function directory(t: { after: (fn: () => Promise<void>) => void }) {
  const dir = await mkdtemp(join(tmpdir(), 'rewindr-persistence-'));
  t.after(async () => {
    if (!dir.startsWith(join(tmpdir(), 'rewindr-persistence-'))) throw new Error('Unexpected cleanup directory.');
    await rm(dir, { recursive: true, force: true });
  });
  return dir;
}

test('import is dry-run by default, preserves IDs and mappings, skips advanced remote records and is repeatable', async t => {
  const local = createLocalPersistence(await directory(t)); const remote = createLocalPersistence(await directory(t));
  await local.init(); await remote.init();
  await local.recordings.put(record); await local.broadcasts.put(broadcast);
  const dry = await importLocalMetadata(local, remote, false);
  assert.deepEqual(dry, { mode: 'dry-run', recordings: 1, broadcasts: 1, alreadyPresent: 0, skippedStale: 0, staleRecordings: [] });
  assert.equal(await remote.recordings.get(id), undefined);
  await importLocalMetadata(local, remote, true);
  assert.equal((await remote.recordings.get(id))!.sha256, record.sha256);
  assert.deepEqual(await remote.broadcasts.get(broadcast.id), broadcast);
  await remote.recordings.put({ ...record, phase: 'failed', error: 'newer remote checkpoint' });
  assert.deepEqual(await importLocalMetadata(local, remote, true), { mode: 'applied', recordings: 0, broadcasts: 0, alreadyPresent: 2, skippedStale: 0, staleRecordings: [] });
  assert.equal((await remote.recordings.get(id))!.error, 'newer remote checkpoint');
});

test('import preflights conflicts before writing any pending metadata', async t => {
  const local = createLocalPersistence(await directory(t)); const remote = createLocalPersistence(await directory(t));
  await local.init(); await remote.init();
  await local.recordings.put(record);
  const second = { ...record, id: '77777777-7777-4777-8777-777777777777' };
  await local.recordings.put(second);
  await remote.recordings.put({ ...second, sha256: 'b'.repeat(64) });
  await assert.rejects(importLocalMetadata(local, remote, true), /identity conflict/);
  assert.equal(await remote.recordings.get(record.id), undefined);
});

test('import reports older remote metadata separately without overwriting its phase or checkpoints', async t => {
  const local = createLocalPersistence(await directory(t)); const remote = createLocalPersistence(await directory(t));
  await local.init(); await remote.init();
  const old = { ...record, phase: 'queued' as const, updatedAt: '2026-10-29T12:00:00.000Z' };
  local.recordings.all = async () => [record];
  remote.recordings.get = async () => old;
  let writes = 0; remote.recordings.put = async () => { writes++; };
  for (const apply of [false, true]) {
    const result = await importLocalMetadata(local, remote, apply);
    assert.equal(result.recordings, 0); assert.equal(result.alreadyPresent, 0); assert.equal(result.skippedStale, 1);
    assert.deepEqual(result.staleRecordings, [{ recordingId: id, localPhase: 'ready', remotePhase: 'queued',
      localUpdatedAt: record.updatedAt, remoteUpdatedAt: old.updatedAt }]);
  }
  assert.equal(writes, 0); assert.equal(old.phase, 'queued');
});

test('safe conflict diagnostics include the affected record and unexpected errors remain redacted', async t => {
  const local = createLocalPersistence(await directory(t)); const remote = createLocalPersistence(await directory(t));
  await local.init(); await remote.init();
  await local.recordings.put(record); await remote.recordings.put({ ...record, sha256: 'b'.repeat(64) });
  let conflict: unknown;
  try { await importLocalMetadata(local, remote, true); } catch (error) { conflict = error; }
  assert.ok(conflict instanceof ImportValidationError);
  assert.match(describeImportError(conflict), /Recording identity conflict/);
  assert.ok(describeImportError(conflict).includes(id));
  assert.doesNotMatch(describeImportError(new Error('postgres://user:PRIVATE_PASSWORD@host')), /PRIVATE_PASSWORD/);
  remote.recordings.get = async () => record;
  await local.broadcasts.put(broadcast);
  remote.broadcasts.get = async () => ({ ...broadcast, title: 'different mapping' });
  await assert.rejects(importLocalMetadata(local, remote, true), error => error instanceof ImportValidationError && describeImportError(error).includes('Broadcast mapping conflict') && error.message.includes(broadcast.id));
  local.recordings.all = async () => []; remote.recordings.get = async () => undefined; remote.broadcasts.get = async () => undefined;
  await assert.rejects(importLocalMetadata(local, remote, true), error => error instanceof ImportValidationError && describeImportError(error).includes('Broadcast source is missing') && error.message.includes(id));
});

test('Supabase SDK scopes reads and upserts to the configured owner and sends atomic broadcast RPCs', async t => {
  const root = await directory(t); const api = Fastify();
  const row = { id, owner_id: LOCAL_OWNER_ID, filename: record.filename, bytes: 3, sha256: record.sha256,
    creator_authorized: true, authorized_at: record.authorizedAt, created_at: record.createdAt, updated_at: record.updatedAt,
    phase: record.phase, media: record.media, index_id: 'index', indexed_asset_id: 'indexed' };
  let puts = 0; let fail = false;
  api.get<{ Querystring: Record<string, string> }>('/rest/v1/rewindr_recordings', async (req, reply) => {
    assert.equal(req.query.owner_id, `eq.${LOCAL_OWNER_ID}`);
    if (fail) return reply.code(503).send({ message: 'PRIVATE DATABASE ERROR', code: 'private' });
    if (req.query.id) { assert.equal(req.query.id, `eq.${id}`); return reply.send(row); }
    return reply.send([row]);
  });
  api.post<{ Querystring: Record<string, string> }>('/rest/v1/rewindr_recordings', async req => {
    assert.equal(req.query.on_conflict, 'owner_id,id');
    assert.equal((req.body as Record<string, unknown>).owner_id, LOCAL_OWNER_ID); puts++;
    return null;
  });
  api.post('/rest/v1/rpc/rewindr_save_broadcast', async req => {
    const body = req.body as { p_owner_id: string; p_broadcast: Broadcast };
    assert.equal(body.p_owner_id, LOCAL_OWNER_ID); assert.deepEqual(body.p_broadcast, broadcast); puts++;
    return null;
  });
  api.post('/rest/v1/rpc/rewindr_get_broadcast', async req => {
    assert.deepEqual(req.body, { p_owner_id: LOCAL_OWNER_ID, p_id: broadcast.id }); return broadcast;
  });
  const address = await api.listen({ host: '127.0.0.1', port: 0 });
  try {
    const remote = new SupabasePersistence(root, createClient(address, 'test-key', { auth: { persistSession: false } }), LOCAL_OWNER_ID);
    assert.equal((await remote.recordings.get(id))!.id, id);
    assert.equal((await remote.recordings.all()).length, 1);
    await remote.recordings.put(record); await remote.broadcasts.put(broadcast);
    assert.deepEqual(await remote.broadcasts.get(broadcast.id), broadcast);
    assert.equal(puts, 2);
    assert.equal(await remote.recordings.get('invalid'), undefined);
    fail = true;
    await assert.rejects(remote.recordings.get(id), error => error instanceof HttpError && error.statusCode === 503 && !error.message.includes('PRIVATE'));
  } finally { await api.close(); }
});

test('API health fails closed when database health fails instead of claiming local persistence', async t => {
  const persistence = createLocalPersistence(await directory(t));
  const app = await buildApp({ dataDir: persistence.recordings.root, persistence });
  try {
    persistence.health = async () => { throw new HttpError(503, 'Database unavailable'); };
    const health = await app.inject('/health');
    assert.equal(health.statusCode, 503);
    assert.equal(health.json().status, undefined);
  } finally { await app.close(); }
});

test('uncertain database metadata writes retain uploaded media instead of deleting a potentially committed recording', async t => {
  const persistence = createLocalPersistence(await directory(t));
  const provider = { name: 'twelvelabs' } as import('../src/types.js').VideoSearchProvider;
  const app = await buildApp({ dataDir: persistence.recordings.root, persistence, provider });
  try {
    persistence.recordings.put = async () => { throw new HttpError(503, 'Metadata timeout'); };
    const boundary = 'metadata-timeout';
    const response = await app.inject({ method: 'POST', url: '/recordings', headers: {
      'x-creator-authorized': 'true', 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload: Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="recording"; filename="source.mp4"\r\nContent-Type: video/mp4\r\n\r\nabc\r\n--${boundary}--\r\n`) });
    assert.equal(response.statusCode, 503, response.body);
    assert.equal((await readdir(join(persistence.recordings.root, 'media'))).filter(name => name.endsWith('.mp4')).length, 1);
  } finally { await app.close(); }
});

test('Supabase setup rejects missing credentials and publishable keys', () => {
  assert.throws(() => createSupabasePersistence('.', {}), /requires/);
  assert.throws(() => createSupabasePersistence('.', { SUPABASE_URL: 'https://example.supabase.co', SUPABASE_SECRET_KEY: 'sb_publishable_test' }), /backend secret/);
  assert.throws(() => createSupabasePersistence('.', { SUPABASE_URL: 'https://example.supabase.co/rest/v1', SUPABASE_SECRET_KEY: 'sb_secret_test' }), /project root/);
});
