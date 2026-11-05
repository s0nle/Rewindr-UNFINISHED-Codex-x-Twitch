import { before, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { PGlite } from '@electric-sql/pglite';
import { applyTestMigrations } from './queue-shim.js';

let db: PGlite;
const owner = '11111111-1111-4111-8111-111111111111';
const other = '22222222-2222-4222-8222-222222222222';
const id = '33333333-3333-4333-8333-333333333333';
const broadcast = { id, title: 'Test broadcast', indexId: 'index', createdAt: '2026-10-30T12:00:00.000Z', sourceOffsetSeconds: 7200,
  chunks: [{ filename: 'chunk_000.mp4', indexedAssetId: 'indexed-a', startSeconds: 0, endSeconds: 60.043317 },
    { filename: "chunk (001) — café's.mp4", indexedAssetId: 'indexed-b', startSeconds: 60.043317, endSeconds: 120.036583 }] };
before(async () => {
  db = new PGlite();
  await db.exec('create role anon; create role authenticated; create role service_role bypassrls;');
  await applyTestMigrations(db);
});
after(async () => { await db?.close(); });
async function begin(t: { after: (fn: () => Promise<void>) => void }) {
  await db.exec('begin'); t.after(async () => { await db.exec('rollback'); });
  await db.query('insert into rewindr_broadcasters(id,label) values($1,$3),($2,$3)', [owner, other, 'test owner']);
}
async function save(value: unknown = broadcast, who = owner) { await db.query('select rewindr_save_broadcast($1,$2::jsonb)', [who, JSON.stringify(value)]); }
async function get(who = owner, sourceId = id) {
  return (await db.query<{ result: typeof broadcast | null }>('select rewindr_get_broadcast($1,$2) result', [who, sourceId])).rows[0]!.result;
}
async function rejectsWithCode(action: () => Promise<unknown>, code: string) {
  await db.exec('savepoint invalid_write');
  let failure: unknown;
  try { await action(); } catch (error) { failure = error; }
  await db.exec('rollback to savepoint invalid_write');
  assert.ok(failure, 'Expected PostgreSQL to reject the operation.');
  assert.equal((failure as { code: string }).code, code);
}

test('real PostgreSQL schema round-trips timestamps and all chunks in one snapshot', async t => {
  await begin(t); await save();
  const result = (await get())!;
  assert.equal(result.title, broadcast.title);
  assert.equal(new Date(result.createdAt).toISOString(), broadcast.createdAt);
  assert.equal(result.sourceOffsetSeconds, 7200);
  assert.deepEqual(result.chunks, broadcast.chunks);
  assert.equal(await get(other), null);
});

test('invalid or duplicate chunks roll back both a new parent and replacements of existing data', async t => {
  await begin(t); await save();
  await rejectsWithCode(() => save({ ...broadcast, title: 'should roll back', chunks: [broadcast.chunks[0], { ...broadcast.chunks[1], endSeconds: 59 }] }), '23514');
  assert.equal((await get())!.title, broadcast.title);
  assert.deepEqual((await get())!.chunks, broadcast.chunks);
  await rejectsWithCode(() => save({ ...broadcast, chunks: [broadcast.chunks[0], { ...broadcast.chunks[1], indexedAssetId: 'indexed-a' }] }), '23505');
  await rejectsWithCode(() => save({ ...broadcast, chunks: [{ ...broadcast.chunks[0], endSeconds: 65 }, { ...broadcast.chunks[1], startSeconds: 60 }] }), '23514');
  const newId = '44444444-4444-4444-8444-444444444444';
  await rejectsWithCode(() => save({ ...broadcast, id: newId, chunks: [{ ...broadcast.chunks[0], endSeconds: -1 }] }), '23514');
  assert.equal(await get(owner, newId), null);
});

test('ownership and source foreign keys prevent cross-owner overwrite and invalid links', async t => {
  await begin(t); await save();
  await rejectsWithCode(() => save({ ...broadcast, title: 'other owner' }, other), '42501');
  await rejectsWithCode(() => save({ ...broadcast, sourceRecordingId: '55555555-5555-4555-8555-555555555555' }), '23503');
  assert.equal((await get())!.title, broadcast.title);
});

test('private tables and RPCs reject browser roles while scoped backend functions work', async t => {
  await begin(t); await save();
  for (const role of ['anon', 'authenticated']) {
    await db.exec(`set local role ${role}`);
    for (const table of ['rewindr_broadcasters', 'rewindr_recordings', 'rewindr_broadcasts', 'rewindr_chunks']) {
      await rejectsWithCode(() => db.query(`select * from ${table}`), '42501');
    }
    await rejectsWithCode(() => get(), '42501');
    await rejectsWithCode(() => save(), '42501');
    await rejectsWithCode(() => db.query('select rewindr_attach_chunk_storage($1,$2,$3,$4::jsonb)', [owner,id,'indexed-a','{}']), '42501');
    await db.exec('reset role');
  }
  await db.exec('set local role service_role');
  assert.equal((await get())!.id, id);
  assert.equal(await get(other), null);
  const rls = await db.query<{ relrowsecurity: boolean }>("select relrowsecurity from pg_class where relname in ('rewindr_broadcasters','rewindr_recordings','rewindr_broadcasts','rewindr_chunks')");
  assert.equal(rls.rows.length, 4); assert.ok(rls.rows.every(row => row.relrowsecurity));
  await db.exec('reset role');
});

test('a broadcast with more than 1000 chunks is not truncated by the read function', async t => {
  await begin(t);
  const chunks = Array.from({ length: 1001 }, (_, i) => ({ filename: `chunk_${i}.mp4`, indexedAssetId: `indexed-${i}`, startSeconds: i * 60, endSeconds: (i + 1) * 60 }));
  await save({ ...broadcast, chunks });
  const result = (await get())!;
  assert.equal(result.chunks.length, 1001);
  assert.equal(result.chunks.at(-1)!.startSeconds, 60000);
});

const mediaHash = 'a'.repeat(64);
const mediaRef = { bucket: 'rewindr-chunks', path: `${owner}/${id}/${createHash('sha256').update('indexed-a').digest('hex')}/${mediaHash}.mp4`,
  sha256: mediaHash, bytes: 1000, uploadedAt: '2026-10-30T12:01:00.000Z' };
async function attach(ref = mediaRef, who = owner) {
  await db.query('select rewindr_attach_chunk_storage($1,$2,$3,$4::jsonb)', [who, id, 'indexed-a', JSON.stringify(ref)]);
}
test('stored media survives metadata updates and cannot be remapped or replaced', async t => {
  await begin(t); await save(); await attach(); await attach({ ...mediaRef, uploadedAt: '2026-10-30T13:00:00.000Z' });
  const stored = (await db.query<{ storage: typeof mediaRef }>("select rewindr_get_broadcast($1,$2)->'chunks'->0->'storage' storage", [owner,id])).rows[0]!.storage;
  assert.equal(stored.path, mediaRef.path); assert.equal(stored.bytes, 1000);
  assert.equal(new Date(stored.uploadedAt).toISOString(), mediaRef.uploadedAt);
  await save({ ...broadcast, title: 'Updated title' });
  assert.equal((await db.query<{ path: string }>('select storage_path path from rewindr_chunks where broadcast_id=$1 and ordinal=0', [id])).rows[0]!.path, mediaRef.path);
  for (const changed of [ { ...broadcast, indexId: 'different' }, { ...broadcast, chunks: [broadcast.chunks[1]] },
    { ...broadcast, chunks: [{ ...broadcast.chunks[0], filename: 'different.mp4' }, broadcast.chunks[1]] } ]) {
    await rejectsWithCode(() => save(changed), '23514');
  }
  await rejectsWithCode(() => attach({ ...mediaRef, bytes: 1001 }), '23514');
  await rejectsWithCode(() => attach({ ...mediaRef, sha256: 'b'.repeat(64) }), '23514');
  await rejectsWithCode(() => attach(mediaRef, other), '23503');
});
test('database rejects forged storage paths and ignores storage injected through ordinary metadata saves', async t => {
  await begin(t);
  await save({ ...broadcast, chunks: [{ ...broadcast.chunks[0], storage: mediaRef }, broadcast.chunks[1]] });
  assert.deepEqual((await get())!.chunks, broadcast.chunks);
  await rejectsWithCode(() => attach({ ...mediaRef, path: mediaRef.path.replace(owner, other) }), '23514');
  await rejectsWithCode(() => attach({ ...mediaRef, bytes: 50000001 }), '23514');
  await attach();
});
