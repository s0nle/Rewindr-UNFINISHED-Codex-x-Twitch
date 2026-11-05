import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { databaseClient, safeDatabaseError } from '../src/db-connection.js';
import { createSupabasePersistence } from '../src/supabase-persistence.js';
import { buildApp } from '../src/app.js';
import type { VideoSearchProvider } from '../src/types.js';
let step = 'configuration';

async function run() {
  const client = await databaseClient();
  const owner = randomUUID(); const recordingId = randomUUID();
  const root = await mkdtemp(join(tmpdir(), 'rewindr-db-verification-'));
  let created = false;
  let app: Awaited<ReturnType<typeof buildApp>> | undefined;
  try {
    step = 'SQL connection'; await client.connect();
    await client.query("insert into public.rewindr_broadcasters(id,label) values($1,'Rewindr temporary verification')", [owner]);
    created = true;
    const env = { ...process.env, REWINDR_OWNER_ID: owner };
    step = 'Data API initialization'; const persistence = createSupabasePersistence(root, env); await persistence.init();
    const now = new Date().toISOString();
    step = 'recording write'; await persistence.recordings.put({ id: recordingId, filename: 'verification.mp4', bytes: 1, sha256: 'a'.repeat(64),
      creatorAuthorized: true, authorizedAt: now, createdAt: now, updatedAt: now, phase: 'ready',
      media: { durationSeconds: 1200, width: 1280, height: 720, videoCodec: 'h264' } });
    // Only the metadata database is live. Never call TwelveLabs from verification.
    const unexpected = async (): Promise<never> => { throw new Error('Unexpected video provider operation.'); };
    const provider: VideoSearchProvider = { name: 'twelvelabs', ensureIndex: unexpected, createUpload: unexpected,
      transfer: unexpected, assetStatus: unexpected, indexAsset: unexpected, indexStatus: unexpected, search: unexpected,
      async listIndexedChunks() { return [
        { filename: 'chunk_000.mp4', indexedAssetId: 'verify-a', durationSeconds: 60.043317, status: 'ready' },
        { filename: 'chunk_001.mp4', indexedAssetId: 'verify-b', durationSeconds: 59.993266, status: 'ready' } ]; },
      async searchMany(_index, ids) { assert.deepEqual(ids, ['verify-a', 'verify-b']); return [{ videoId: 'verify-b', startSeconds: 11, endSeconds: 20, rank: 1 }]; } };
    step = 'application initialization'; app = await buildApp({ dataDir: root, persistence, provider });
    step = 'broadcast registration'; const registered = await app.inject({ method: 'POST', url: '/broadcasts/import', headers: { 'x-creator-authorized': 'true' }, payload: {
      title: 'Temporary database verification', indexId: 'verification-index', sourceRecordingId: recordingId, sourceOffsetSeconds: 3600,
      chunks: [{ filename: 'chunk_000.mp4', startSeconds: 0, endSeconds: 60.043317 },
        { filename: 'chunk_001.mp4', startSeconds: 60.043317, endSeconds: 120.036583 }] } });
    assert.equal(registered.statusCode, 201, 'Database-backed broadcast registration failed.');
    const id = registered.json().broadcastId as string;
    await app.close(); app = undefined;
    const restarted = createSupabasePersistence(root, env);
    step = 'restart and search'; app = await buildApp({ dataDir: root, persistence: restarted, provider });
    const response = await app.inject({ method: 'POST', url: `/broadcasts/${id}/search`, payload: { question: 'Verification query' } });
    assert.equal(response.statusCode, 200, 'Database-backed search after restart failed.');
    assert.equal(response.json().moments[0].recordingStartSeconds, 71.043317);
    assert.equal(response.json().moments[0].broadcastStartSeconds, 3671.043317);
    assert.equal((await app.inject('/health')).statusCode, 200);
    assert.equal((await app.inject(`/recordings/${recordingId}/status`)).statusCode, 200);
    const outsider = createSupabasePersistence(root, { ...process.env, REWINDR_OWNER_ID: randomUUID() });
    assert.equal(await outsider.recordings.get(recordingId), undefined);
    assert.equal(await outsider.broadcasts.get(id), undefined);
    console.log('Verified live Supabase writes, restart reads, API timestamp mapping, health and owner scoping with a mocked video provider.');
  } finally {
    await app?.close();
    if (created) await client.query("delete from public.rewindr_broadcasters where id=$1 and label='Rewindr temporary verification'", [owner]);
    await client.end();
    if (!root.startsWith(join(tmpdir(), 'rewindr-db-verification-'))) throw new Error('Unexpected verification cleanup path.');
    await rm(root, { recursive: true, force: true });
  }
  console.log('Temporary verification owner and metadata removed; existing recordings were not modified.');
}
run().catch(error => { console.error(`Verification stopped during ${step}. ${safeDatabaseError(error)}`); process.exitCode = 1; });
