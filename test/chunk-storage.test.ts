import { before, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Script } from 'node:vm';
import ffmpeg from 'ffmpeg-static';
import { createClient } from '@supabase/supabase-js';
import { createHash } from 'node:crypto';
import { SupabaseChunkStorage, chunkObjectPath, type ChunkStorage } from '../src/chunk-storage.js';
import { candidatePlayback, playbackPlan, signedPlayback } from '../src/playback.js';
import { replayPage } from '../src/replay-page.js';
import { buildApp } from '../src/app.js';
import { createLocalPersistence } from '../src/local-persistence.js';
import { HttpError } from '../src/http-errors.js';
import type { Broadcast, ChunkStorageRef, VideoSearchProvider } from '../src/types.js';

const owner = '11111111-1111-4111-8111-111111111111', id = '33333333-3333-4333-8333-333333333333';
const hash = 'a'.repeat(64);
const reference = (asset: string): ChunkStorageRef => ({ bucket: 'rewindr-chunks', path: chunkObjectPath(owner,id,asset,hash), sha256: hash, bytes: 1000, uploadedAt: new Date().toISOString() });
const broadcast: Broadcast = { id, title: 'Test', indexId: 'test', createdAt: new Date().toISOString(), sourceOffsetSeconds: 7200,
  chunks: [0,1,2].map(i => ({ filename: `chunk_00${i}.mp4`, indexedAssetId: `asset-${i}`, startSeconds: i*4, endSeconds: (i+1)*4, storage: reference(`asset-${i}`) })) };
let fixtureRoot: string, fixture: Buffer;
before(async () => {
  fixtureRoot = await mkdtemp(join(tmpdir(), 'rewindr-storage-fixture-'));
  const file = join(fixtureRoot,'fixture.mp4');
  await promisify(execFile)(ffmpeg as unknown as string, ['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=10','-t','4','-an','-c:v','libx264','-pix_fmt','yuv420p','-movflags','+faststart',file], { windowsHide: true, timeout: 30000 });
  fixture = await readFile(file);
});
after(async () => {
  if (!fixtureRoot.startsWith(join(tmpdir(),'rewindr-storage-fixture-'))) throw Error('Unexpected fixture cleanup path');
  await rm(fixtureRoot,{ recursive:true,force:true });
});
test('private playback maps a boundary encounter into chunk-local seeks and keeps original broadcast time', async () => {
  const plan = playbackPlan(broadcast,3,6)!;
  assert.deepEqual(plan.segments.map(s=>[s.startSeconds,s.endSeconds]), [[3,4],[0,2]]);
  const storage = { sign: async () => ({ url:'https://example.invalid/private-token',expiresAt:'2026-10-30T13:00:00.000Z' }) } as unknown as ChunkStorage;
  const result = await signedPlayback(broadcast,3,6,storage);
  assert.equal(result.segments[1]!.broadcastStartSeconds,7204);
  assert.equal(result.segments[1]!.recordingStartSeconds,4);
  assert.equal(result.segments[1]!.startSeconds,0);
  assert.equal(playbackPlan(broadcast,0,301),undefined);
  const missing = structuredClone(broadcast); delete missing.chunks[1]!.storage;
  assert.equal(playbackPlan(missing,3,6),undefined);
  await assert.rejects(signedPlayback(missing,3,6,storage), error=>error instanceof HttpError && error.statusCode===409);
  missing.chunks[1]!.storage = reference('asset-1'); missing.chunks[1]!.startSeconds=4.5;
  assert.equal(playbackPlan(missing,3,6),undefined);
});
test('candidate replay trims unavailable context but never conceals missing matched footage', () => {
  const partial=structuredClone(broadcast); delete partial.chunks[0]!.storage;
  const candidate={ startSeconds:0,endSeconds:12,matchStartSeconds:5,matchEndSeconds:6 };
  const result=candidatePlayback(partial,candidate,true);
  assert.equal(result.playbackStartSeconds,4); assert.equal(result.contextTrimmed,true);
  assert.equal(result.playbackFormat,'html'); assert.ok(result.manifestUrl);
  assert.deepEqual(candidatePlayback(partial,{...candidate,matchStartSeconds:3},true),{});
  assert.deepEqual(candidatePlayback(partial,candidate,false),{});
  new Script(replayPage.match(/<script>([\s\S]*?)<\/script>/)![1]!);
});
test('Storage SDK uploads without overwrite, retries transient failures, reuses immutable objects and scopes signed links', async () => {
  let uploaded=false, attempts=0, publicBucket=false; const path=chunkObjectPath(owner,id,'asset-0',hash);
  const client=createClient('https://storage-test.supabase.co','test-key',{ auth:{persistSession:false}, global:{fetch:async(input,init)=>{
    const url=new URL(String(input));
    if(url.pathname.includes('/bucket/')) return Response.json({id:'rewindr-chunks',name:'rewindr-chunks',public:publicBucket});
    if(url.pathname.includes('/object/info/')) return uploaded?Response.json({size:fixture.length}):Response.json({message:'not found',statusCode:'404'},{status:404});
    if(url.pathname.includes('/object/sign/')) { assert.equal(JSON.parse(String(init?.body)).expiresIn,300); return Response.json({signedURL:'/object/sign/private?token=example'}); }
    assert.equal(init?.method,'POST'); assert.equal(new Headers(init?.headers).get('x-upsert'),'false');
    attempts++; if(attempts===1)return Response.json({message:'temporary',statusCode:'503'},{status:503});
    uploaded=true; return Response.json({Key:`rewindr-chunks/${path}`,Id:'test'});
  }}});
  const storage=new SupabaseChunkStorage(client,owner,'rewindr-chunks',300,1);
  const file=join(fixtureRoot,'fixture.mp4');
  const first=await storage.upload(id,'asset-0',file,hash,fixture.length);
  assert.equal(first.reused,false); assert.equal(attempts,2);
  assert.equal((await storage.upload(id,'asset-0',file,hash,fixture.length)).reused,true); assert.equal(attempts,2);
  assert.ok((await storage.sign(id,'asset-0',first.reference)).url.includes('token='));
  await assert.rejects(storage.sign(id,'other-asset',first.reference),error=>error instanceof HttpError && error.statusCode===409);
  publicBucket=true; await assert.rejects(storage.init(),/must be private/);
});

function multipart(bytes: Buffer, filename='chunk_000.mp4',field='chunk') {
  return { headers:{'content-type':'multipart/form-data; boundary=storage-test','x-creator-authorized':'true'},
    payload:Buffer.concat([Buffer.from(`--storage-test\r\nContent-Disposition: form-data; name="${field}"; filename="${filename}"\r\nContent-Type: video/mp4\r\n\r\n`),bytes,Buffer.from('\r\n--storage-test--\r\n')]) };
}
async function setup(t:{after:(fn:()=>Promise<void>)=>void},maxBytes?:number) {
  const root=await mkdtemp(join(tmpdir(),'rewindr-media-test-')); const persistence=createLocalPersistence(root); await persistence.init();
  const value=structuredClone(broadcast); value.chunks.forEach(c=>delete c.storage); await persistence.broadcasts.put(value);
  let uploads=0;
  const storage:ChunkStorage={bucket:'rewindr-chunks',async init(){},async upload(b,a,_file,sha,bytes){uploads++;return {reference:{...reference(a),path:chunkObjectPath(owner,b,a,sha),sha256:sha,bytes},reused:uploads>1};},
    async sign(){return {url:'https://example.invalid/private',expiresAt:'2026-10-30T13:00:00.000Z'};} };
  const provider={name:'twelvelabs',async searchMany(){return [{videoId:'asset-0',startSeconds:3,endSeconds:4,rank:1},{videoId:'asset-1',startSeconds:0,endSeconds:1,rank:2}];}} as unknown as VideoSearchProvider;
  const app=await buildApp({dataDir:root,persistence,storage,provider,maxChunkUploadBytes:maxBytes});
  t.after(async()=>{await app.close();if(!root.startsWith(join(tmpdir(),'rewindr-media-test-')))throw Error('Unexpected cleanup path');await rm(root,{recursive:true,force:true});});
  return {root,persistence,app,get uploads(){return uploads;}};
}
test('multipart upload validates media, persists its hash, supports retry and exposes private replay',async t=>{
  const {app,persistence,root}=await setup(t);const url=`/broadcasts/${id}/chunks/asset-0/media`;
  const first=await app.inject({method:'POST',url,...multipart(fixture)});assert.equal(first.statusCode,201,first.body);
  assert.equal(first.json().sha256,createHash('sha256').update(fixture).digest('hex'));
  assert.equal((await persistence.broadcasts.get(id))!.chunks[0]!.storage!.bytes,fixture.length);
  assert.equal((await app.inject({method:'POST',url,...multipart(fixture)})).statusCode,200);
  assert.equal((await app.inject({method:'POST',url,...multipart(Buffer.from('different'))})).statusCode,409);
  assert.equal((await app.inject(`/broadcasts/${id}/playback?startSeconds=1&endSeconds=3`)).statusCode,200);
  const page=await app.inject(`/broadcasts/${id}/replay?startSeconds=1&endSeconds=3`);assert.equal(page.statusCode,200);assert.match(page.headers['content-type']!,/text\/html/);
  assert.equal(page.headers['referrer-policy'],'no-referrer');assert.equal(page.headers['cache-control'],'no-store');
  assert.equal((await app.inject(`/broadcasts/${id}/playback?startSeconds=3&endSeconds=6`)).statusCode,409);
  assert.equal((await app.inject({method:'POST',url:`/broadcasts/${id}/chunks/asset-1/media`,...multipart(fixture,'chunk_001.mp4')})).statusCode,200);
  const search=await app.inject({method:'POST',url:`/broadcasts/${id}/search`,payload:{question:'A boundary encounter'}});
  assert.equal(search.statusCode,200,search.body);assert.equal(search.json().encounters[0].playbackFormat,'html');assert.match(search.json().encounters[0].manifestUrl,/\/playback\?/);
  assert.equal((await app.inject(`/broadcasts/${id}/playback?startSeconds=-1&endSeconds=3`)).statusCode,400);
  assert.deepEqual(await readdir(join(root,'chunk-uploads')),[]);
});
test('upload rejects wrong filenames, unvalidated media, unauthorized requests and oversized bodies',async t=>{
  const state=await setup(t);const url=`/broadcasts/${id}/chunks/asset-0/media`;
  assert.equal((await state.app.inject({method:'POST',url,...multipart(fixture,'wrong.mp4')})).statusCode,400);
  assert.equal((await state.app.inject({method:'POST',url,...multipart(Buffer.from('not mp4'))})).statusCode,415);
  assert.equal((await state.app.inject({method:'POST',url,...multipart(fixture),headers:{'content-type':'multipart/form-data; boundary=storage-test'}})).statusCode,400);
  assert.equal(state.uploads,0);
  const small=await setup(t,10);
  assert.equal((await small.app.inject({method:'POST',url,...multipart(fixture)})).statusCode,413);
  assert.equal(small.uploads,0);
});
test('uncertain metadata acknowledgment leaves the uploaded object recoverable by retry',async t=>{
  const {app,persistence,root}=await setup(t);const attach=persistence.broadcasts.attachStorage!.bind(persistence.broadcasts);let failed=true;
  persistence.broadcasts.attachStorage=async(...args)=>{if(failed){failed=false;throw new HttpError(503,'timeout');}await attach(...args);};
  const url=`/broadcasts/${id}/chunks/asset-0/media`;
  const response=await app.inject({method:'POST',url,...multipart(fixture)});assert.equal(response.statusCode,503);assert.match(response.json().error,/Retry the same chunk/);
  assert.deepEqual(await readdir(join(root,'chunk-uploads')),[]);
  assert.equal((await app.inject({method:'POST',url,...multipart(fixture)})).statusCode,200);
  assert.ok((await persistence.broadcasts.get(id))!.chunks[0]!.storage);
});
