import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import ffmpeg from 'ffmpeg-static';
import { databaseClient, safeDatabaseError } from '../src/db-connection.js';
import { configuredSupabaseClient, createSupabasePersistence } from '../src/supabase-persistence.js';
import { chunkObjectPath, configuredChunkStorage } from '../src/chunk-storage.js';
import { buildApp } from '../src/app.js';
import type { VideoSearchProvider } from '../src/types.js';

let step='configuration';
async function run() {
  const db=await databaseClient();const owner=randomUUID();
  const root=await mkdtemp(join(tmpdir(),'rewindr-storage-verification-'));
  const env={...process.env,REWINDR_OWNER_ID:owner};const storage=configuredChunkStorage(env);const client=configuredSupabaseClient(env);
  let created=false;let app:Awaited<ReturnType<typeof buildApp>>|undefined;
  // Include each attempted object before upload; cleanup covers an uncertain DB acknowledgment too.
  const paths=new Set<string>();
  try {
    step='SQL connection';await db.connect();
    await db.query("insert into public.rewindr_broadcasters(id,label) values($1,'Rewindr temporary storage verification')",[owner]);created=true;
    step='synthetic fixture';const file=join(root,'synthetic.mp4');
    await promisify(execFile)(process.env.FFMPEG_PATH || ffmpeg as unknown as string,
      ['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=10','-t','4','-an','-c:v','libx264','-pix_fmt','yuv420p','-movflags','+faststart',file],{windowsHide:true,timeout:30000});
    const bytes=await readFile(file);
    const unexpected=async():Promise<never>=>{throw Error('Unexpected video provider operation');};
    const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,search:unexpected,
      async listIndexedChunks(){return [0,1].map(i=>({filename:`chunk_00${i}.mp4`,indexedAssetId:`verify-${i}`,durationSeconds:4,status:'ready'}));},
      async searchMany(){return [{videoId:'verify-0',startSeconds:3,endSeconds:4,rank:1},{videoId:'verify-1',startSeconds:0,endSeconds:1,rank:2}];}};
    step='application initialization';const persistence=createSupabasePersistence(root,env);app=await buildApp({dataDir:root,persistence,provider,storage});
    step='registration';const registered=await app.inject({method:'POST',url:'/broadcasts/import',headers:{'x-creator-authorized':'true'},payload:{title:'Temporary storage verification',indexId:'verify-index',sourceOffsetSeconds:7200,
      chunks:[{filename:'chunk_000.mp4',startSeconds:0,endSeconds:4},{filename:'chunk_001.mp4',startSeconds:4,endSeconds:8}]}});
    assert.equal(registered.statusCode,201,'Synthetic broadcast registration failed');const id=registered.json().broadcastId as string;
    for(const i of [0,1])paths.add(chunkObjectPath(owner,id,`verify-${i}`,createHash('sha256').update(bytes).digest('hex')));
    const upload=async(i:number)=>app!.inject({method:'POST',url:`/broadcasts/${id}/chunks/verify-${i}/media`,headers:{'x-creator-authorized':'true','content-type':'multipart/form-data; boundary=verify'},
      payload:Buffer.concat([Buffer.from(`--verify\r\nContent-Disposition: form-data; name="chunk"; filename="chunk_00${i}.mp4"\r\nContent-Type: video/mp4\r\n\r\n`),bytes,Buffer.from('\r\n--verify--\r\n')])});
    step='private object upload';for(const i of [0,1])assert.equal((await upload(i)).statusCode,201,'Synthetic chunk upload failed');
    step='idempotent retry';assert.equal((await upload(0)).statusCode,200,'Stored object was not reused');
    await app.close();app=undefined;
    step='restart reads';app=await buildApp({dataDir:root,persistence:createSupabasePersistence(root,env),provider,storage});
    assert.equal((await app.inject(`/broadcasts/${id}/status`)).json().storedChunkCount,2);
    step='boundary search and playback';const searched=await app.inject({method:'POST',url:`/broadcasts/${id}/search`,payload:{question:'Synthetic verification'}});
    assert.equal(searched.statusCode,200);assert.equal(searched.json().encounters[0].replayMode,'stored-chunks');
    const response=await app.inject(`/broadcasts/${id}/playback?startSeconds=3&endSeconds=6`);
    assert.equal(response.statusCode,200,'Signed playback failed');const manifest=response.json();
    assert.equal(manifest.segments.length,2);assert.equal(manifest.segments[1].startSeconds,0);assert.equal(manifest.segments[1].broadcastStartSeconds,7204);
    assert.equal((await app.inject(`/broadcasts/${id}/replay?startSeconds=3&endSeconds=6`)).statusCode,200);
    step='signed media byte range';const media=await fetch(manifest.segments[0].url,{headers:{range:'bytes=0-31'},signal:AbortSignal.timeout(20000)});
    assert.equal(media.status,206,'Signed MP4 did not support byte ranges');assert.deepEqual(Buffer.from(await media.arrayBuffer()),bytes.subarray(0,32));
    step='private bucket enforcement';const path=[...paths][0]!;
    const publicUrl=client.storage.from(storage.bucket).getPublicUrl(path).data.publicUrl;
    const denied=await fetch(publicUrl,{signal:AbortSignal.timeout(20000)});await denied.arrayBuffer();assert.equal(denied.ok,false,'Private video was accessible through a public URL');
    console.log('Verified real private Storage uploads, repeat-upload reuse, PostgreSQL links after restart, two-chunk playback, signed byte ranges and public-access denial. Video search was mocked; no TwelveLabs indexing was requested.');
  } finally {
    await app?.close();
    let cleanupError:unknown;
    try {
      if([...paths].some(path=>!path.startsWith(`${owner}/`)))throw Error('Unexpected verification object path');
      if(paths.size){const removed=await client.storage.from(storage.bucket).remove([...paths]);if(removed.error)throw Error('Temporary object cleanup failed');}
      if(created)await db.query("delete from public.rewindr_broadcasters where id=$1 and label='Rewindr temporary storage verification'",[owner]);
    } catch(error){cleanupError=error;console.error(`Temporary verification cleanup needs attention for owner ${owner}. Existing project objects were not targeted.`);}
    await db.end();
    if(!root.startsWith(join(tmpdir(),'rewindr-storage-verification-')))throw Error('Unexpected verification cleanup path');
    await rm(root,{recursive:true,force:true});
    if(cleanupError)throw cleanupError;
  }
  console.log('Temporary test objects and metadata removed. Your original video files were retained.');
}
run().catch(error=>{console.error(`Storage verification stopped during ${step}. ${safeDatabaseError(error)}`);process.exitCode=1;});
