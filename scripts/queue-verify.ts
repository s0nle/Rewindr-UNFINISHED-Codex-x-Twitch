import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,readFile,readdir,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import ffmpeg from 'ffmpeg-static';
import {databaseClient,safeDatabaseError} from '../src/db-connection.js';
import {configuredSupabaseClient,createSupabasePersistence} from '../src/supabase-persistence.js';
import {configuredChunkStorage,chunkObjectPath} from '../src/chunk-storage.js';
import {SupabaseJobQueue,LeaseLostError} from '../src/jobs.js';
import {buildApp} from '../src/app.js';
import type {VideoSearchProvider} from '../src/types.js';

let step='configuration';
async function run() {
  const db=await databaseClient();const owner=randomUUID();const queueName=`rewindr_verify_${randomUUID().replaceAll('-','')}`;
  const root=await mkdtemp(join(tmpdir(),'rewindr-queue-verification-'));const env={...process.env,REWINDR_OWNER_ID:owner,DATA_DIR:root};
  const client=configuredSupabaseClient(env);const storage=configuredChunkStorage(env);const queue=new SupabaseJobQueue(client,owner,queueName);
  let ownerCreated=false,queueCreated=false;let app:Awaited<ReturnType<typeof buildApp>>|undefined;
  const objects=new Set<string>();
  try {
    step='SQL connection and isolated queue';await db.connect();await db.query('select pgmq.create($1)',[queueName]);queueCreated=true;
    await db.query("insert into rewindr_broadcasters(id,label) values($1,'Rewindr temporary queue verification')",[owner]);ownerCreated=true;
    step='recording trigger rollback';const recordingId=randomUUID();
    await db.query('begin');
    try {
      await db.query("insert into rewindr_recordings(id,owner_id,filename,bytes,sha256,creator_authorized,authorized_at,created_at,updated_at,phase) values($1,$2,'verify.mp4',1,$3,true,now(),now(),now(),'queued')",[recordingId,owner,'a'.repeat(64)]);
      assert.equal((await db.query('select id from rewindr_jobs where id=$1 and owner_id=$2',[recordingId,owner])).rows.length,1);
      assert.equal((await db.query("select msg_id from pgmq.q_rewindr_jobs where message->>'jobId'=$1",[recordingId])).rows.length,1);
    } finally {await db.query('rollback');}
    assert.equal((await db.query("select msg_id from pgmq.q_rewindr_jobs where message->>'jobId'=$1",[recordingId])).rows.length,0);
    step='synthetic fixture';const file=join(root,'fixture.mp4');
    await promisify(execFile)(process.env.FFMPEG_PATH||ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=10','-t','4','-an','-c:v','libx264','-pix_fmt','yuv420p','-movflags','+faststart',file],{windowsHide:true,timeout:30000});
    const bytes=await readFile(file);const sha=createHash('sha256').update(bytes).digest('hex');
    const unexpected=async():Promise<never>=>{throw Error('Unexpected TwelveLabs operation');};
    const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,search:unexpected,
      async listIndexedChunks(){return [{filename:'chunk_000.mp4',indexedAssetId:'verify-asset',durationSeconds:4,status:'ready'}];}};
    const makeApp=()=>buildApp({dataDir:root,persistence:createSupabasePersistence(root,env),storage,jobs:queue,provider});
    app=await makeApp();
    const imported=await app.inject({method:'POST',url:'/broadcasts/import',headers:{'x-creator-authorized':'true'},payload:{title:'Queue verification',indexId:'verify-index',sourceOffsetSeconds:7200,chunks:[{filename:'chunk_000.mp4',startSeconds:0,endSeconds:4}]}});
    assert.equal(imported.statusCode,201,'Synthetic registration failed');const broadcastId=imported.json().broadcastId as string;
    objects.add(chunkObjectPath(owner,broadcastId,'verify-asset',sha));
    const upload=()=>app!.inject({method:'POST',url:`/broadcasts/${broadcastId}/chunks/verify-asset/media`,headers:{'x-creator-authorized':'true','content-type':'multipart/form-data; boundary=verify'},
      payload:Buffer.concat([Buffer.from('--verify\r\nContent-Disposition: form-data; name="chunk"; filename="chunk_000.mp4"\r\nContent-Type: video/mp4\r\n\r\n'),bytes,Buffer.from('\r\n--verify--\r\n')])});
    step='queued HTTP intake and deduplication';const response=await upload();assert.equal(response.statusCode,202,'Upload was not queued');const jobId=response.json().jobId as string;
    const repeated=await upload();assert.equal(repeated.json().jobId,jobId);assert.equal((await readdir(join(root,'chunk-jobs'))).length,1);
    assert.equal((await app.inject('/health')).statusCode,503,'Missing worker/backlog was not visible');
    await app.close();app=undefined;
    step='simulated expired worker lease';const abandoned=(await queue.claim(randomUUID(),60))!;assert.equal(abandoned.id,jobId);
    await db.query("update rewindr_jobs set lease_until=clock_timestamp()-interval '1 second' where id=$1 and owner_id=$2",[jobId,owner]);
    await db.query('select pgmq.set_vt($1,message_id,0) from rewindr_jobs where id=$2 and owner_id=$3',[queueName,jobId,owner]);
    await assert.rejects(queue.renew(abandoned,60),error=>error instanceof LeaseLostError);
    step='separate competing worker processes';
    const launch=()=>promisify(execFile)(process.execPath,[resolve('dist/src/worker.js'),'--once','--verification-queue',queueName],
      {cwd:process.cwd(),env:{...env,TWELVELABS_API_KEY:''},windowsHide:true,timeout:120000,maxBuffer:1024*1024});
    // Both children read the same queue; only one may claim this visible job.
    const children=await Promise.allSettled([launch(),launch()]);
    assert.ok(children.every(result=>result.status==='fulfilled'),'A verification worker process failed');
    const completed=(await queue.get(jobId))!;assert.equal(completed.state,'completed','Worker did not complete the queued upload');assert.equal(completed.attempts,2,'Both workers claimed the same unexpired job');
    await assert.rejects(queue.finish(abandoned,'failed'),error=>error instanceof LeaseLostError);
    assert.deepEqual(await readdir(join(root,'chunk-jobs')),[],'Staged copy was not removed after commit');
    step='API restart, owner scope and signed playback';app=await makeApp();
    const visible=await app.inject(`/jobs/${jobId}`);assert.equal(visible.statusCode,200);assert.equal(visible.json().input,undefined);assert.equal(visible.json().lease_token,undefined);
    assert.equal(await new SupabaseJobQueue(client,randomUUID(),queueName).get(jobId),undefined);
    assert.equal((await app.inject(`/broadcasts/${broadcastId}/status`)).json().storedChunkCount,1);
    const playback=await app.inject(`/broadcasts/${broadcastId}/playback?startSeconds=1&endSeconds=3`);assert.equal(playback.statusCode,200);
    const segment=playback.json().segments[0];const media=await fetch(segment.url,{headers:{range:'bytes=0-31'},signal:AbortSignal.timeout(20000)});
    assert.equal(media.status,206);assert.deepEqual(Buffer.from(await media.arrayBuffer()),bytes.subarray(0,32));
    console.log('Verified real pgmq enqueue/deduplication, transactional recording enqueue, API restart, expired-lease redelivery, competing worker processes, fenced stale acknowledgment, private upload/playback and staged-file cleanup. No TwelveLabs requests were made.');
  } finally {
    await app?.close();
    let cleanupError:unknown;
    try {
      if([...objects].some(path=>!path.startsWith(`${owner}/`)))throw Error('Unexpected object path');
      if(objects.size){const removed=await client.storage.from(storage.bucket).remove([...objects]);if(removed.error)throw Error('Object cleanup failed');}
      if(ownerCreated)await db.query("delete from rewindr_broadcasters where id=$1 and label='Rewindr temporary queue verification'",[owner]);
      if(queueCreated){await db.query('delete from rewindr_workers where queue_name=$1',[queueName]);await db.query('select pgmq.drop_queue($1)',[queueName]);}
    }catch(error){cleanupError=error;console.error(`Temporary queue cleanup needs attention for owner ${owner}, queue ${queueName}. Existing archive data was not targeted.`);}
    await db.end();
    if(!root.startsWith(join(tmpdir(),'rewindr-queue-verification-')))throw Error('Unexpected local cleanup path');await rm(root,{recursive:true,force:true});
    if(cleanupError)throw cleanupError;
  }
  console.log('Temporary verification queue, objects and metadata removed. Existing videos and recordings were retained.');
}
run().catch(error=>{console.error(`Queue verification stopped during ${step}. ${safeDatabaseError(error)}`);process.exitCode=1;});
