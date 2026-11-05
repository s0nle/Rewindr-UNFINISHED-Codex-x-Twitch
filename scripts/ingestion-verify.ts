import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {setTimeout as wait} from 'node:timers/promises';
import ffmpeg from 'ffmpeg-static';
import {databaseClient,safeDatabaseError} from '../src/db-connection.js';
import {configuredSupabaseClient,createSupabasePersistence} from '../src/supabase-persistence.js';
import {configuredChunkStorage,chunkObjectPath} from '../src/chunk-storage.js';
import {SupabaseJobQueue} from '../src/jobs.js';
import {SupabaseIngestions} from '../src/ingestions.js';
import {JobWorker} from '../src/job-worker.js';
import {buildApp} from '../src/app.js';
import {runRecordedIngestion} from '../src/recorded-ingestion.js';
import type {VideoSearchProvider} from '../src/types.js';
let step='configuration';
async function run(){
  const db=await databaseClient();const owner=randomUUID(),queueName=`rewindr_verify_${randomUUID().replaceAll('-','')}`;
  const root=await mkdtemp(join(tmpdir(),'rewindr-ingestion-verification-'));const env={...process.env,REWINDR_OWNER_ID:owner};
  const client=configuredSupabaseClient(env),queue=new SupabaseJobQueue(client,owner,queueName),ingestions=new SupabaseIngestions(client,owner,queueName),storage=configuredChunkStorage(env);
  let queueCreated=false,ownerCreated=false,app:Awaited<ReturnType<typeof buildApp>>|undefined;let pumping=false;let pump:Promise<void>|undefined;let pumpFailure:unknown;
  const objects=new Set<string>();let downloads=0,uploads=0,indexes=0,transfers=0,loseSource=true;
  const download=storage.download.bind(storage);storage.download=async(...args)=>{downloads++;await download(...args);};
  const upload=storage.upload.bind(storage);storage.upload=async(...args)=>{objects.add(chunkObjectPath(owner,args[0],args[1],args[3]));return upload(...args);};
  try{
    step='isolated queue and owner';await db.connect();await db.query('select pgmq.create($1)',[queueName]);queueCreated=true;
    await db.query("insert into rewindr_broadcasters(id,label) values($1,'Rewindr temporary ingestion verification')",[owner]);ownerCreated=true;
    step='synthetic recording';const source=join(root,'source.mp4');
    await promisify(execFile)(process.env.FFMPEG_PATH||ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=5','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','68','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac',source],{windowsHide:true,timeout:30000});
    const unexpected=async():Promise<never>=>{throw Error('Unexpected real-provider operation');};
    // No SDK client or external video-provider requests: only Supabase is live.
    const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,
      async validateIndex(index){assert.equal(index,'verification-index');},
      async createUpload(_file,_name,bytes){uploads++;return {uploadId:`verify-upload-${uploads}`,assetId:`verify-asset-${uploads}`,chunkSize:bytes,totalChunks:1,completedChunks:0,uploadHeaders:{}};},
      async transfer(file,_bytes,_session,checkpoint){transfers++;if(loseSource){loseSource=false;await rm(file);throw Error('Simulated staged source loss');}await checkpoint(1);},
      async assetStatus(){return 'ready';},async indexAsset(_index,asset){indexes++;return `indexed-${asset}`;},async indexStatus(){return 'ready';},
      async searchMany(_index,ids){assert.equal(ids.length,2);return [{videoId:ids[0]!,startSeconds:59,endSeconds:60,rank:1},{videoId:ids[1]!,startSeconds:0,endSeconds:1,rank:2}];}};
    const persistence=createSupabasePersistence(root,env);const makeApp=()=>buildApp({dataDir:root,persistence:createSupabasePersistence(root,env),storage,jobs:queue,ingestions,provider});
    app=await makeApp();const address=await app.listen({host:'127.0.0.1',port:0});const port=Number(new URL(address).port);
    const worker=new JobWorker({queue,dataDir:root,persistence:()=>persistence,storage:()=>storage,provider,ingestions:()=>ingestions});
    pumping=true;pump=(async()=>{try{while(pumping){await worker.runOnce();await wait(100);}}catch(error){pumpFailure=error;}})();
    step='FFmpeg, HTTP intake, storage and queued indexing';
    const produced=await runRecordedIngestion({source,workspace:join(root,'producer'),indexId:'verification-index',title:'Synthetic ingestion verification',offset:7200,
      creatorAuthorized:true,port,maxPending:2,pollMs:250,jobTimeoutMs:120000,log:message=>{if(message.includes('closed and queued'))console.log(message);}});
    pumping=false;await pump;pump=undefined;assert.equal(pumpFailure,undefined,'Worker loop failed');
    step='ready coverage and recovery';const status=(await ingestions.status(produced.session.id))!;
    assert.equal(status.status,'ready');assert.equal(status.totalAcceptedChunks,2);assert.equal(status.searchableThroughSeconds,68);assert.equal(status.storedThroughSeconds,68);
    assert.equal(downloads,1,'Lost staged media was not recovered from private Storage');assert.equal(uploads,2,'Provider creation was duplicated');assert.equal(indexes,2);assert.equal(transfers,3);
    step='resume without duplicate submissions';await runRecordedIngestion({resume:produced.workspace,creatorAuthorized:true,port,log:()=>{}});assert.equal(uploads,2);assert.equal(indexes,2);
    await app.close();app=undefined;
    step='API restart and boundary replay';app=await makeApp();
    const result=await app.inject({method:'POST',url:`/broadcasts/${produced.session.id}/search`,payload:{question:'A synthetic boundary encounter'}});
    assert.equal(result.statusCode,200);assert.equal(result.json().encounters.length,1);
    const manifest=await app.inject(result.json().encounters[0].manifestUrl);assert.equal(manifest.statusCode,200);assert.equal(manifest.json().segments.length,2);assert.equal(manifest.json().segments[1].broadcastStartSeconds,7260);
    const media=await fetch(manifest.json().segments[0].url,{headers:{range:'bytes=0-31'},signal:AbortSignal.timeout(20000)});assert.equal(media.status,206);assert.equal((await media.arrayBuffer()).byteLength,32);
    assert.equal(await new SupabaseIngestions(client,randomUUID(),queueName).get(produced.session.id),undefined);
    console.log('Verified real FFmpeg/AAC chunks, HTTP intake, pgmq jobs, private Storage, staged-file recovery, ready-only source coverage, resume without duplicate submissions, API restart and cross-chunk signed playback. TwelveLabs upload/index/search were mocked; no video-provider requests were made.');
  }finally{
    pumping=false;await pump;await app?.close();let cleanupError:unknown;
    try{
      if([...objects].some(path=>!path.startsWith(`${owner}/`)))throw Error('Unexpected object cleanup scope');
      if(objects.size){const removed=await client.storage.from(storage.bucket).remove([...objects]);if(removed.error)throw Error('Temporary object cleanup failed');}
      if(ownerCreated)await db.query("delete from rewindr_broadcasters where id=$1 and label='Rewindr temporary ingestion verification'",[owner]);
      if(queueCreated){await db.query('delete from rewindr_workers where queue_name=$1',[queueName]);await db.query('select pgmq.drop_queue($1)',[queueName]);}
    }catch(error){cleanupError=error;console.error(`Temporary cleanup needs attention for owner ${owner}, queue ${queueName}. Existing archive data was not targeted.`);}
    await db.end();if(!root.startsWith(join(tmpdir(),'rewindr-ingestion-verification-')))throw Error('Unexpected cleanup path');await rm(root,{recursive:true,force:true});if(cleanupError)throw cleanupError;
  }
  console.log('Temporary ingestion, queue, objects and source files removed. Existing archive footage was retained.');
}
run().catch(error=>{console.error(`Ingestion verification stopped during ${step}. ${safeDatabaseError(error)}`);process.exitCode=1;});
