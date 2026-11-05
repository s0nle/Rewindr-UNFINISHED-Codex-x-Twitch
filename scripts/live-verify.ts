import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {spawn} from 'node:child_process';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createServer} from 'node:net';
import {setTimeout as wait} from 'node:timers/promises';
import ffmpeg from 'ffmpeg-static';
import {databaseClient,safeDatabaseError} from '../src/db-connection.js';
import {configuredSupabaseClient,createSupabasePersistence} from '../src/supabase-persistence.js';
import {configuredChunkStorage,chunkObjectPath} from '../src/chunk-storage.js';
import {SupabaseJobQueue} from '../src/jobs.js';
import {SupabaseIngestions} from '../src/ingestions.js';
import {JobWorker} from '../src/job-worker.js';
import {buildApp} from '../src/app.js';
import {runLiveCapture} from '../src/live-capture.js';
import type {VideoSearchProvider} from '../src/types.js';
let step='configuration';
async function run(){
  const db=await databaseClient(),owner=randomUUID(),queueName=`rewindr_verify_${randomUUID().replaceAll('-','')}`,root=await mkdtemp(join(tmpdir(),'rewindr-live-verification-'));
  const env={...process.env,REWINDR_OWNER_ID:owner},client=configuredSupabaseClient(env),storage=configuredChunkStorage(env),queue=new SupabaseJobQueue(client,owner,queueName),ingestions=new SupabaseIngestions(client,owner,queueName);
  const objects=new Set<string>(),controller=new AbortController();let app:Awaited<ReturnType<typeof buildApp>>|undefined,publisher:ReturnType<typeof spawn>|undefined,pump:Promise<void>|undefined,capture:ReturnType<typeof runLiveCapture>|undefined;
  let ownerCreated=false,queueCreated=false,pumping=false,pumpError:unknown,uploads=0,indexes=0,publisherEnded=false,firstReadyWhileLive=false,providerReady=false,broadcastId:string|undefined;
  const upload=storage.upload.bind(storage);storage.upload=async(...args)=>{objects.add(chunkObjectPath(owner,args[0],args[1],args[3]));return upload(...args);};
  try{
    step='isolated owner/queue';await db.connect();await db.query('select pgmq.create($1)',[queueName]);queueCreated=true;await db.query("insert into rewindr_broadcasters(id,label) values($1,'Rewindr temporary live verification')",[owner]);ownerCreated=true;
    const unexpected=async():Promise<never>=>{throw Error('Unexpected provider operation');};
    // Real local RTMP + FFmpeg + hosted Supabase; video-model operations are mock-only.
    const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,async validateIndex(id){assert.equal(id,'verification-index');},
      async createUpload(_file,_name,bytes){uploads++;return {uploadId:`verify-upload-${uploads}`,assetId:`verify-asset-${uploads}`,chunkSize:bytes,totalChunks:1,completedChunks:0,uploadHeaders:{}};},
      async transfer(_file,_bytes,_session,checkpoint){await checkpoint(1);},async assetStatus(){return 'ready';},async indexAsset(_index,asset){indexes++;return `indexed-${asset}`;},async indexStatus(){providerReady=true;return 'ready';},
      async searchMany(_index,ids){return ids.map((id,i)=>({videoId:id,startSeconds:i===0?59:0,endSeconds:i===0?60:1,rank:i+1}));}};
    const persistence=createSupabasePersistence(root,env);app=await buildApp({dataDir:root,persistence,storage,jobs:queue,ingestions,provider});const address=await app.listen({host:'127.0.0.1',port:0}),apiPort=Number(new URL(address).port);
    const portServer=createServer();await new Promise<void>(resolve=>portServer.listen(0,'127.0.0.1',resolve));const rtmpPort=(portServer.address() as {port:number}).port;await new Promise<void>(resolve=>portServer.close(()=>resolve()));
    const worker=new JobWorker({queue,dataDir:root,persistence:()=>persistence,storage:()=>storage,provider,ingestions:()=>ingestions});
    pumping=true;pump=(async()=>{try{while(pumping){await db.query('select pgmq.set_vt($1,msg_id,0) from pgmq.read($1,0,100)',[queueName]);await worker.runOnce();
      if(providerReady&&!publisherEnded&&!firstReadyWhileLive&&broadcastId)firstReadyWhileLive=(await ingestions.status(broadcastId))!.readyChunkCount>0;await wait(100);}}catch(e){pumpError=e;controller.abort();}})();
    let disconnected:()=>void=()=>{};const eof=new Promise<void>(resolve=>{disconnected=resolve;});
    step='RTMP receiver startup';capture=runLiveCapture({workspace:join(root,'capture'),indexId:'verification-index',title:'Synthetic live verification',rtmpPort,maxSeconds:100,creatorAuthorized:true,port:apiPort,signal:controller.signal,
      log:message=>{if(message.startsWith('Broadcast: '))broadcastId=message.slice('Broadcast: '.length);if(message.includes('closed and validated'))console.log('A live segment closed and validated.');if(message.includes('disconnected'))disconnected();}});void capture.catch(()=>{});
    let bound=false;for(let i=0;i<200;i++){const probe=createServer();bound=await new Promise<boolean>((resolve,reject)=>{probe.once('error',(e:NodeJS.ErrnoException)=>e.code==='EADDRINUSE'?resolve(true):reject(e));probe.listen(rtmpPort,'127.0.0.1',()=>probe.close(()=>resolve(false)));});if(bound)break;await wait(50);}assert.ok(bound,'RTMP receiver did not bind');
    step='68-second real-time synthetic broadcast';console.log('Publishing 68 seconds of synthetic RTMP footage; the first chunk should close at about 60 seconds.');
    publisher=spawn(process.env.FFMPEG_PATH||ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-re','-f','lavfi','-i','testsrc2=s=360x360:r=10','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','68','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-c:a','aac','-f','flv',`rtmp://127.0.0.1:${rtmpPort}/live/${broadcastId}`],{windowsHide:true,stdio:'ignore'});
    const code=await new Promise<number|null>((resolve,reject)=>{publisher!.once('error',reject);publisher!.once('close',code=>{publisherEnded=true;resolve(code);});});assert.equal(code,0,'Synthetic publisher failed');
    await Promise.race([eof,wait(20000).then(()=>{throw Error('Receiver did not finish closed segments');})]);controller.abort();const result=await capture;capture=undefined;
    step='durable indexing and coverage';for(let i=0;i<300;i++){if((await ingestions.status(result.session.id))!.status==='ready')break;await wait(100);}assert.equal(pumpError,undefined);const status=(await ingestions.status(result.session.id))!;
    console.log('Synthetic coverage check:',{status:status.status,chunks:status.totalAcceptedChunks,uploads,indexes,firstReadyWhileLive,phases:status.chunks.map(c=>({phase:c.phase,jobState:c.jobState,error:c.error}))});
    assert.equal(status.mode,'live');assert.equal(status.status,'ready','All hosted live chunks should become ready');assert.equal(status.totalAcceptedChunks,2);assert.equal(uploads,2);assert.equal(indexes,2);
    assert.ok(Math.abs(result.session.chunks[0]!.endSeconds-result.session.chunks[0]!.startSeconds-60)<0.2);assert.ok(result.session.chunks[1]!.endSeconds>result.session.chunks[0]!.endSeconds);
    step='search and cross-chunk replay';const response=await app.inject({method:'POST',url:`/broadcasts/${result.session.id}/search`,payload:{question:'Synthetic boundary event'}});assert.equal(response.statusCode,200,'Search HTTP status');assert.equal(response.json().encounters.length,1);
    const manifest=await app.inject(response.json().encounters[0].manifestUrl);assert.equal(manifest.statusCode,200,'Replay HTTP status');assert.equal(manifest.json().segments.length,2);
    const media=await fetch(manifest.json().segments[0].url,{headers:{range:'bytes=0-31'},signal:AbortSignal.timeout(20000)});assert.equal(media.status,206);await media.arrayBuffer();
    await runLiveCapture({resume:result.workspace,finish:true,creatorAuthorized:true,port:apiPort,log:()=>{}});assert.equal(uploads,2);
    console.log('Verified real-time RTMP, a full 60-second straight chunk, incremental ready coverage, private Storage, native pgmq, durable indexing, signed cross-chunk replay and idempotent finish/cleanup. TwelveLabs was mocked; no indexing or analysis charges were incurred.');
  }finally{
    controller.abort();publisher?.kill();await capture?.catch(()=>{});pumping=false;await pump;await app?.close();let cleanupError:unknown;
    try{if([...objects].some(path=>!path.startsWith(`${owner}/`)))throw Error('Unexpected object cleanup scope');if(objects.size){const result=await client.storage.from(storage.bucket).remove([...objects]);if(result.error)throw Error('Object cleanup failed');}
      if(ownerCreated)await db.query("delete from rewindr_broadcasters where id=$1 and label='Rewindr temporary live verification'",[owner]);if(queueCreated){await db.query('delete from rewindr_workers where queue_name=$1',[queueName]);await db.query('select pgmq.drop_queue($1)',[queueName]);}
    }catch(e){cleanupError=e;console.error(`Temporary live verification cleanup needs attention: owner ${owner}, queue ${queueName}.`);}
    await db.end();if(!root.startsWith(join(tmpdir(),'rewindr-live-verification-')))throw Error('Unexpected cleanup path');await rm(root,{recursive:true,force:true});if(cleanupError)throw cleanupError;
  }
  console.log('Temporary live verification media, owner and queue removed. Existing archive retained.');
}
run().catch(error=>{console.error(`Live verification stopped during ${step}. ${safeDatabaseError(error)}`);if((error as {code?:string})?.code==='ERR_ASSERTION')console.error('Synthetic assertion:',(error as Error).message.split('\n')[0]);process.exitCode=1;});
