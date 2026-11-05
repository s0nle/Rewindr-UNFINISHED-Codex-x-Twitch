import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {execFile,spawn} from 'node:child_process';
import {createServer} from 'node:net';
import {promisify} from 'node:util';
import {mkdtemp,readFile,writeFile,rm,mkdir,copyFile,open,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as wait} from 'node:timers/promises';
import ffmpeg from 'ffmpeg-static';
import {PGlite} from '@electric-sql/pglite';
import {createClient} from '@supabase/supabase-js';
import {applyTestMigrations} from './queue-shim.js';
import {createLocalPersistence} from '../src/local-persistence.js';
import {broadcastSchema} from '../src/metadata.js';
import {SupabaseIngestions} from '../src/ingestions.js';
import {SupabaseJobQueue} from '../src/jobs.js';
import {JobWorker} from '../src/job-worker.js';
import {chunkJobFile} from '../src/staged-media.js';
import {chunkObjectPath,type ChunkStorage} from '../src/chunk-storage.js';
import {ingestStorageKey} from '../src/ingestion-types.js';
import {runRecordedIngestion} from '../src/recorded-ingestion.js';
import {runLiveCapture} from '../src/live-capture.js';
import {splitRecording,fileIdentity} from '../src/segmenter.js';
import {buildApp} from '../src/app.js';
import type {Persistence} from '../src/persistence.js';
import type {VideoSearchProvider,UploadSession} from '../src/types.js';

let db:PGlite,fixtureRoot:string,source:string,firstFile:string;
before(async()=>{
  db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);
  fixtureRoot=await mkdtemp(join(tmpdir(),'rewindr-ingestion-flow-fixture-'));source=join(fixtureRoot,'source.mp4');
  await promisify(execFile)(ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=5','-t','68','-an','-c:v','libx264','-pix_fmt','yuv420p',source],{windowsHide:true,timeout:30000});
  await splitRecording({source,directory:join(fixtureRoot,'reference'),durationSeconds:68,onChunk:async c=>{if(c.ordinal===0)firstFile=c.path;}});
});
after(async()=>{await db?.close();if(!fixtureRoot.startsWith(join(tmpdir(),'rewindr-ingestion-flow-fixture-')))throw Error('Bad cleanup');await rm(fixtureRoot,{recursive:true,force:true});});

async function setup(t:{after:(fn:()=>Promise<void>)=>void}) {
  const owner=randomUUID(),root=await mkdtemp(join(tmpdir(),'rewindr-ingestion-flow-'));
  await db.query('insert into rewindr_broadcasters(id,label) values($1,$2)',[owner,'test']);
  const errors:{rpc:string;code?:string;message?:string}[]=[];let failIndexCheckpoint=true;
  const client=createClient('https://test.supabase.co','test-key',{auth:{persistSession:false},global:{fetch:async(input,init)=>{
    const url=new URL(String(input));
    try {
      if(url.pathname.includes('/rpc/')) {
        const rpc=url.pathname.split('/').at(-1)!;assert.match(rpc,/^rewindr_[a-z_]+$/);
        const args=JSON.parse(String(init?.body)) as Record<string,unknown>;const keys=Object.keys(args);keys.forEach(key=>assert.match(key,/^p_[a-z_]+$/));
        if(rpc==='rewindr_checkpoint_ingest_chunk'&&(args.p_checkpoint as {phase?:string})?.phase==='indexing'&&failIndexCheckpoint){failIndexCheckpoint=false;return Response.json({code:'53300',message:'temporary test failure'},{status:503});}
        const result=await db.query<{value:unknown}>(`select public.${rpc}(${keys.map((key,i)=>`${key}=>$${i+1}`).join(',')}) value`,keys.map(key=>typeof args[key]==='object'&&args[key]!==null?JSON.stringify(args[key]):args[key]));
        return Response.json(result.rows[0]!.value??null);
      }
      const table=url.pathname.split('/').at(-1)!;assert.ok(['rewindr_jobs','rewindr_ingestions','rewindr_ingest_chunks'].includes(table));
      const filters=[...url.searchParams].filter(([key])=>['id','owner_id','queue_name'].includes(key));
      const rows=(await db.query(`select * from public.${table} where ${filters.map(([key],i)=>`${key}=$${i+1}`).join(' and ')}`,filters.map(([,value])=>value.replace(/^eq\./,'')))).rows;
      return Response.json(rows);
    }catch(error){const e=error as {code?:string;message?:string};errors.push({rpc:url.pathname,code:e.code,message:e.message});return Response.json({code:e.code||'TEST_ERROR',message:'Test operation failed'},{status:400});}
  }}});
  const queue=new SupabaseJobQueue(client,owner),ingestions=new SupabaseIngestions(client,owner);const local=createLocalPersistence(root);await local.init();
  const persistence:Persistence={kind:'supabase',recordings:local.recordings,health:async()=>{},init:()=>local.init(),broadcasts:{init:async()=>{},all:async()=>[],put:async()=>{throw Error('Unexpected metadata replacement');},
    get:async id=>{const data=(await db.query<{b:unknown}>('select rewindr_get_broadcast($1,$2) b',[owner,id])).rows[0]!.b;return data?broadcastSchema.parse(data):undefined;},attachStorage:async()=>{throw Error('Unexpected legacy attachment');}}};
  const objects=new Map<string,Buffer>();let downloads=0,uploads=0,creations=0,indexes=0,transfers=0,searches=0;let simulateTransferLoss=true;
  const storage:ChunkStorage={bucket:'rewindr-chunks',async init(){},async upload(bid,key,file,sha,bytes){const path=chunkObjectPath(owner,bid,key,sha),reused=objects.has(path);if(!reused){objects.set(path,await readFile(file));uploads++;}
    return {reference:{bucket:'rewindr-chunks',path,sha256:sha,bytes,uploadedAt:new Date().toISOString()},reused};},
    async sign(bid,asset,reference,key){assert.equal(reference.path,chunkObjectPath(owner,bid,key||asset,reference.sha256));return {url:'https://example.invalid/private',expiresAt:new Date(Date.now()+300000).toISOString()};},
    async download(_bid,_key,reference,destination){downloads++;await writeFile(destination,objects.get(reference.path)!,{flag:'wx'});}};
  const unexpected=async():Promise<never>=>{throw Error('Unexpected provider operation');};
  const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,
    async validateIndex(index){if(index!=='test-index')throw Error('Wrong index');},
    async createUpload(_file,_name,bytes){creations++;return {uploadId:`upload-${creations}`,assetId:`asset-${creations}`,chunkSize:bytes,totalChunks:1,completedChunks:0,uploadHeaders:{}};},
    async transfer(file,_bytes,_session:UploadSession,checkpoint){transfers++;if(simulateTransferLoss){simulateTransferLoss=false;await rm(file);throw Error('Simulated worker/source loss after Storage checkpoint');}await checkpoint(1);},
    async assetStatus(){return 'ready';},async indexAsset(_index,asset){indexes++;return `indexed-${asset}`;},async indexStatus(){return 'ready';},
    async searchMany(_index,ids){searches++;return ids.length===2?[{videoId:ids[0]!,startSeconds:59,endSeconds:60,rank:1},{videoId:ids[1]!,startSeconds:0,endSeconds:1,rank:2}]:[];}};
  const app=await buildApp({dataDir:root,persistence,jobs:queue,storage,ingestions,provider});
  const worker=new JobWorker({queue,dataDir:root,persistence:()=>persistence,storage:()=>storage,provider,ingestions:()=>ingestions,leaseRetryMs:1,processorRetry:{initialDelayMs:1,maxDelayMs:2,maxFailures:5}});
  t.after(async()=>{await app.close();await db.exec('delete from pgmq.q_rewindr_jobs;delete from pgmq.a_rewindr_jobs');await db.query('delete from rewindr_broadcasters where id=$1',[owner]);
    if(!root.startsWith(join(tmpdir(),'rewindr-ingestion-flow-')))throw Error('Bad cleanup');await rm(root,{recursive:true,force:true});});
  return {owner,root,app,worker,queue,ingestions,persistence,storage,provider,errors,get downloads(){return downloads;},get uploads(){return uploads;},get creations(){return creations;},get indexes(){return indexes;},get transfers(){return transfers;},get searches(){return searches;}};
}

test('recording -> FFmpeg -> queued ingestion -> private Storage -> indexed chunks -> search/replay, with recovery and idempotent resume',async t=>{
  const state=await setup(t);let firstReadyBeforeSecond=false;
  state.app.addHook('onRequest',async req=>{if(req.url.endsWith('/chunks/1')){const id=req.url.split('/')[2]!;firstReadyBeforeSecond=(await state.persistence.broadcasts.get(id))!.chunks.length===1;}});
  const address=await state.app.listen({host:'127.0.0.1',port:0});const port=Number(new URL(address).port);
  let pumping=true;let pumpFailure:unknown;
  const pump=(async()=>{try{while(pumping){await db.exec("update pgmq.q_rewindr_jobs set vt=clock_timestamp()-interval '1 second'");await state.worker.runOnce();await wait(5);}}catch(error){pumpFailure=error;}})();
  const workspace=join(state.root,'producer');let produced:Awaited<ReturnType<typeof runRecordedIngestion>>;
  try{produced=await runRecordedIngestion({source,workspace,title:'Test stream',indexId:'test-index',offset:7200,creatorAuthorized:true,port,maxPending:1,pollMs:10,jobTimeoutMs:60000,log:()=>{}});}
  finally{pumping=false;await pump;}
  assert.equal(pumpFailure,undefined,JSON.stringify(state.errors));assert.equal(firstReadyBeforeSecond,true);
  const status=(await state.ingestions.status(produced!.session.id))!;assert.equal(status.status,'ready',JSON.stringify(state.errors));assert.equal(status.searchableThroughSeconds,68);assert.equal(status.storedThroughSeconds,68);
  assert.equal(state.creations,2);assert.equal(state.indexes,2);assert.equal(state.uploads,2);assert.equal(state.downloads,1);assert.equal(state.transfers,3);
  const searched=await state.app.inject({method:'POST',url:`/broadcasts/${produced!.session.id}/search`,payload:{question:'Someone asks for a photo'}});assert.equal(searched.statusCode,200,searched.body);assert.equal(searched.json().encounters.length,1);
  const replay=await state.app.inject(searched.json().encounters[0].manifestUrl);assert.equal(replay.statusCode,200,replay.body);assert.equal(replay.json().segments.length,2);assert.equal(replay.json().segments[1].broadcastStartSeconds,7260);
  await runRecordedIngestion({resume:workspace,creatorAuthorized:true,port,pollMs:10,log:()=>{}});assert.equal(state.creations,2);assert.equal(state.indexes,2);assert.equal(state.uploads,2);
});

test('new ingestion has an honest empty search, rejects unauthorized input and keeps private state owner-scoped',async t=>{
  const state=await setup(t);const id=randomUUID();const payload={id,title:'Test',indexId:'test-index',sourceDurationSeconds:68,sourceOffsetSeconds:0,sourceFingerprint:'a'.repeat(64)};
  assert.equal((await state.app.inject({method:'POST',url:'/ingestions',payload})).statusCode,400);
  assert.equal((await state.app.inject({method:'POST',url:'/ingestions',headers:{'x-creator-authorized':'true'},payload})).statusCode,201);
  const response=await state.app.inject({method:'POST',url:`/broadcasts/${id}/search`,payload:{question:'person in jersey'}});assert.equal(response.statusCode,200);assert.deepEqual(response.json().encounters,[]);assert.equal(response.json().pendingIngestion,true);assert.equal(state.searches,0);
  const other=new SupabaseIngestions(createClient('https://test.supabase.co','key',{global:{fetch:async()=>Response.json([])},auth:{persistSession:false}}),randomUUID());assert.equal(await other.get(id),undefined);
  assert.equal((await state.app.inject({method:'POST',url:`/ingestions/${id}/complete`,headers:{'x-creator-authorized':'true'}})).statusCode,409);
  assert.equal((await state.app.inject({method:'POST',url:`/ingestions/${id}/chunks/9999`,headers:{'x-creator-authorized':'true'}})).statusCode,400);
});

test('an interrupted chunk upload creation is reviewed without calling the provider again',async t=>{
  const state=await setup(t);const id=randomUUID();await state.ingestions.create({id,title:'Test',indexId:'test-index',sourceDurationSeconds:68,sourceOffsetSeconds:0,sourceFingerprint:'a'.repeat(64)});
  const fileId=randomUUID();await mkdir(join(state.root,'chunk-jobs'),{recursive:true});await writeFile(chunkJobFile(state.root,fileId),await readFile(firstFile));const identity=await fileIdentity(firstFile);
  const chunk=await state.ingestions.accept(id,{fileId,ordinal:0,filename:'chunk_0000.mp4',startSeconds:0,endSeconds:60,...identity});
  const job=(await state.queue.claim(randomUUID(),300))!;assert.equal(job.id,chunk.job_id);await state.ingestions.checkpoint(job,{phase:'creating_upload'});await state.queue.finish(job,'pending',0);
  await state.worker.runOnce();const status=(await state.queue.get(job.id))!;assert.equal(status.state,'needs_review');assert.equal(state.creations,0);assert.equal((await state.ingestions.status(id))!.status,'attention_required');
});

test('live RTMP reconnect -> explicit gap -> incremental private ingestion/search; sealed resume prunes only stored media',async t=>{
  const state=await setup(t),controller=new AbortController();const server=createServer();await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));const value=(server.address() as {port:number}).port;await new Promise<void>(resolve=>server.close(()=>resolve()));
  const address=await state.app.listen({host:'127.0.0.1',port:0}),apiPort=Number(new URL(address).port);state.provider.searchMany=async(_index,ids)=>ids.map((id,i)=>({videoId:id,startSeconds:1,endSeconds:2,rank:i+1}));
  let pumping=true,pumpError:unknown;const pump=(async()=>{try{while(pumping){await db.exec("update pgmq.q_rewindr_jobs set vt=clock_timestamp()-interval '1 second'");await state.worker.runOnce();await wait(10);}}catch(e){pumpError=e;}})();
  let reconnect:()=>void=()=>{},secondDisconnect:()=>void=()=>{},disconnects=0;const reconnected=new Promise<void>(resolve=>{reconnect=resolve;}),secondEnded=new Promise<void>(resolve=>{secondDisconnect=resolve;});let firstSearchableWhileLive=false;
  const capture=runLiveCapture({workspace:join(state.root,'live'),indexId:'test-index',title:'Synthetic live',rtmpPort:value,maxSeconds:60,creatorAuthorized:true,port:apiPort,signal:controller.signal,segmentSeconds:5,pollMs:20,
    log:message=>{if(message.includes('disconnected')){disconnects++;if(disconnects===1)reconnect();if(disconnects===2)secondDisconnect();}}});
  const listening=async()=>{for(let i=0;i<100;i++){const probe=createServer();const busy=await new Promise<boolean>((resolve,reject)=>{probe.once('error',(e:NodeJS.ErrnoException)=>e.code==='EADDRINUSE'?resolve(true):reject(e));probe.listen(value,'127.0.0.1',()=>probe.close(()=>resolve(false)));});if(busy)return;await wait(50);}throw Error('Receiver did not listen');};
  const publish=async()=>{const session=JSON.parse(await readFile(join(state.root,'live','live-session.json'),'utf8')) as {id:string};const child=spawn(ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-re','-f','lavfi','-i','testsrc2=s=360x360:r=10','-t','6','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-f','flv',`rtmp://127.0.0.1:${value}/live/${session.id}`],{windowsHide:true,stdio:'ignore'});
    const code=await new Promise<number|null>((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});assert.equal(code,0);};
  let result:Awaited<ReturnType<typeof runLiveCapture>>;
  try{await listening();await publish();await reconnected;await listening();
    const liveSession=JSON.parse(await readFile(join(state.root,'live','live-session.json'),'utf8')) as {id:string};
    for(let i=0;i<200;i++){if((await state.persistence.broadcasts.get(liveSession.id))!.chunks.length){firstSearchableWhileLive=true;break;}await wait(20);}
    await publish();await secondEnded;controller.abort();result=await capture;
    for(let i=0;i<400;i++){if((await state.ingestions.status(result.session.id))!.status==='ready')break;await wait(20);}
  }finally{controller.abort();await capture.catch(()=>{});pumping=false;await pump;}
  assert.equal(pumpError,undefined,JSON.stringify(state.errors));assert.equal(firstSearchableWhileLive,true);
  assert.equal(result!.session.chunks.length,2);assert.equal(result!.session.skipped.length,2);assert.ok(result!.session.chunks[1]!.startSeconds>result!.session.chunks[0]!.endSeconds+0.5,'Reconnect gap must not be removed');
  const status=(await state.ingestions.status(result!.session.id))!;assert.equal(status.mode,'live');assert.equal(status.status,'ready');assert.equal(state.creations,2);assert.equal(state.indexes,2);
  const searched=await state.app.inject({method:'POST',url:`/broadcasts/${result!.session.id}/search`,payload:{question:'Synthetic live scene'}});assert.equal(searched.statusCode,200,searched.body);assert.equal(searched.json().encounters.length,2);
  const replay=await state.app.inject(searched.json().encounters[0].manifestUrl);assert.equal(replay.statusCode,200,replay.body);assert.equal(replay.json().segments.length,1);
  await runLiveCapture({resume:result!.workspace,finish:true,creatorAuthorized:true,port:apiPort,log:()=>{}});
  for(const chunk of result!.session.chunks)await assert.rejects(()=>readFile(join(result!.workspace,'chunks',chunk.filename)),{code:'ENOENT'});
  assert.ok((await readFile(join(result!.workspace,'live-session.json'),'utf8')).includes('"sealed"'));
});

async function resumeFixture(root:string,segments:number){
  const workspace=join(root,'resumed-live'),attempt=join(workspace,'attempt_0000');await mkdir(attempt,{recursive:true});const id=randomUUID();
  // Deliberately use the original session format, which did not have a quarantine ledger.
  const session={version:1,id,title:'Resumed live',indexId:'test-index',offset:0,fingerprint:'a'.repeat(64),originMs:Date.now(),maxSeconds:3600,kind:'rtmp',rtmpPort:1935,phase:'interrupted',ended:false,
    attempts:[{directory:'attempt_0000',startedMs:Date.now(),base:0,consumed:0}],chunks:[],skipped:[]};
  await writeFile(join(workspace,'live-session.json'),JSON.stringify(session));
  await writeFile(join(attempt,'closed.csv'),Array.from({length:segments},(_,i)=>`raw_${String(i).padStart(6,'0')}.mp4,${i*60},${(i+1)*60}\n`).join(''));
  return {workspace,attempt,id};
}
test('resume quarantines corrupt and oversized raw segments, keeps their gaps, and accepts later footage without renumbering jobs',async t=>{
  const state=await setup(t),fixture=await resumeFixture(state.root,3);await writeFile(join(fixture.attempt,'raw_000000.mp4'),'corrupt');
  const oversized=await open(join(fixture.attempt,'raw_000001.mp4'),'w');try{await oversized.truncate(50_000_001);}finally{await oversized.close();}
  await copyFile(firstFile,join(fixture.attempt,'raw_000002.mp4'));
  const address=await state.app.listen({host:'127.0.0.1',port:0}),port=Number(new URL(address).port);
  const result=await runLiveCapture({resume:fixture.workspace,finish:true,creatorAuthorized:true,port,keepLocal:true,pollMs:10,log:()=>{}});
  assert.deepEqual(result.session.quarantined.map(c=>c.reason),['invalid_media','oversize']);assert.equal(result.session.attempts[0]!.consumed,3);assert.equal(result.session.chunks.length,1);
  assert.equal(result.session.chunks[0]!.ordinal,0);assert.equal(result.session.chunks[0]!.startSeconds,120);assert.equal(result.session.chunks[0]!.endSeconds,180);
  assert.equal((await stat(join(fixture.attempt,'quarantine','raw_000001.mp4'))).size,50_000_001);assert.equal(await readFile(join(fixture.attempt,'quarantine','raw_000000.mp4'),'utf8'),'corrupt');
  const status=(await state.ingestions.status(fixture.id))!;assert.deepEqual(status.gaps,[{startSeconds:0,endSeconds:120}]);assert.equal(status.totalAcceptedChunks,1);
  const again=await runLiveCapture({resume:fixture.workspace,finish:true,creatorAuthorized:true,port,keepLocal:true,log:()=>{}});
  assert.deepEqual(again.session.quarantined,result.session.quarantined);assert.equal(again.session.chunks[0]!.jobId,result.session.chunks[0]!.jobId);assert.equal(state.creations,0);
});
test('a missing raw segment is recorded once and does not block later valid footage on resume',async t=>{
  const state=await setup(t),fixture=await resumeFixture(state.root,2);await copyFile(firstFile,join(fixture.attempt,'raw_000001.mp4'));
  const address=await state.app.listen({host:'127.0.0.1',port:0}),port=Number(new URL(address).port);
  const result=await runLiveCapture({resume:fixture.workspace,finish:true,creatorAuthorized:true,port,keepLocal:true,pollMs:10,log:()=>{}});
  assert.equal(result.session.quarantined[0]!.reason,'missing');assert.equal(result.session.chunks[0]!.startSeconds,60);assert.equal(result.session.attempts[0]!.consumed,2);
});
test('missing FFprobe remains an operational error, and a subsequent resume can save and accept the original footage',async t=>{
  const state=await setup(t),fixture=await resumeFixture(state.root,1);await copyFile(firstFile,join(fixture.attempt,'raw_000000.mp4'));
  const address=await state.app.listen({host:'127.0.0.1',port:0}),port=Number(new URL(address).port),previous=process.env.FFPROBE_PATH;
  try{process.env.FFPROBE_PATH=join(state.root,'missing-ffprobe.exe');await assert.rejects(runLiveCapture({resume:fixture.workspace,finish:true,creatorAuthorized:true,port,log:()=>{}}));}
  finally{if(previous===undefined)delete process.env.FFPROBE_PATH;else process.env.FFPROBE_PATH=previous;}
  const saved=JSON.parse(await readFile(join(fixture.workspace,'live-session.json'),'utf8'));assert.equal(saved.phase,'interrupted');assert.deepEqual(saved.quarantined,[]);assert.equal(saved.attempts[0].consumed,0);
  await assert.rejects(()=>stat(join(fixture.workspace,'capture.lock')),{code:'ENOENT'});
  const result=await runLiveCapture({resume:fixture.workspace,finish:true,creatorAuthorized:true,port,keepLocal:true,pollMs:10,log:()=>{}});assert.equal(result.session.chunks.length,1);assert.equal(result.session.phase,'sealed');
});
