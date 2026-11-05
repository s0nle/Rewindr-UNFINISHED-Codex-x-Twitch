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
import {fileIdentity} from '../src/segmenter.js';
import {SupabaseJobQueue} from '../src/jobs.js';
import {SupabaseSearches} from '../src/search-store.js';
import {JobWorker} from '../src/job-worker.js';
import {buildApp} from '../src/app.js';
import type {AnalysisPart,AnalysisProvider} from '../src/search-types.js';
import type {VideoSearchProvider,Broadcast} from '../src/types.js';
import type {SearchView} from '../src/search-service.js';
let step='configuration';
async function run(){
  const preview=process.argv.includes('--preview');
  if(process.argv.slice(2).some(arg=>arg!=='--preview'))throw Error('Use search:verify [--preview].');
  if(preview&&!process.stdin.isTTY)throw Error('Synthetic preview requires an interactive terminal; use search:verify without --preview in automated runs.');
  const db=await databaseClient(),owner=randomUUID(),queueName=`rewindr_verify_${randomUUID().replaceAll('-','')}`;
  const root=await mkdtemp(join(tmpdir(),'rewindr-search-verification-')),env={...process.env,REWINDR_OWNER_ID:owner};
  const client=configuredSupabaseClient(env),storage=configuredChunkStorage(env),queue=new SupabaseJobQueue(client,owner,queueName),searches=new SupabaseSearches(client,owner,queueName);
  const persistence=createSupabasePersistence(root,env),objects=new Set<string>(),tasks=new Map<string,AnalysisPart>();
  let queueCreated=false,ownerCreated=false,app:Awaited<ReturnType<typeof buildApp>>|undefined,pumping=false,pump:Promise<void>|undefined,pumpFailure:unknown,creates=0;
  const queries:string[]=[];
  try{
    step='isolated owner and queue';await db.connect();await db.query('select pgmq.create($1)',[queueName]);queueCreated=true;
    await db.query("insert into rewindr_broadcasters(id,label) values($1,'Rewindr temporary search verification')",[owner]);ownerCreated=true;
    step='synthetic media and private Storage';const file=join(root,'synthetic.mp4');
    await promisify(execFile)(process.env.FFMPEG_PATH||ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=s=360x360:r=5','-t','60','-an','-c:v','libx264','-crf','32','-pix_fmt','yuv420p',file],{windowsHide:true,timeout:30000});
    const identity=await fileIdentity(file);const source:Broadcast={id:randomUUID(),title:'Synthetic verification — provider results are mocked',indexId:'verification-index',createdAt:new Date().toISOString(),sourceOffsetSeconds:7200,
      chunks:Array.from({length:6},(_,i)=>({filename:`chunk_00${i}.mp4`,indexedAssetId:`verification-video-${i}`,startSeconds:i*60,endSeconds:(i+1)*60}))};
    await persistence.broadcasts.put(source);
    for(const chunk of source.chunks){objects.add(chunkObjectPath(owner,source.id,chunk.indexedAssetId,identity.sha256));const stored=await storage.upload(source.id,chunk.indexedAssetId,file,identity.sha256,identity.bytes);await persistence.broadcasts.attachStorage!(source.id,chunk.indexedAssetId,stored.reference);}
    const unexpected=async():Promise<never>=>{throw Error('Unexpected provider request');};
    // Deliberately no TwelveLabs SDK or API calls: only Supabase/Storage are live.
    const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,
      async searchMany(index,ids,question){assert.equal(index,source.indexId);assert.deepEqual(ids,source.chunks.map(c=>c.indexedAssetId));queries.push(question);return ids.map((id,i)=>({videoId:id,startSeconds:10,endSeconds:14,rank:i+1}));}};
    const analysisProvider:AnalysisProvider={model:'pegasus1.5',async create(part,_question,url){assert.ok(url,'Stored-source URL fallback was not used');const response=await fetch(url,{headers:{range:'bytes=0-31'},signal:AbortSignal.timeout(20000)});assert.equal(response.status,206);await response.arrayBuffer();const id=`mock-task-${++creates}`;tasks.set(id,part);return id;},
      async retrieve(id){const part=tasks.get(id)!;return {status:'ready',assessment:{verdict:'unclear',summary:'Synthetic test footage cannot establish the remembered event.',matchingDetails:[],unclearDetails:['This is a synthetic verification clip.'],evidence:[{timestampSeconds:part.startSeconds+1,detail:'Synthetic test pattern is visible.',modality:'visual'}]}};}};
    const makeApp=()=>buildApp({dataDir:root,persistence:createSupabasePersistence(root,env),storage,jobs:queue,searches,provider,analysisConfigured:true});
    app=await makeApp();const worker=new JobWorker({queue,dataDir:root,persistence:()=>persistence,storage:()=>storage,searches:()=>searches,analysisProvider});
    async function drain(){for(let i=0;i<90;i++){
      const pending=await db.query<{count:string}>("select count(*) from rewindr_jobs where queue_name=$1 and state in ('queued','retrying','running')",[queueName]);if(Number(pending.rows[0]!.count)===0)return;
      await db.query('select pgmq.set_vt($1,msg_id,0) from pgmq.read($1,0,100)',[queueName]);await worker.runOnce();await wait(50);
    }throw Error('Verification queue did not drain');}
    step='exact query and async assessments';const question='Lady with black floral dress and black sunglasses walks by';
    const start=async()=>{const response=await app!.inject({method:'POST',url:`/broadcasts/${source.id}/searches`,payload:{requestId:randomUUID(),question}});assert.equal(response.statusCode,201,response.body);return response.json<SearchView>();};
    const first=await start();assert.equal(first.candidates.length,3,'Expected three first-search candidates');assert.deepEqual(queries,[question]);assert.equal(creates,0);
    step='queueing candidate assessments';const queued=await app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});assert.equal(queued.statusCode,200,'Assessment enqueue HTTP status');await drain();
    step='assessment completion';const ready=(await app.inject(`/searches/${first.searchId}`)).json<SearchView>();
    if(!ready.candidates.every(c=>c.analysis.status==='ready'))console.log('Verification assessment states:',ready.candidates.map(c=>({status:c.analysis.status,error:c.analysis.error})),{creates});
    assert.ok(ready.candidates.every(c=>c.analysis.status==='ready'),'Expected completed assessment states');assert.equal(creates,3,'Expected one task per candidate');
    step='signed playback and evidence mapping';
    const manifest=await app.inject(ready.candidates[1]!.manifestUrl!);assert.equal(manifest.statusCode,200,manifest.body);assert.equal(manifest.json().segments.length,2);
    assert.equal(ready.candidates[1]!.analysis.assessment!.evidence[0]!.timestampSeconds,66);
    step='cache and opt-in session exclusions';const cached=await start();await app.inject({method:'POST',url:cached.analysisEndpoint,payload:{version:0}});await drain();assert.equal(creates,3);
    const requestId=randomUUID(),url=`/searches/${first.searchId}/recover`;const recovered=await app.inject({method:'POST',url,payload:{version:0,requestId}});assert.equal(recovered.statusCode,200,recovered.body);
    const next=recovered.json<SearchView>();assert.equal(next.question,question);assert.equal(next.rejectedIntervalCount,3);assert.ok(next.candidates.every(c=>c.matchStartSeconds>=180));assert.equal(next.queriesUsed.length,3);
    const count=queries.length;assert.equal((await app.inject({method:'POST',url,payload:{version:0,requestId}})).statusCode,200);assert.equal(queries.length,count);
    assert.equal((await app.inject({method:'POST',url,payload:{version:0,requestId:randomUUID()}})).statusCode,409);
    assert.equal((await start()).candidates[0]!.matchStartSeconds,10);assert.equal(await new SupabaseSearches(client,randomUUID(),queueName).get(first.searchId),undefined);
    step='API restart';await app.close();app=await makeApp();assert.equal((await app.inject(`/searches/${first.searchId}`)).json<SearchView>().version,1);
    console.log('Verified hosted PostgreSQL/pgmq, private signed playback, exact original query, async assessments, stored-source URL fallback, evidence timestamps, cache reuse, session-only exclusions, idempotent recovery, owner scoping and API restart. Marengo/Pegasus were mocked; no paid video-provider calls were made.');
    if(preview){step='interactive synthetic preview';const address=await app.listen({host:'127.0.0.1',port:0});console.log(`Synthetic UI preview: ${address}. Press Enter to finish and clean up.`);
      pumping=true;pump=(async()=>{try{while(pumping){await worker.runOnce();await wait(200);}}catch(error){pumpFailure=error;}})();
      await new Promise<void>(resolve=>{const done=()=>{clearTimeout(timer);process.stdin.removeListener('data',done);process.stdin.pause();resolve();};const timer=setTimeout(done,15*60*1000);process.stdin.once('data',done);process.stdin.resume();});pumping=false;await pump;pump=undefined;assert.equal(pumpFailure,undefined);
    }
  }finally{
    pumping=false;await pump;await app?.close();let cleanupError:unknown;
    try{
      if([...objects].some(path=>!path.startsWith(`${owner}/`)))throw Error('Unexpected cleanup scope');
      if(objects.size){const result=await client.storage.from(storage.bucket).remove([...objects]);if(result.error)throw Error('Temporary object cleanup failed');}
      if(ownerCreated)await db.query("delete from rewindr_broadcasters where id=$1 and label='Rewindr temporary search verification'",[owner]);
      if(queueCreated){await db.query('delete from rewindr_workers where queue_name=$1',[queueName]);await db.query('select pgmq.drop_queue($1)',[queueName]);}
    }catch(error){cleanupError=error;console.error(`Temporary verification cleanup needs attention: owner ${owner}, queue ${queueName}.`);}
    await db.end();if(!root.startsWith(join(tmpdir(),'rewindr-search-verification-')))throw Error('Unexpected cleanup path');await rm(root,{recursive:true,force:true});if(cleanupError)throw cleanupError;
  }
  console.log('Temporary search verification data, queue and media removed. Existing archive retained.');
}
run().catch(error=>{console.error(`Search verification stopped during ${step}. ${safeDatabaseError(error)}`);if((error as {code?:string})?.code==='ERR_ASSERTION')console.error('Synthetic assertion:',(error as Error).message.split('\n')[0]);process.exitCode=1;});
