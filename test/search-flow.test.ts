import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PGlite} from '@electric-sql/pglite';
import {createClient} from '@supabase/supabase-js';
import {applyTestMigrations} from './queue-shim.js';
import {createLocalPersistence} from '../src/local-persistence.js';
import {SupabaseSearches} from '../src/search-store.js';
import {SupabaseJobQueue} from '../src/jobs.js';
import {JobWorker} from '../src/job-worker.js';
import {buildApp} from '../src/app.js';
import {broadcastSchema} from '../src/metadata.js';
import type {Persistence} from '../src/persistence.js';
import type {VideoSearchProvider,Broadcast} from '../src/types.js';
import type {AnalysisPart,AnalysisProvider} from '../src/search-types.js';
import type {ChunkStorage} from '../src/chunk-storage.js';
import type {SearchView} from '../src/search-service.js';

let db:PGlite;
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});
after(async()=>{await db?.close();});
async function setup(t:{after:(fn:()=>Promise<void>)=>void}) {
  const owner=randomUUID(),root=await mkdtemp(join(tmpdir(),'rewindr-search-flow-')),bid=randomUUID();
  const source:Broadcast={id:bid,title:'Synthetic stream',indexId:'test-index',createdAt:new Date().toISOString(),sourceOffsetSeconds:7200,
    chunks:Array.from({length:6},(_,i)=>({filename:`chunk_00${i}.mp4`,indexedAssetId:`video-${i}`,assetId:`asset-${i}`,startSeconds:i*60,endSeconds:(i+1)*60}))};
  await db.query("insert into rewindr_broadcasters(id,label) values($1,'test')",[owner]);await db.query('select rewindr_save_broadcast($1,$2::jsonb)',[owner,JSON.stringify(source)]);
  const errors:unknown[]=[];let failCheckpoint=false;
  const client=createClient('https://test.supabase.co','test-key',{auth:{persistSession:false},global:{fetch:async(input,init)=>{
    const url=new URL(String(input));try{
      if(url.pathname.includes('/rpc/')){
        const rpc=url.pathname.split('/').at(-1)!;assert.match(rpc,/^rewindr_[a-z_]+$/);const args=JSON.parse(String(init?.body)) as Record<string,unknown>;const keys=Object.keys(args);
        if(rpc==='rewindr_checkpoint_analysis'&&failCheckpoint&&(args.p_checkpoint as {parts:{taskId?:string}[]}).parts.some(p=>p.taskId)){failCheckpoint=false;return Response.json({code:'53300'},{status:503});}
        keys.forEach(k=>assert.match(k,/^p_[a-z_]+$/));const result=await db.query<{v:unknown}>(`select ${rpc}(${keys.map((k,i)=>`${k}=>$${i+1}`).join(',')}) v`,keys.map(k=>args[k]!==null&&typeof args[k]==='object'?JSON.stringify(args[k]):args[k]));return Response.json(result.rows[0]!.v??null);
      }
      const table=url.pathname.split('/').at(-1)!;assert.ok(['rewindr_searches','rewindr_analyses','rewindr_jobs'].includes(table));const filters=[...url.searchParams].filter(([k])=>['id','owner_id','queue_name'].includes(k));
      const rows=(await db.query(`select * from ${table} where ${filters.map(([k],i)=>`${k}=$${i+1}`).join(' and ')}`,filters.map(([,v])=>v.replace(/^eq\./,'')))).rows;return Response.json(rows);
    }catch(e){const error=e as {code?:string};errors.push(e);return Response.json({code:error.code??'TEST_ERROR',message:'Test error'},{status:400});}
  }}});
  const local=createLocalPersistence(root);await local.init();
  const persistence:Persistence={kind:'supabase',recordings:local.recordings,init:async()=>{},health:async()=>{},broadcasts:{init:async()=>{},put:async()=>{throw Error('Unexpected write');},all:async()=>[source],
    get:async id=>{const value=(await db.query<{v:unknown}>('select rewindr_get_broadcast($1,$2) v',[owner,id])).rows[0]!.v;return value?broadcastSchema.parse(value):undefined;},attachStorage:async()=>{throw Error('Unexpected write');}}};
  const calls:string[]=[],tasks=new Map<string,AnalysisPart>();let failSearch=false,failCreate=false,invalidEvidence=false,createCount=0;
  const unexpected=async():Promise<never>=>{throw Error('Unexpected operation');};
  const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,
    async searchMany(index,ids,question){assert.equal(index,source.indexId);assert.deepEqual(ids,source.chunks.map(c=>c.indexedAssetId));calls.push(question);if(failSearch)throw Error('Temporary failure');
      return source.chunks.map((c,i)=>({videoId:c.indexedAssetId,startSeconds:10,endSeconds:14,rank:i+1}));}};
  const analysisProvider:AnalysisProvider={model:'pegasus1.5',async create(part,question){assert.ok(question);createCount++;if(failCreate)throw Error('Unknown submission outcome');const id=`task-${createCount}`;tasks.set(id,part);return id;},
    async retrieve(id){const part=tasks.get(id)!;return {status:'ready',assessment:{verdict:'partial_match',summary:'Sunglasses are visible; the dress pattern is unclear.',matchingDetails:['Sunglasses'],unclearDetails:['Dress pattern'],evidence:[{timestampSeconds:invalidEvidence?part.endSeconds+100:part.startSeconds+1,detail:'Sunglasses are visible.',modality:'visual'}]}};}};
  const storage:ChunkStorage={bucket:'test',init:async()=>{},upload:unexpected,sign:unexpected,download:unexpected};
  const queue=new SupabaseJobQueue(client,owner),searches=new SupabaseSearches(client,owner);
  const app=await buildApp({dataDir:root,persistence,provider,jobs:queue,searches,analysisConfigured:true});
  const worker=new JobWorker({queue,dataDir:root,persistence:()=>persistence,storage:()=>storage,searches:()=>searches,analysisProvider,processorRetry:{initialDelayMs:1,maxDelayMs:2,maxFailures:5}});
  async function drain(){for(let i=0;i<50;i++){await db.exec("update pgmq.q_rewindr_jobs set vt=clock_timestamp()-interval '1 second'");if(!await worker.runOnce())return;}throw Error('Worker did not drain');}
  async function start(question='Lady with black floral dress and black sunglasses walks by'){const result=await app.inject({method:'POST',url:`/broadcasts/${bid}/searches`,payload:{requestId:randomUUID(),question}});assert.equal(result.statusCode,201,result.body);return result.json<SearchView>();}
  t.after(async()=>{await app.close();await db.exec('delete from pgmq.q_rewindr_jobs;delete from pgmq.a_rewindr_jobs');await db.query('delete from rewindr_broadcasters where id=$1',[owner]);if(!root.startsWith(join(tmpdir(),'rewindr-search-flow-')))throw Error('Bad cleanup');await rm(root,{recursive:true,force:true});});
  return {source,owner,app,searches,queue,worker,calls,tasks,errors,drain,start,get createCount(){return createCount;},failSearch:()=>{failSearch=true;},failCreate:()=>{failCreate=true;},invalidEvidence:()=>{invalidEvidence=true;},validEvidence:()=>{invalidEvidence=false;},failCheckpoint:()=>{failCheckpoint=true;}};
}
test('exact first query -> three immediate cards -> cached async evidence; opt-in recovery excludes only this session',async t=>{
  const s=await setup(t),first=await s.start();assert.equal(first.candidates.length,3);assert.deepEqual(s.calls,[first.question]);assert.equal(s.createCount,0);
  const details=await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});assert.equal(details.statusCode,200,details.body);s.failCheckpoint();await s.drain();
  const ready=(await s.app.inject(`/searches/${first.searchId}`)).json<SearchView>();assert.ok(ready.candidates.every(c=>c.analysis.status==='ready'));assert.equal(s.createCount,3);
  assert.equal(ready.candidates[1]!.analysis.assessment!.evidence[0]!.timestampSeconds,66);assert.equal(ready.candidates[0]!.broadcastMatchStartSeconds,7210);
  const again=await s.start();await s.app.inject({method:'POST',url:again.analysisEndpoint,payload:{version:0}});await s.drain();assert.equal(s.createCount,3,'Cache should prevent duplicate paid work');
  const requestId=randomUUID(),url=`/searches/${first.searchId}/recover`;const response=await s.app.inject({method:'POST',url,payload:{version:0,requestId}});assert.equal(response.statusCode,200,response.body);const recovered=response.json<SearchView>();
  assert.equal(recovered.question,first.question);assert.equal(recovered.queriesUsed.length,3);assert.equal(recovered.rejectedIntervalCount,3);assert.equal(recovered.candidates.length,3);assert.ok(recovered.candidates.every(c=>c.matchStartSeconds>=180));
  assert.equal(s.calls.length,4);assert.equal((await s.app.inject({method:'POST',url,payload:{version:0,requestId}})).statusCode,200);assert.equal(s.calls.length,4);
  assert.equal((await s.app.inject({method:'POST',url,payload:{version:0,requestId:randomUUID()}})).statusCode,409);
  const independent=await s.start();assert.equal(independent.candidates[0]!.matchStartSeconds,10);assert.deepEqual((await s.searches.get(again.searchId))!.state.rejected,[]);
  assert.equal((await s.app.inject({method:'POST',url:`/searches/${first.searchId}/analyses`,payload:{version:0}})).statusCode,409);
});
test('rejected, unsubmitted analysis is skipped; ambiguous submissions never automatically incur another create',async t=>{
  const s=await setup(t),first=await s.start();await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});
  await s.app.inject({method:'POST',url:`/searches/${first.searchId}/close`,payload:{}});await s.drain();assert.equal(s.createCount,0);
  const next=await s.start();s.failCreate();await s.app.inject({method:'POST',url:next.analysisEndpoint,payload:{version:0}});await s.drain();assert.equal(s.createCount,3);
  const view=(await s.app.inject(`/searches/${next.searchId}`)).json<SearchView>();assert.ok(view.candidates.every(c=>c.analysis.status==='needs_review'));assert.equal(view.canRecover,true);
  await s.app.inject({method:'POST',url:next.analysisEndpoint,payload:{version:0}});await s.drain();assert.equal(s.createCount,3);
});
test('bad evidence fails assessments without breaking search, and unavailable variants retain rejections for retry',async t=>{
  const s=await setup(t),first=await s.start();s.invalidEvidence();await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});await s.drain();
  const view=(await s.app.inject(`/searches/${first.searchId}`)).json<SearchView>();assert.ok(view.candidates.every(c=>c.analysis.status==='failed'));assert.equal(view.canRecover,true);
  // Remove remaining raw candidates to exercise complete provider failure rather than pool fallback.
  await db.query("update rewindr_searches set state=jsonb_set(state,'{pool}',(select jsonb_agg(p) from jsonb_array_elements(state->'pool') p where (p->>'rank')::int<=3)) where id=$1",[first.searchId]);
  s.failSearch();const response=await s.app.inject({method:'POST',url:`/searches/${first.searchId}/recover`,payload:{version:0,requestId:randomUUID()}});assert.equal(response.statusCode,502,response.body);
  const after=(await s.app.inject(`/searches/${first.searchId}`)).json<SearchView>();assert.equal(after.rejectedIntervalCount,3);assert.equal(after.candidates.length,0);assert.equal(after.canRecover,true);assert.equal(after.question,first.question);
});

test('an explicit assessment re-request replaces confirmed invalid tasks once and produces fresh cached details',async t=>{
  const s=await setup(t),first=await s.start();s.invalidEvidence();await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});await s.drain();assert.equal(s.createCount,3);
  s.validEvidence();for(let i=0;i<2;i++)assert.equal((await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}})).statusCode,200);await s.drain();
  const view=(await s.app.inject(`/searches/${first.searchId}`)).json<SearchView>();assert.ok(view.candidates.every(c=>c.analysis.status==='ready'));assert.equal(s.createCount,6,'Duplicate requests must not create additional paid work');
});
test('daily budgets stop initial retrieval before provider calls and leave analysis cards explicitly unavailable until retry',async t=>{
  const s=await setup(t);await db.query("select rewindr_reserve_usage($1,'search','fixture-cap',100)",[s.owner]);
  const denied=await s.app.inject({method:'POST',url:`/broadcasts/${s.source.id}/searches`,payload:{requestId:randomUUID(),question:'A person walks past'}});assert.equal(denied.statusCode,429,denied.body);assert.equal(s.calls.length,0);
  await db.query("delete from rewindr_usage where owner_id=$1 and metric='search'",[s.owner]);const first=await s.start();await db.query("select rewindr_reserve_usage($1,'analysis','fixture-cap',60)",[s.owner]);
  const limited=await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});assert.equal(limited.statusCode,200,limited.body);assert.ok(limited.json<SearchView>().candidates.every(c=>c.analysis.status==='unavailable'));await s.drain();assert.equal(s.createCount,0);
  await db.query("delete from rewindr_usage where owner_id=$1 and metric='analysis'",[s.owner]);await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});await s.drain();assert.equal(s.createCount,3);
});
test('operational retention removes expired search snapshots and unreferenced old results while preserving active cache associations',async t=>{
  const s=await setup(t),first=await s.start();await s.app.inject({method:'POST',url:first.analysisEndpoint,payload:{version:0}});await s.drain();
  await db.query("update rewindr_searches set expires_at=clock_timestamp()-interval '2 days' where id=$1",[first.searchId]);await db.query("update rewindr_analyses set created_at=clock_timestamp()-interval '8 days' where owner_id=$1",[s.owner]);
  const second=await s.start();await s.app.inject({method:'POST',url:second.analysisEndpoint,payload:{version:0}});await db.query('select rewindr_prune_operational($1)',[s.owner]);assert.equal(await s.searches.get(first.searchId),undefined);assert.ok(await s.searches.get(second.searchId));assert.equal((await db.query('select id from rewindr_analyses where owner_id=$1',[s.owner])).rows.length,3);
  await db.query("update rewindr_searches set expires_at=clock_timestamp()-interval '2 days' where id=$1",[second.searchId]);await db.query('select rewindr_prune_operational($1)',[s.owner]);assert.equal((await db.query('select id from rewindr_analyses where owner_id=$1',[s.owner])).rows.length,0);
});
