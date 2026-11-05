import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PGlite} from '@electric-sql/pglite';
import {applyTestMigrations} from './queue-shim.js';
import {selectCandidates,planAnalysis,analysisCacheKey} from '../src/search-ranking.js';
import type {Broadcast} from '../src/types.js';
import type {SearchState} from '../src/search-types.js';
let db:PGlite;
const owner='11111111-1111-4111-8111-111111111111',other='22222222-2222-4222-8222-222222222222',bid='33333333-3333-4333-8333-333333333333';
const source:Broadcast={id:bid,title:'Test',indexId:'index',createdAt:new Date().toISOString(),sourceOffsetSeconds:0,chunks:[{filename:'chunk.mp4',indexedAssetId:'video',assetId:'asset',startSeconds:0,endSeconds:60}]};
const settings={modalities:['visual'],operator:'or',transcriptionOptions:['semantic'],candidateLimit:30};
const hit={videoId:'video',startSeconds:10,endSeconds:15,rank:1,query:'original'};
const state:SearchState={pool:[hit],current:selectCandidates(source,[hit],[]),usedQueries:['original'],rejected:[],round:0,warnings:[],stopReason:'none'};
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});
after(async()=>{await db?.close();});
async function setup(t:{after:(fn:()=>Promise<void>)=>void}){await db.exec('begin');t.after(async()=>{await db.exec('rollback');});await db.query('insert into rewindr_broadcasters(id,label) values($1,$3),($2,$3)',[owner,other,'test']);await db.query('select rewindr_save_broadcast($1,$2::jsonb)',[owner,JSON.stringify(source)]);return create();}
async function create(id=randomUUID()){await db.query('select rewindr_create_search($1,$2,$3::jsonb,$4,$5::jsonb,$6::jsonb)',[owner,id,JSON.stringify(source),'original',JSON.stringify(settings),JSON.stringify(state)]);return id;}
async function reject(action:()=>Promise<unknown>,code:string){await db.exec('savepoint invalid');let e:unknown;try{await action();}catch(error){e=error;}await db.exec('rollback to savepoint invalid');assert.ok(e);assert.equal((e as {code:string}).code,code);}
async function reserve(id:string,requestId=randomUUID(),who=owner,version=0){return(await db.query<{r:{owned:boolean;replayed:boolean;search:{state:SearchState;version:number}}}>('select rewindr_reserve_recovery($1,$2,$3,$4) r',[who,id,version,requestId])).rows[0]!.r;}
async function analysis(id:string){const candidate=state.current[0]!,parts=planAnalysis(source,candidate),key=analysisCacheKey(source,'original',candidate,parts,'pegasus1.5');
  return(await db.query<{a:{id:string;job_id:string;plan:typeof parts;checkpoint:unknown}}>('select rewindr_request_analysis($1,$2,0,$3,$4,$5,$6,$7::jsonb,true) a',[owner,id,candidate.candidateId,key,'pegasus1.5','evidence-v1',JSON.stringify(parts)])).rows[0]!.a;}
async function claim(){return(await db.query<{j:{id:string;lease_token:string}}>('select rewindr_claim_job($1,$2,60) j',['rewindr_jobs',randomUUID()])).rows[0]!.j;}
test('recovery persists only this session’s interval rejections and serializes duplicate/concurrent requests',async t=>{
  const id=await setup(t),otherId=await create(),requestId=randomUUID();const first=await reserve(id,requestId);assert.equal(first.owned,true);assert.equal(first.search.state.rejected.length,1);assert.deepEqual(first.search.state.current,[]);
  assert.equal((await reserve(id,requestId)).owned,false);await reject(()=>reserve(id,randomUUID()),'23514');await reject(()=>reserve(id,randomUUID(),other),'23503');
  const untouched=(await db.query<{state:SearchState}>('select state from rewindr_searches where id=$1',[otherId])).rows[0]!.state;assert.deepEqual(untouched.rejected,[]);
  await db.query('select rewindr_commit_recovery($1,$2,$3,$4::jsonb)',[owner,id,requestId,JSON.stringify({...first.search.state,round:1})]);
  assert.equal((await reserve(id,requestId)).replayed,true);await reject(()=>reserve(id,randomUUID()),'23514');
});
test('failed recovery keeps rejections; expired leases may be retried without removing them',async t=>{
  const id=await setup(t),requestId=randomUUID();await reserve(id,requestId);await db.query('select rewindr_fail_recovery($1,$2,$3)',[owner,id,requestId]);
  const again=await reserve(id);assert.equal(again.search.state.rejected.length,1);assert.deepEqual(again.search.state.current,[]);
  await db.query("update rewindr_searches set pending_until=clock_timestamp()-interval '1 second' where id=$1",[id]);assert.equal((await reserve(id)).owned,true);
});
test('closing and expiry prevent session reuse without mutating broadcast/index mappings',async t=>{
  const id=await setup(t);await db.query('select rewindr_close_search($1,$2)',[owner,id]);await reject(()=>reserve(id),'P0003');
  assert.equal((await db.query('select * from rewindr_chunks where broadcast_id=$1',[bid])).rows.length,1);
});
test('analyses cache across matching sessions and complete atomically with fenced worker acknowledgment',async t=>{
  const id=await setup(t),otherId=await create(),a=await analysis(id),b=await analysis(otherId);assert.equal(a.id,b.id);assert.equal(a.job_id,b.job_id);
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);const job=await claim();
  const cp={parts:[{phase:'processing',taskId:'known-task'}]};await db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify(cp)]);
  await reject(()=>db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify({parts:[{phase:'processing',taskId:'different'}]})]),'23514');
  await reject(()=>db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[job.id,randomUUID(),JSON.stringify(cp)]),'P0002');
  const result={verdict:'unclear',summary:'Clothing is unclear.',matchingDetails:[],unclearDetails:['Pattern'],evidence:[]};
  await reject(()=>db.query("select rewindr_checkpoint_analysis($1,$2,$3::jsonb,'ready',$4::jsonb)",[job.id,job.lease_token,JSON.stringify(cp),JSON.stringify(result)]),'23514');
  await db.query("select rewindr_checkpoint_analysis($1,$2,$3::jsonb,'ready',$4::jsonb)",[job.id,job.lease_token,JSON.stringify({parts:[{...cp.parts[0],phase:'ready',assessment:result}]}),JSON.stringify(result)]);
  assert.equal((await db.query<{status:string}>('select status from rewindr_analyses where id=$1',[a.id])).rows[0]!.status,'ready');assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,0);
});
test('rejected analysis can be skipped and reactivated; unknown task submissions remain review-only',async t=>{
  const id=await setup(t),a=await analysis(id),job=await claim();await reserve(id);assert.equal((await db.query<{v:boolean}>('select rewindr_analysis_active($1,$2) v',[owner,a.id])).rows[0]!.v,false);
  await db.query("select rewindr_checkpoint_analysis($1,$2,$3::jsonb,'skipped')",[job.id,job.lease_token,JSON.stringify({parts:[{phase:'queued'}]})]);
  const next=await create();assert.equal((await analysis(next)).job_id,a.job_id);const resumed=await claim();
  await db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[resumed.id,resumed.lease_token,JSON.stringify({parts:[{phase:'submitting'}]})]);
  for(let n=0;n<5;n++){const running=n===0?resumed:await claim();await db.query("select rewindr_finish_job($1,$2,'retry',0,'temporary')",[running.id,running.lease_token]);}
  assert.equal((await db.query<{status:string}>('select status from rewindr_analyses where id=$1',[a.id])).rows[0]!.status,'needs_review');await reject(()=>db.query('select rewindr_retry_job($1,$2)',[owner,a.job_id]),'23514');
});
test('browser roles cannot read private prompts/results or invoke search/analysis mutations',async t=>{
  const id=await setup(t);await analysis(id);
  for(const role of ['anon','authenticated']){await db.exec(`set local role ${role}`);for(const table of ['rewindr_searches','rewindr_analyses','rewindr_search_analyses'])await reject(()=>db.query(`select * from ${table}`),'42501');
    await reject(()=>reserve(id),'42501');await reject(()=>db.query('select rewindr_analysis_active($1,$2)',[owner,randomUUID()]),'42501');await db.exec('reset role');}
});

test('enabling analysis after configuration becomes available queues an unavailable cache entry',async t=>{
  const id=await setup(t),candidate=state.current[0]!,parts=planAnalysis(source,candidate),key=analysisCacheKey(source,'original',candidate,parts,'pegasus1.5');
  const request=(available:boolean)=>db.query<{a:{status:string;job_id:string|null}}>('select rewindr_request_analysis($1,$2,0,$3,$4,$5,$6,$7::jsonb,$8) a',[owner,id,candidate.candidateId,key,'pegasus1.5','evidence-v1',JSON.stringify(parts),available]);
  assert.equal((await request(false)).rows[0]!.a.status,'unavailable');const enabled=(await request(true)).rows[0]!.a;assert.equal(enabled.status,'queued');assert.ok(enabled.job_id);assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
});

test('failed analysis re-request resumes a known task once, renews its deadline and fences the old lease',async t=>{
  const id=await setup(t),a=await analysis(id),job=await claim(),cp={parts:[{phase:'processing',taskId:'known-task'}]};
  await db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify(cp)]);
  await db.query("select rewindr_finish_job($1,$2,'failed',0,'temporary polling failure')",[job.id,job.lease_token]);
  await db.query("update rewindr_analyses set attempt_started_at=clock_timestamp()-interval '2 hours' where id=$1",[a.id]);
  const requested=await analysis(id);assert.equal(requested.job_id,a.job_id);assert.deepEqual(requested.checkpoint,cp);await analysis(id);
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  const row=(await db.query<{attempt_started_at:Date;attempt_history:unknown[]}>('select attempt_started_at,attempt_history from rewindr_analyses where id=$1',[a.id])).rows[0]!;
  assert.ok(Date.now()-new Date(row.attempt_started_at).getTime()<5000);assert.equal(row.attempt_history.length,1);
  await reject(()=>db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify(cp)]),'P0002');
});
test('confirmed failed/invalid tasks get fresh submission identities on re-request while unknown tasks remain review-only',async t=>{
  const id=await setup(t),a=await analysis(id),job=await claim();
  const cp={parts:[{phase:'processing',taskId:'terminal-task',failureKind:'provider_terminal'}]};
  await db.query("select rewindr_checkpoint_analysis($1,$2,$3::jsonb,'failed',null,'provider failed')",[job.id,job.lease_token,JSON.stringify(cp)]);
  const retried=await analysis(id);assert.deepEqual(retried.checkpoint,{parts:[{phase:'queued'}]});assert.notEqual(retried.plan[0]!.id,a.plan[0]!.id);
  const history=(await db.query<{attempt_history:{checkpoint:typeof cp}[]}>('select attempt_history from rewindr_analyses where id=$1',[a.id])).rows[0]!.attempt_history;assert.equal(history[0]!.checkpoint.parts[0]!.taskId,'terminal-task');
  const next=await claim();await db.query('select rewindr_checkpoint_analysis($1,$2,$3::jsonb)',[next.id,next.lease_token,JSON.stringify({parts:[{phase:'submitting'}]})]);
  await db.query("select rewindr_finish_job($1,$2,'failed',0,'unknown creation')",[next.id,next.lease_token]);
  const review=await analysis(id);assert.equal((review as unknown as {status:string}).status,'needs_review');assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,0);
});
