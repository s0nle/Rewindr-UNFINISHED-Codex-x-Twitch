import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {PGlite} from '@electric-sql/pglite';
import {applyTestMigrations} from './queue-shim.js';
import {planChunkIntervals} from '../src/ingestion-types.js';
let db:PGlite;
const owner='11111111-1111-4111-8111-111111111111',other='22222222-2222-4222-8222-222222222222';
const id='33333333-3333-4333-8333-333333333333',worker='44444444-4444-4444-8444-444444444444';
const source={id,title:'Ingestion test',indexId:'index',sourceDurationSeconds:124,sourceOffsetSeconds:7200,sourceFingerprint:'a'.repeat(64)};
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});
after(async()=>{await db?.close();});
async function setup(t:{after:(fn:()=>Promise<void>)=>void}){await db.exec('begin');t.after(async()=>{await db.exec('rollback');});await db.query('insert into rewindr_broadcasters(id,label) values($1,$3),($2,$3)',[owner,other,'test']);await create();}
async function create(who=owner,value=source){return db.query('select rewindr_create_ingestion($1,$2::jsonb)',[who,JSON.stringify(value)]);}
async function accept(ordinal=0,options:{fileId?:string;sha256?:string;startSeconds?:number}={},who=owner) {
  const plan=planChunkIntervals(source.sourceDurationSeconds)[ordinal]!;
  return(await db.query<{c:{id:string;job_id:string;file_id:string}}>('select rewindr_accept_ingest_chunk($1,$2,$3::jsonb) c',[who,id,JSON.stringify({...plan,fileId:options.fileId??randomUUID(),sha256:'b'.repeat(64),bytes:1000,...options})])).rows[0]!.c;
}
async function claim(){return(await db.query<{j:{id:string;lease_token:string}}>('select rewindr_claim_job($1,$2,60) j',['rewindr_jobs',worker])).rows[0]!.j;}
async function broadcast(){return(await db.query<{b:{chunks:{indexedAssetId:string;storageKey:string;startSeconds:number}[];ingestionId:string}}>('select rewindr_get_broadcast($1,$2) b',[owner,id])).rows[0]!.b;}
async function reject(action:()=>Promise<unknown>,code:string){await db.exec('savepoint invalid');let error:unknown;try{await action();}catch(e){error=e;}await db.exec('rollback to savepoint invalid');assert.ok(error);assert.equal((error as {code:string}).code,code);}
function checkpoint(chunkId:string,asset='indexed'){return {phase:'indexing',assetId:`asset-${asset}`,indexedAssetId:asset,storage:{bucket:'rewindr-chunks',sha256:'b'.repeat(64),bytes:1000,uploadedAt:new Date().toISOString(),
  path:`${owner}/${id}/${createHash('sha256').update(`ingest:${chunkId}`).digest('hex')}/${'b'.repeat(64)}.mp4`}};}
test('ingestion creation is idempotent and an empty broadcast is searchable only after chunks become ready',async t=>{
  await setup(t);await create();assert.equal((await broadcast()).ingestionId,id);assert.deepEqual((await broadcast()).chunks,[]);
  await reject(()=>create(other),'23514');await reject(()=>create(owner,{...source,indexId:'different'}),'23514');
  await reject(()=>db.query('select rewindr_save_broadcast($1,$2::jsonb)',[owner,JSON.stringify({...source,createdAt:new Date().toISOString(),chunks:[{filename:'a.mp4',indexedAssetId:'a',startSeconds:0,endSeconds:60}]})]),'23514');
});
test('chunk acceptance and message enqueue are atomic, deduplicated and bound to the planned source interval',async t=>{
  await setup(t);const a=await accept();assert.equal((await accept()).id,a.id);
  await reject(()=>accept(0,{sha256:'c'.repeat(64)}),'23514');await reject(()=>accept(1,{startSeconds:55}),'23514');await reject(()=>accept(1,{},other),'23503');
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  await db.exec('savepoint pending');await accept(1);assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,2);
  await db.exec('rollback to savepoint pending');assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  await reject(()=>db.query('select rewindr_seal_ingestion($1,$2)',[owner,id]),'23514');await accept(1);await db.query('select rewindr_seal_ingestion($1,$2)',[owner,id]);
});
test('ready promotion is fenced and atomic with acknowledgment; stored/pending chunks never enter search',async t=>{
  await setup(t);const c=await accept(1);const job=await claim();const cp=checkpoint(c.id);
  await db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify(cp)]);assert.deepEqual((await broadcast()).chunks,[]);
  await reject(()=>db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,worker,JSON.stringify(cp)]),'P0002');
  const ready={...cp,phase:'ready',providerStatus:'ready'};
  await reject(()=>db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify(ready)]),'23514');
  await reject(()=>db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb,true)',[job.id,job.lease_token,JSON.stringify({...ready,indexedAssetId:'changed'})]),'23514');
  await db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb,true)',[job.id,job.lease_token,JSON.stringify(ready)]);
  assert.equal((await broadcast()).chunks[0]!.storageKey,`ingest:${c.id}`);assert.equal((await broadcast()).chunks[0]!.startSeconds,60);
  assert.equal((await db.query<{state:string}>('select state from rewindr_jobs where id=$1',[job.id])).rows[0]!.state,'completed');assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,0);
});
test('Storage identities cannot be forged or reassigned to another owner, chunk or provider asset',async t=>{
  await setup(t);const c=await accept();const job=await claim();const cp=checkpoint(c.id);
  await reject(()=>db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify({...cp,storage:{}})]),'23514');
  await reject(()=>db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify({...cp,storage:{...cp.storage,path:cp.storage.path.replace(owner,other)}})]),'23514');
  await db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify(cp)]);
  await reject(()=>db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[job.id,job.lease_token,JSON.stringify({...cp,assetId:'changed'})]),'23514');
});
test('ambiguous ingestion submissions remain review-only after retry exhaustion',async t=>{
  await setup(t);const c=await accept();let j=await claim();
  await db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[j.id,j.lease_token,JSON.stringify({phase:'creating_upload'})]);
  for(let n=0;n<5;n++){await db.query("select rewindr_finish_job($1,$2,'retry',0,'temporary')",[j.id,j.lease_token]);if(n<4)j=await claim();}
  assert.equal((await db.query<{state:string}>('select state from rewindr_jobs where id=$1',[c.job_id])).rows[0]!.state,'needs_review');
  await reject(()=>db.query('select rewindr_retry_job($1,$2)',[owner,c.job_id]),'23514');
});
test('browser roles cannot inspect ingestion state or enqueue, checkpoint or seal footage',async t=>{
  await setup(t);await accept();
  for(const role of ['anon','authenticated']){await db.exec(`set local role ${role}`);
    await reject(()=>db.query('select * from rewindr_ingestions'),'42501');await reject(()=>db.query('select * from rewindr_ingest_chunks'),'42501');
    await reject(()=>db.query('select rewindr_get_ingestion($1,$2)',[owner,id]),'42501');await reject(()=>db.query('select rewindr_seal_ingestion($1,$2)',[owner,id]),'42501');await db.exec('reset role');}
});
test('explicit failed-job retry resumes a known indexed asset rather than creating another submission',async t=>{
  await setup(t);const c=await accept();const j=await claim();const cp=checkpoint(c.id);
  await db.query('select rewindr_checkpoint_ingest_chunk($1,$2,$3::jsonb)',[j.id,j.lease_token,JSON.stringify({...cp,phase:'failed',error:'provider indexing failed'})]);
  await reject(()=>db.query("select rewindr_finish_job($1,$2,'completed',0,null)",[j.id,j.lease_token]),'23514');
  await db.query("select rewindr_finish_job($1,$2,'failed',0,'provider indexing failed')",[j.id,j.lease_token]);
  await db.query('select rewindr_retry_job($1,$2)',[owner,j.id]);
  const saved=(await db.query<{checkpoint:typeof cp}>('select checkpoint from rewindr_ingest_chunks where id=$1',[c.id])).rows[0]!.checkpoint;
  assert.equal(saved.phase,'indexing');assert.equal(saved.indexedAssetId,cp.indexedAssetId);
});

test('TypeScript plans and PostgreSQL accept/seal agree at exact and adjacent microsecond tail boundaries',async t=>{
  await setup(t);
  for(const boundary of [64.1,124.1,184.1,1804.1])for(const duration of [boundary-0.000001,boundary,boundary+0.000001,Number(`${Math.floor(boundary)}.0999994`),Number(`${Math.floor(boundary)}.0999995`)]){
    const broadcastId=randomUUID(),plan=planChunkIntervals(duration),caseOwner=randomUUID();
    // Isolate independent duration cases from the per-channel daily ingestion budget.
    await db.query("insert into rewindr_broadcasters(id,label) values($1,'boundary case')",[caseOwner]);
    const input={...source,id:broadcastId,sourceDurationSeconds:duration};await create(caseOwner,input);
    for(const chunk of plan)await db.query('select rewindr_accept_ingest_chunk($1,$2,$3::jsonb)',[caseOwner,broadcastId,JSON.stringify({...chunk,fileId:randomUUID(),sha256:'b'.repeat(64),bytes:1000})]);
    await db.query('select rewindr_seal_ingestion($1,$2)',[caseOwner,broadcastId]);
    const rows=(await db.query<{start_seconds:string;end_seconds:string}>('select start_seconds,end_seconds from rewindr_ingest_chunks where ingestion_id=$1 order by ordinal',[broadcastId])).rows;
    assert.deepEqual(rows.map(r=>({startSeconds:Number(r.start_seconds),endSeconds:Number(r.end_seconds)})),plan.map(({startSeconds,endSeconds})=>({startSeconds,endSeconds})),`Duration ${duration}`);
    if(duration===64.1)assert.equal(rows.length,2);
  }
});
