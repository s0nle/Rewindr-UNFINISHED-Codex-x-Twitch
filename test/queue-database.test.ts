import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {PGlite} from '@electric-sql/pglite';
import {applyTestMigrations} from './queue-shim.js';

let db:PGlite;
const owner='11111111-1111-4111-8111-111111111111',other='22222222-2222-4222-8222-222222222222';
const id='33333333-3333-4333-8333-333333333333',worker='44444444-4444-4444-8444-444444444444';
const fileId='55555555-5555-4555-8555-555555555555';
const input={indexedAssetId:'asset',filename:'chunk_000.mp4',fileId,sha256:'a'.repeat(64),bytes:1000};
type Row={id:string;lease_token:string;owner_id:string;state:string;attempts:number;failures:number;message_id:number};
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});
after(async()=>{await db?.close();});
async function setup(t:{after:(fn:()=>Promise<void>)=>void}) {
  await db.exec('begin');t.after(async()=>{await db.exec('rollback');});
  await db.query('insert into rewindr_broadcasters(id,label) values($1,$3),($2,$3)',[owner,other,'test']);
  await db.query('select rewindr_save_broadcast($1,$2::jsonb)',[owner,JSON.stringify({id,title:'Test',indexId:'index',sourceOffsetSeconds:0,createdAt:new Date().toISOString(),chunks:[{filename:'chunk_000.mp4',indexedAssetId:'asset',startSeconds:0,endSeconds:4}]})]);
}
async function enqueue(who=owner,value=input) {return(await db.query<{j:Row}>('select rewindr_enqueue_job($1,$2,$3,$4::jsonb) j',[who,'chunk_upload',id,JSON.stringify(value)])).rows[0]!.j;}
async function claim() {return(await db.query<{j:Row|null}>('select rewindr_claim_job($1,$2,60) j',['rewindr_jobs',worker])).rows[0]!.j;}
async function rejectCode(operation:()=>Promise<unknown>,code:string) {
  await db.exec('savepoint bad');let error:unknown;try{await operation();}catch(e){error=e;}await db.exec('rollback to savepoint bad');
  assert.ok(error);assert.equal((error as {code:string}).code,code);
}
test('enqueue deduplicates uploads, rejects identity conflicts, and never creates another owner’s job',async t=>{
  await setup(t);const a=await enqueue();const b=await enqueue(owner,{...input,fileId:worker});assert.equal(a.id,b.id);
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  await rejectCode(()=>enqueue(owner,{...input,sha256:'b'.repeat(64)}),'23514');await rejectCode(()=>enqueue(other),'23503');
  await db.exec('savepoint uncommitted');await db.query('delete from rewindr_jobs where id=$1',[a.id]);
  await db.exec('rollback to savepoint uncommitted');assert.equal((await claim())!.id,a.id);assert.equal(await claim(),null);
});
test('expired messages redeliver with a new token and stale checkpoints/completions are fenced',async t=>{
  await setup(t);await enqueue();const a=(await claim())!;
  await db.exec("update rewindr_jobs set lease_until=clock_timestamp()-interval '1 second';update pgmq.q_rewindr_jobs set vt=clock_timestamp()-interval '1 second'");
  const b=(await claim())!;assert.equal(b.id,a.id);assert.notEqual(b.lease_token,a.lease_token);assert.equal(b.attempts,2);
  await rejectCode(()=>db.query('select rewindr_renew_job($1,$2,60)',[a.id,a.lease_token]),'P0002');
  await rejectCode(()=>db.query("select rewindr_finish_job($1,$2,'failed',0,'stale')",[a.id,a.lease_token]),'P0002');
  await db.query('select rewindr_renew_job($1,$2,60)',[b.id,b.lease_token]);assert.equal(await claim(),null);
});
test('chunk attachment and queue acknowledgment commit together and survive redelivery',async t=>{
  await setup(t);await enqueue();const j=(await claim())!;
  const ref={bucket:'rewindr-chunks',path:`${owner}/${id}/${createHash('sha256').update('asset').digest('hex')}/${input.sha256}.mp4`,sha256:input.sha256,bytes:1000,uploadedAt:new Date().toISOString()};
  await rejectCode(()=>db.query('select rewindr_complete_chunk_job($1,$2,$3::jsonb)',[j.id,j.lease_token,JSON.stringify({...ref,bytes:1001})]),'23514');
  assert.equal((await db.query<{state:string}>('select state from rewindr_jobs')).rows[0]!.state,'running');
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  await db.query('select rewindr_complete_chunk_job($1,$2,$3::jsonb)',[j.id,j.lease_token,JSON.stringify(ref)]);
  assert.equal((await db.query<{storage_path:string}>('select storage_path from rewindr_chunks')).rows[0]!.storage_path,ref.path);
  assert.equal((await db.query<{state:string}>('select state from rewindr_jobs')).rows[0]!.state,'completed');
  assert.equal(await claim(),null);assert.equal((await enqueue()).state,'completed');
});
test('transient retries are bounded, failed jobs are visible, and explicit retry preserves job identity',async t=>{
  await setup(t);const initial=await enqueue();
  for(let n=0;n<5;n++){const j=(await claim())!;assert.ok(j);await db.query("select rewindr_finish_job($1,$2,'retry',0,'temporary')",[j.id,j.lease_token]);}
  assert.equal(await claim(),null);assert.equal((await db.query<{state:string}>('select state,failures from rewindr_jobs')).rows[0]!.state,'failed');
  await rejectCode(()=>db.query('select rewindr_retry_job($1,$2)',[other,initial.id]),'P0002');
  const retry=(await db.query<{j:Row}>('select rewindr_retry_job($1,$2) j',[owner,initial.id])).rows[0]!.j;
  assert.equal(retry.id,initial.id);assert.equal(retry.failures,0);assert.equal((await claim())!.id,initial.id);
});
test('recording inserts atomically enqueue, checkpoints use the lease, and needs_review cannot blindly retry',async t=>{
  await setup(t);
  const recordId='66666666-6666-4666-8666-666666666666';
  await db.query("insert into rewindr_recordings(id,owner_id,filename,bytes,sha256,creator_authorized,authorized_at,created_at,updated_at,phase) values($1,$2,'source.mp4',100,$3,true,now(),now(),now(),'queued')",[recordId,owner,'b'.repeat(64)]);
  const j=(await claim())!;assert.equal(j.id,recordId);
  const record={id:recordId,filename:'source.mp4',bytes:100,sha256:'b'.repeat(64),phase:'creating_upload',indexId:'index'};
  await db.query('select rewindr_checkpoint_recording($1,$2,$3::jsonb)',[j.id,j.lease_token,JSON.stringify(record)]);
  assert.equal((await db.query<{phase:string}>('select phase from rewindr_recordings')).rows[0]!.phase,'creating_upload');
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  await rejectCode(()=>db.query('select rewindr_checkpoint_recording($1,$2,$3::jsonb)',[j.id,worker,JSON.stringify(record)]),'P0002');
  await db.query("select rewindr_finish_job($1,$2,'needs_review',0,'Check provider')",[j.id,j.lease_token]);
  await rejectCode(()=>db.query('select rewindr_retry_job($1,$2)',[owner,j.id]),'23514');
});
test('browser roles cannot read jobs, consume messages or invoke privileged queue functions',async t=>{
  await setup(t);await enqueue();
  for(const role of ['anon','authenticated']) {
    await db.exec(`set local role ${role}`);
    await rejectCode(()=>db.query('select * from rewindr_jobs'),'42501');
    await rejectCode(()=>db.query('select * from pgmq.q_rewindr_jobs'),'42501');
    await rejectCode(()=>claim(),'42501');
    await rejectCode(()=>db.query('select rewindr_queue_status($1)',[owner]),'42501');
    await db.exec('reset role');
  }
});
test('rolling back a recording insert also rolls back its job and pgmq message',async t=>{
  await setup(t);await db.exec('savepoint before_recording');
  await db.query("insert into rewindr_recordings(id,owner_id,filename,bytes,sha256,creator_authorized,authorized_at,created_at,updated_at,phase) values($1,$2,'source.mp4',100,$3,true,now(),now(),now(),'queued')",[fileId,owner,'b'.repeat(64)]);
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,1);
  await db.exec('rollback to savepoint before_recording');assert.equal((await db.query('select * from rewindr_jobs')).rows.length,0);assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,0);
});
test('retry exhaustion preserves ambiguous provider state for review and updates recording failure status',async t=>{
  await setup(t);
  await db.query("insert into rewindr_recordings(id,owner_id,filename,bytes,sha256,creator_authorized,authorized_at,created_at,updated_at,phase) values($1,$2,'source.mp4',100,$3,true,now(),now(),now(),'creating_upload')",[fileId,owner,'b'.repeat(64)]);
  for(let i=0;i<5;i++){const j=(await claim())!;await db.query("select rewindr_finish_job($1,$2,'retry',0,'transient')",[j.id,j.lease_token]);}
  assert.equal((await db.query<{state:string}>('select state from rewindr_jobs')).rows[0]!.state,'needs_review');
  assert.equal((await db.query<{phase:string}>('select phase from rewindr_recordings')).rows[0]!.phase,'failed');
  await rejectCode(()=>db.query('select rewindr_retry_job($1,$2)',[owner,fileId]),'23514');
});
test('explicit reupload repairs a failed chunk source without changing the job identity',async t=>{
  await setup(t);const original=await enqueue();const j=(await claim())!;
  await db.query("select rewindr_finish_job($1,$2,'failed',0,'source missing')",[j.id,j.lease_token]);
  const fresh={...input,fileId:worker};const retried=await enqueue(owner,fresh);assert.equal(retried.id,original.id);assert.equal(retried.state,'queued');
  const saved=(await db.query<{input:typeof input}>('select input from rewindr_jobs where id=$1',[original.id])).rows[0]!.input;assert.equal(saved.fileId,worker);
  assert.equal((await claim())!.id,original.id);
});
test('successful progress resets consecutive retry failures rather than exhausting a lifetime budget',async t=>{
  await setup(t);await enqueue();let j=(await claim())!;
  await db.query("select rewindr_finish_job($1,$2,'retry',0,'temporary')",[j.id,j.lease_token]);j=(await claim())!;assert.equal(j.failures,1);
  await db.query("select rewindr_finish_job($1,$2,'pending',0,null)",[j.id,j.lease_token]);j=(await claim())!;assert.equal(j.failures,0);
});
