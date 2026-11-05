import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PGlite} from '@electric-sql/pglite';
import {applyTestMigrations} from './queue-shim.js';
let db:PGlite;
const owner=randomUUID(),other=randomUUID(),id=randomUUID();
const source={mode:'live',id,title:'Live test',indexId:'test-index',sourceOffsetSeconds:7200,sourceFingerprint:'a'.repeat(64)};
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});
after(async()=>{await db?.close();});
async function setup(t:{after:(fn:()=>Promise<void>)=>void}){await db.exec('begin');t.after(async()=>{await db.exec('rollback');});await db.query("insert into rewindr_broadcasters(id,label) values($1,'test'),($2,'test')",[owner,other]);await create();}
async function create(who=owner,input=source){return(await db.query<{v:{mode:string;source_duration_seconds:number}}>('select rewindr_create_live_ingestion($1,$2::jsonb) v',[who,JSON.stringify(input)])).rows[0]!.v;}
async function accept(ordinal:number,startSeconds:number,endSeconds:number,who=owner){return(await db.query<{v:{id:string;job_id:string}}>('select rewindr_accept_ingest_chunk($1,$2,$3::jsonb) v',[who,id,JSON.stringify({fileId:randomUUID(),ordinal,filename:`chunk_${String(ordinal).padStart(4,'0')}.mp4`,startSeconds,endSeconds,sha256:'b'.repeat(64),bytes:1000})])).rows[0]!.v;}
async function reject(action:()=>Promise<unknown>,code:string){await db.exec('savepoint invalid');let caught:unknown;try{await action();}catch(e){caught=e;}await db.exec('rollback to savepoint invalid');assert.ok(caught);assert.equal((caught as {code:string}).code,code);}
test('live ingestion has an unknown final duration, private owner and immutable source identity',async t=>{
  await setup(t);assert.equal((await create()).mode,'live');assert.equal(Number((await create()).source_duration_seconds),0);
  await reject(()=>create(other),'23514');await reject(()=>create(owner,{...source,indexId:'other-index'}),'23514');
  const b=(await db.query<{b:{chunks:unknown[]}}>('select rewindr_get_broadcast($1,$2) b',[owner,id])).rows[0]!.b;assert.deepEqual(b.chunks,[]);
});
test('live intervals accept real gaps, serialize ordinals, and never remap an accepted minute on retry',async t=>{
  await setup(t);const a=await accept(0,2,62);assert.equal((await accept(0,2,62)).id,a.id);
  await reject(()=>accept(0,3,63),'23514');await reject(()=>accept(1,61,121),'23514');await reject(()=>accept(2,80,140),'23514');
  await reject(()=>accept(1,80,82),'23514');await reject(()=>accept(1,80,150),'23514');await reject(()=>accept(1,80,140,other),'23503');
  await accept(1,80,140);const value=(await db.query<{v:{mode:string;source_duration_seconds:number}}>('select to_jsonb(i) v from rewindr_ingestions i where id=$1',[id])).rows[0]!.v;assert.equal(Number(value.source_duration_seconds),140);
  assert.equal((await db.query('select * from pgmq.q_rewindr_jobs')).rows.length,2);
});
test('live sealing needs no invented duration and allows idempotent retries but no further footage',async t=>{
  await setup(t);const first=await accept(0,0,60);await db.query('select rewindr_seal_ingestion($1,$2)',[owner,id]);assert.equal((await accept(0,0,60)).job_id,first.job_id);
  await reject(()=>accept(1,60,120),'23514');await reject(()=>db.query('select rewindr_seal_ingestion($1,$2)',[other,id]),'23503');
});
test('browser roles cannot create a live source or enqueue live footage',async t=>{
  await setup(t);for(const role of ['anon','authenticated']){await db.exec(`set local role ${role}`);await reject(()=>create(),'42501');await reject(()=>accept(0,0,60),'42501');await db.exec('reset role');}
});
