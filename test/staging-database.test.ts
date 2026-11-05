import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {PGlite} from '@electric-sql/pglite';
import {applyTestMigrations} from './queue-shim.js';
import type {StagedFileStatus} from '../src/staging-sweeper.js';
let db:PGlite;const owner=randomUUID(),other=randomUUID(),bid=randomUUID();
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});after(async()=>{await db.close();});
async function setup(t:{after:(fn:()=>Promise<void>)=>void}){await db.exec('begin');t.after(async()=>{await db.exec('rollback');});await db.query("insert into rewindr_broadcasters(id,label) values($1,'test'),($2,'test')",[owner,other]);}
async function status(fileId:string){return(await db.query<{v:StagedFileStatus[]}>('select rewindr_staged_file_status($1) v',[[fileId]])).rows[0]!.v[0]!;}
test('staging catalog sees other-owner jobs and preserves failed files without a recoverable source',async t=>{
  await setup(t);const fileId=randomUUID();await db.query('select rewindr_save_broadcast($1,$2::jsonb)',[other,JSON.stringify({id:bid,title:'Test',indexId:'test',createdAt:new Date().toISOString(),sourceOffsetSeconds:0,chunks:[{filename:'chunk.mp4',indexedAssetId:'asset',startSeconds:0,endSeconds:4}]})]);
  await db.query("select rewindr_enqueue_job($1,'chunk_upload',$2,$3::jsonb)",[other,bid,JSON.stringify({fileId,indexedAssetId:'asset',filename:'chunk.mp4',sha256:'a'.repeat(64),bytes:1000})]);assert.equal((await status(fileId)).state,'keep');
  await db.query("update rewindr_jobs set state='failed' where owner_id=$1",[other]);assert.equal((await status(fileId)).state,'keep');assert.equal((await status(randomUUID())).state,'orphan');
});
test('terminal ingestion sources with immutable Storage checkpoints become recoverable, while running work remains protected',async t=>{
  await setup(t);await db.query('select rewindr_create_ingestion($1,$2::jsonb)',[owner,JSON.stringify({id:bid,title:'Test',indexId:'test',sourceDurationSeconds:4,sourceOffsetSeconds:0,sourceFingerprint:'a'.repeat(64)})]);const id=randomUUID();
  const chunk=(await db.query<{v:{job_id:string}}>('select rewindr_accept_ingest_chunk($1,$2,$3::jsonb) v',[owner,bid,JSON.stringify({fileId:id,ordinal:0,filename:'chunk_0000.mp4',startSeconds:0,endSeconds:4,sha256:'b'.repeat(64),bytes:1000})])).rows[0]!.v;
  const reference={bucket:'rewindr-chunks',path:`${owner}/${bid}/${createHash('sha256').update('ingest:'+id).digest('hex')}/${'b'.repeat(64)}.mp4`,sha256:'b'.repeat(64),bytes:1000,uploadedAt:new Date().toISOString()};
  await db.query('update rewindr_ingest_chunks set checkpoint=$1::jsonb where id=$2',[JSON.stringify({phase:'stored',storage:reference}),id]);assert.equal((await status(id)).state,'keep');
  await db.query("update rewindr_jobs set state='failed' where id=$1",[chunk.job_id]);const row=await status(id);assert.equal(row.state,'recoverable');assert.deepEqual(row.replicas,[{bucket:reference.bucket,path:reference.path}]);
  await db.query("update rewindr_jobs set state='running' where id=$1",[chunk.job_id]);assert.equal((await status(id)).state,'keep');
});
test('browser roles cannot inspect global staging references',async t=>{
  await setup(t);await db.exec('set local role authenticated;savepoint denied');let code:string|undefined;try{await status(randomUUID());}catch(e){code=(e as {code:string}).code;}await db.exec('rollback to savepoint denied;reset role');assert.equal(code,'42501');
});
