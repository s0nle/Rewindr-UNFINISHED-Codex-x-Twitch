import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,readFile,readdir,rm,writeFile,mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import ffmpeg from 'ffmpeg-static';
import {buildApp} from '../src/app.js';
import {createLocalPersistence} from '../src/local-persistence.js';
import {JobWorker,chunkJobFile} from '../src/job-worker.js';
import {chunkJobInputSchema,LeaseLostError,type Job,type JobQueue,type JobOutcome} from '../src/jobs.js';
import type {Persistence} from '../src/persistence.js';
import type {ChunkStorage} from '../src/chunk-storage.js';
import {chunkObjectPath} from '../src/chunk-storage.js';
import type {Broadcast,Recording,ChunkStorageRef,VideoSearchProvider} from '../src/types.js';
import {HttpError} from '../src/http-errors.js';

const owner='11111111-1111-4111-8111-111111111111',id='33333333-3333-4333-8333-333333333333';
let fixture:Buffer,fixtureRoot:string;
before(async()=>{fixtureRoot=await mkdtemp(join(tmpdir(),'rewindr-worker-fixture-'));const file=join(fixtureRoot,'fixture.mp4');
  await promisify(execFile)(ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=10','-t','4','-an','-c:v','libx264','-pix_fmt','yuv420p',file],{windowsHide:true,timeout:30000});fixture=await readFile(file);});
after(async()=>{if(!fixtureRoot.startsWith(join(tmpdir(),'rewindr-worker-fixture-')))throw Error('Bad cleanup');await rm(fixtureRoot,{recursive:true,force:true});});

// A deterministic transport fake; SQL transactions and real pgmq are tested separately.
class MemoryQueue implements JobQueue {
  readonly ownerId=owner;readonly queueName='rewindr_jobs';records=new Map<string,Job>();online=false;checkpointFailures=0;completeFailures=0;
  constructor(private persistence:Persistence){}
  async get(key:string){return this.records.get(key);}
  async enqueue(kind:Job['kind'],resourceId:string,input:Record<string,unknown>={}) {
    const existing=[...this.records.values()].find(j=>j.kind===kind&&(kind==='recording'?j.recording_id===resourceId:j.broadcast_id===resourceId&&j.input.indexedAssetId===input.indexedAssetId));
    if(existing)return structuredClone(existing);
    const now=new Date().toISOString();const job:Job={id:kind==='recording'?resourceId:randomUUID(),owner_id:owner,queue_name:this.queueName,kind,recording_id:kind==='recording'?resourceId:null,
      broadcast_id:kind==='chunk_upload'?resourceId:null,input,state:'queued',attempts:0,failures:0,lease_token:null,lease_until:null,created_at:now,updated_at:now,available_at:now,error:null};
    this.records.set(job.id,job);return structuredClone(job);
  }
  async claim(){this.online=true;const job=[...this.records.values()].find(j=>j.state==='queued'||j.state==='retrying');if(!job)return undefined;
    job.state='running';job.attempts++;job.lease_token=randomUUID();return structuredClone(job);}
  private check(job:Job){const row=this.records.get(job.id)!;if(row.state!=='running'||row.lease_token!==job.lease_token)throw new LeaseLostError();return row;}
  async renew(job:Job){this.check(job);}
  async checkpoint(job:Job,record:Recording){this.check(job);if(this.checkpointFailures>0){this.checkpointFailures--;throw new HttpError(503,'temporary');}await this.persistence.recordings.put(record);}
  async completeChunk(job:Job,ref:ChunkStorageRef){this.check(job);if(this.completeFailures>0){this.completeFailures--;throw new HttpError(503,'temporary');}
    await this.persistence.broadcasts.attachStorage!(job.broadcast_id!,String(job.input.indexedAssetId),ref);this.records.get(job.id)!.state='completed';}
  async finish(job:Job,outcome:JobOutcome,_delay?:number,error?:string){const row=this.check(job);row.state=outcome==='pending'?'queued':outcome==='retry'?'retrying':outcome;row.lease_token=null;
    row.failures=outcome==='retry'?row.failures+1:0;row.error=error??null;}
  async retry(key:string){const row=this.records.get(key)!;if(row.state!=='failed')throw new HttpError(409,'cannot retry');row.state='queued';return structuredClone(row);}
  async status(){return {queue:this.queueName,pending:[...this.records.values()].filter(j=>['queued','running','retrying'].includes(j.state)).length,failed:0,needsReview:0,workerOnline:this.online,workerLastSeenAt:null};}
}
async function setup(t:{after:(fn:()=>Promise<void>)=>void}) {
  const root=await mkdtemp(join(tmpdir(),'rewindr-job-test-'));const persistence=createLocalPersistence(root);await persistence.init();
  const broadcast:Broadcast={id,title:'Test',indexId:'index',sourceOffsetSeconds:0,createdAt:new Date().toISOString(),chunks:[{filename:'chunk_000.mp4',indexedAssetId:'asset',startSeconds:0,endSeconds:4}]};
  await persistence.broadcasts.put(broadcast);const queue=new MemoryQueue(persistence);let uploads=0;const objects=new Set<string>();
  const storage:ChunkStorage={bucket:'rewindr-chunks',async init(){},async upload(b,a,_file,sha,bytes){uploads++;const path=chunkObjectPath(owner,b,a,sha);const reused=objects.has(path);objects.add(path);
    return {reference:{bucket:'rewindr-chunks',path,sha256:sha,bytes,uploadedAt:new Date().toISOString()},reused};},async sign(){return {url:'https://example.invalid/private',expiresAt:new Date().toISOString()};}};
  const makeWorker=(provider?:VideoSearchProvider)=>new JobWorker({queue,dataDir:root,persistence:()=>persistence,storage:()=>storage,provider,leaseRetryMs:1,processorRetry:{initialDelayMs:1,maxDelayMs:2,maxFailures:5}});
  t.after(async()=>{if(!root.startsWith(join(tmpdir(),'rewindr-job-test-')))throw Error('Bad cleanup');await rm(root,{recursive:true,force:true});});
  const stage=async()=>{const fileId=randomUUID();await mkdir(join(root,'chunk-jobs'),{recursive:true});await writeFile(chunkJobFile(root,fileId),fixture);return queue.enqueue('chunk_upload',id,{fileId,indexedAssetId:'asset',filename:'chunk_000.mp4',bytes:fixture.length,sha256:createHash('sha256').update(fixture).digest('hex')});};
  return {root,persistence,queue,storage,makeWorker,stage,get uploads(){return uploads;},get objectCount(){return objects.size;}};
}
function multipart(){return {headers:{'content-type':'multipart/form-data; boundary=job','x-creator-authorized':'true'},payload:Buffer.concat([Buffer.from('--job\r\nContent-Disposition: form-data; name="chunk"; filename="chunk_000.mp4"\r\nContent-Type: video/mp4\r\n\r\n'),fixture,Buffer.from('\r\n--job--\r\n')])};}
test('queued chunk uploads survive API restart and only the external worker uploads/attaches/removes staging',async t=>{
  const state=await setup(t);const options={dataDir:state.root,persistence:state.persistence,jobs:state.queue,storage:state.storage};
  const api=await buildApp(options);const response=await api.inject({method:'POST',url:`/broadcasts/${id}/chunks/asset/media`,...multipart()});assert.equal(response.statusCode,202,response.body);
  assert.equal(state.uploads,0);const jobId=response.json().jobId;
  const duplicate=await api.inject({method:'POST',url:`/broadcasts/${id}/chunks/asset/media`,...multipart()});assert.equal(duplicate.json().jobId,jobId);
  assert.equal((await readdir(join(state.root,'chunk-jobs'))).length,1);
  assert.equal((await api.inject('/health')).statusCode,503);await api.close();
  const restarted=await buildApp(options);t.after(()=>restarted.close());await state.makeWorker().runOnce();
  assert.equal(state.uploads,1);assert.equal((await state.persistence.broadcasts.get(id))!.chunks[0]!.storage!.bytes,fixture.length);
  const status=await restarted.inject(`/jobs/${jobId}`);assert.equal(status.json().state,'completed');assert.equal(status.json().input,undefined);assert.equal(status.json().lease_token,undefined);
  assert.deepEqual(await readdir(join(state.root,'chunk-jobs')),[]);assert.equal((await restarted.inject('/health')).statusCode,200);
});
test('a failed database acknowledgment retains the staged source and retry reuses the same cloud object',async t=>{
  const state=await setup(t);const job=await state.stage();state.queue.completeFailures=1;
  await state.makeWorker().runOnce();assert.equal((await state.queue.get(job.id))!.state,'retrying');assert.equal((await readdir(join(state.root,'chunk-jobs'))).length,1);
  await state.makeWorker().runOnce();assert.equal((await state.queue.get(job.id))!.state,'completed');assert.equal(state.uploads,2);assert.equal(state.objectCount,1);
  assert.deepEqual(await readdir(join(state.root,'chunk-jobs')),[]);
});
test('an uncertain enqueue retains the staged file instead of deleting potentially committed work',async t=>{
  const state=await setup(t);const enqueue=state.queue.enqueue.bind(state.queue);state.queue.enqueue=async(...args)=>{await enqueue(...args);throw new HttpError(503,'uncertain');};
  const app=await buildApp({dataDir:state.root,persistence:state.persistence,jobs:state.queue,storage:state.storage});t.after(()=>app.close());
  assert.equal((await app.inject({method:'POST',url:`/broadcasts/${id}/chunks/asset/media`,...multipart()})).statusCode,503);
  assert.equal((await readdir(join(state.root,'chunk-jobs'))).length,1);await state.makeWorker().runOnce();assert.equal(state.uploads,1);
});
function recording(phase:Recording['phase']):Recording {const now=new Date().toISOString();return {id:randomUUID(),filename:'source.mp4',bytes:10,sha256:'b'.repeat(64),creatorAuthorized:true,authorizedAt:now,createdAt:now,updatedAt:now,phase,
  media:{durationSeconds:1200,width:1280,height:720,videoCodec:'h264'},indexId:'index',assetId:'asset',indexedAssetId:phase==='indexing'?'indexed':undefined};}
test('recording jobs resume known indexed assets and never recover another worker’s unclaimed record',async t=>{
  const state=await setup(t);const first=recording('indexing'),other=recording('creating_upload');await state.persistence.recordings.put(first);await state.persistence.recordings.put(other);
  const job=await state.queue.enqueue('recording',first.id);let polls=0;
  const provider={name:'twelvelabs',async indexStatus(index:string,asset:string){assert.equal(index,'index');assert.equal(asset,'indexed');polls++;return 'ready';}} as unknown as VideoSearchProvider;
  await state.makeWorker(provider).runOnce();assert.equal(polls,1);assert.equal((await state.queue.get(job.id))!.state,'completed');
  assert.equal((await state.persistence.recordings.get(first.id))!.phase,'ready');assert.equal((await state.persistence.recordings.get(other.id))!.phase,'creating_upload');
});
test('ambiguous provider submissions go to needs_review without repeating paid work',async t=>{
  const state=await setup(t);const record=recording('creating_upload');await state.persistence.recordings.put(record);const job=await state.queue.enqueue('recording',record.id);let calls=0;
  const provider={name:'twelvelabs',async createUpload(){calls++;throw Error('must not submit');}} as unknown as VideoSearchProvider;
  await state.makeWorker(provider).runOnce();assert.equal(calls,0);assert.equal((await state.queue.get(job.id))!.state,'needs_review');assert.equal((await state.persistence.recordings.get(record.id))!.phase,'failed');
});
test('worker checkpoint retries keep returned provider IDs and do not resubmit indexing',async t=>{
  const state=await setup(t);const record=recording('asset_processing');await state.persistence.recordings.put(record);const job=await state.queue.enqueue('recording',record.id);let submissions=0;
  const checkpoint=state.queue.checkpoint.bind(state.queue);let failed=false;
  state.queue.checkpoint=async(j,r)=>{if(r.phase==='indexing'&&!failed){failed=true;throw new HttpError(503,'temporary');}await checkpoint(j,r);};
  const renew=state.queue.renew.bind(state.queue);let failRenew=false;
  state.queue.renew=async j=>{if(failRenew){failRenew=false;throw new HttpError(503,'temporary renewal');}await renew(j);};
  const provider={name:'twelvelabs',async assetStatus(){return 'ready';},async indexAsset(){submissions++;failRenew=true;return 'returned-id';},async indexStatus(){return 'ready';}} as unknown as VideoSearchProvider;
  await state.makeWorker(provider).runOnce();assert.equal(submissions,1);assert.equal((await state.persistence.recordings.get(record.id))!.indexedAssetId,'returned-id');assert.equal((await state.queue.get(job.id))!.state,'queued');
  await state.makeWorker(provider).runOnce();assert.equal(submissions,1);assert.equal((await state.queue.get(job.id))!.state,'completed');
});
test('lease loss prevents an old worker from attaching media or acknowledging the job',async t=>{
  const state=await setup(t);const job=await state.stage();const original=state.storage.upload.bind(state.storage);
  state.storage.upload=async(...args)=>{const result=await original(...args);state.queue.records.get(job.id)!.lease_token=randomUUID();return result;};
  await state.makeWorker().runOnce();assert.equal((await state.queue.get(job.id))!.state,'running');assert.equal((await state.persistence.broadcasts.get(id))!.chunks[0]!.storage,undefined);
  assert.equal((await readdir(join(state.root,'chunk-jobs'))).length,1);
});
