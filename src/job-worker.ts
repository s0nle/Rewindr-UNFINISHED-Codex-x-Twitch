import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import { Processor, type MetadataRetryOptions } from './processor.js';
import { chunkJobInputSchema, LeaseLostError, type Job, type JobQueue } from './jobs.js';
import { HttpError } from './http-errors.js';
import type { Persistence, RecordingRepository } from './persistence.js';
import type { ChunkStorage } from './chunk-storage.js';
import type { VideoSearchProvider } from './types.js';
import { probeChunkMedia } from './media.js';
import {chunkJobFile,lockStagedFile,StagedFileBusyError} from './staged-media.js';
export {chunkJobFile} from './staged-media.js';
import type {IngestionRepository} from './ingestions.js';
import {advanceIngestChunk} from './ingest-processor.js';
import type {SearchRepository} from './search-store.js';
import type {AnalysisProvider} from './search-types.js';
import {advanceAnalysis} from './analysis-processor.js';

export interface WorkerDependencies {
  queue:JobQueue;dataDir:string;
  persistence(owner:string):Persistence;
  storage(owner:string):ChunkStorage;
  provider?:VideoSearchProvider;
  ingestions?(owner:string):IngestionRepository;
  searches?(owner:string):SearchRepository;
  analysisProvider?:AnalysisProvider;
  analysisStorage?(owner:string):ChunkStorage;
  leaseSeconds?:number;heartbeatMs?:number;pollMs?:number;leaseRetryMs?:number;processorRetry?:MetadataRetryOptions;
}
export class JobWorker {
  readonly id=randomUUID();
  private stopping=false;
  private wake=new AbortController();
  private readonly leaseSeconds:number;
  private readonly heartbeatMs:number;
  private readonly pollMs:number;
  constructor(private options:WorkerDependencies) {
    this.leaseSeconds=options.leaseSeconds??300;this.heartbeatMs=options.heartbeatMs??30000;this.pollMs=options.pollMs??5000;
    if(!Number.isInteger(this.leaseSeconds)||this.leaseSeconds<60||this.leaseSeconds>900||this.heartbeatMs<1||this.heartbeatMs>=this.leaseSeconds*1000/2||this.pollMs<1||(options.leaseRetryMs??250)<1)throw Error('Invalid worker timing configuration.');
  }
  stop(){this.stopping=true;this.wake.abort();}
  async run() {
    let errors=0;
    while(!this.stopping) {
      try { const worked=await this.runOnce();errors=0;if(worked)continue; }
      catch { errors++;console.error('Worker queue unavailable; polling will retry.',{consecutiveFailures:errors}); }
      try {await wait(Math.min(30000,this.pollMs*2**Math.min(errors,4)),undefined,{signal:this.wake.signal});}catch{if(!this.stopping)throw Error('Worker wait interrupted.');}
    }
  }
  async runOnce() {
    if(this.stopping)return false;
    const job=await this.options.queue.claim(this.id,this.leaseSeconds);if(!job)return false;
    let lost=false;let renewing=false;let releaseFile:(()=>Promise<void>)|undefined;
    let renewal:Promise<void>|undefined;
    let safeUntil=Date.now()+this.leaseSeconds*1000-2000;
    const renew=async()=>{
      if(lost)throw new LeaseLostError();
      for(let attempt=0;;attempt++) {
        try {await this.options.queue.renew(job,this.leaseSeconds);safeUntil=Date.now()+this.leaseSeconds*1000-2000;return;}
        catch(error) {
          if(error instanceof HttpError&&error.statusCode===503&&attempt<2&&Date.now()<safeUntil-5000) {
            await wait((this.options.leaseRetryMs??250)*2**attempt);continue;
          }
          lost=true;throw error;
        }
      }
    };
    // Share one in-flight renewal between the heartbeat and checkpoint guards.
    const guard=()=>renewal??(renewal=renew().finally(()=>{renewal=undefined;}));
    const heartbeat=setInterval(()=>{
      if(renewing||lost)return;renewing=true;
      void guard().catch(()=>{console.warn('Worker lost its job lease; awaiting safe redelivery.',{jobId:job.id});}).finally(()=>{renewing=false;});
    },this.heartbeatMs);
    heartbeat.unref();
    try {
      const persistence=this.options.persistence(job.owner_id);await persistence.init();
      await guard();
      if(job.kind==='chunk_upload')releaseFile=await lockStagedFile(this.options.dataDir,chunkJobInputSchema.parse(job.input).fileId);
      if(job.kind==='ingest_chunk'){
        const chunk=await this.options.ingestions?.(job.owner_id).chunk(String(job.input.chunkId));
        if(!chunk)throw new HttpError(503,'Ingestion source is unavailable.');releaseFile=await lockStagedFile(this.options.dataDir,chunk.file_id);
      }
      if(job.kind==='candidate_analysis') {
        const repository=this.options.searches?.(job.owner_id);if(!repository)throw new HttpError(503,'Analysis worker is not configured.');
        if(!this.options.analysisProvider){const record=await repository.analysis(String(job.input.analysisId));if(!record)throw new HttpError(409,'Analysis not found.');
          await repository.checkpoint(job,record.checkpoint,record.checkpoint.parts.some(p=>p.phase==='submitting'||p.phase==='processing')?'failed':'skipped',undefined,'Candidate analysis is disabled or unavailable.');return true;}
        const outcome=await advanceAnalysis({job,repository,provider:this.options.analysisProvider,storage:this.options.analysisStorage?.(job.owner_id)??this.options.storage(job.owner_id),guard,retryMs:this.options.processorRetry?.initialDelayMs});
        if(outcome==='pending'){await guard();await this.options.queue.finish(job,'pending',5);}return true;
      }
      if(job.kind==='chunk_upload')await this.uploadChunk(job,persistence,guard);
      else {
        if(!this.options.provider)throw new HttpError(503,'Worker provider configuration is missing.');
        // Renew before each external operation. A stale worker's checkpoint RPC
        // also rejects its token, even if an already-started request returns late.
        const provider=new Proxy(this.options.provider,{get(target,key){const value=Reflect.get(target,key);return typeof value==='function'?async(...args:unknown[])=>{await guard();return value.apply(target,args);}:value;}});
        if(job.kind==='ingest_chunk') {
          const repository=this.options.ingestions?.(job.owner_id);if(!repository)throw new HttpError(503,'Ingestion worker is not configured.');
          const outcome=await advanceIngestChunk({job,repository,storage:this.options.storage(job.owner_id),provider,dataDir:this.options.dataDir,guard,retryMs:this.options.processorRetry?.initialDelayMs});
          if(outcome!=='completed'){await guard();await this.options.queue.finish(job,outcome,outcome==='pending'?5:0,outcome==='needs_review'?'Inspect TwelveLabs and reconcile the uncertain provider submission.':outcome==='failed'?'Ingestion processing failed. Inspect its status and retained provider resources.':undefined);}
          return true;
        }
        const source=persistence.recordings;
        const store:RecordingRepository={root:source.root,mediaPath:id=>source.mediaPath(id),init:()=>source.init(),get:id=>source.get(id),all:()=>source.all(),
          put:async record=>{await guard();await this.options.queue.checkpoint(job,record);}};
        const result=await new Processor(store,provider,5000,this.options.processorRetry).processOne(job.recording_id!);
        await guard();
        const outcome=result.needsReview?'needs_review':result.record.phase==='ready'?'completed':result.record.phase==='failed'?'failed':'pending';
        await this.options.queue.finish(job,outcome,outcome==='pending'?Math.min(300,Math.max(5,Math.ceil((result.retryAfterMs??0)/1000))):0,result.record.error);
      }
    } catch(error) {
      if(!lost&&!(error instanceof LeaseLostError)) {
        const terminal=error instanceof HttpError&&[404,409,413,415,422].includes(error.statusCode);
        try {await guard();await this.options.queue.finish(job,error instanceof StagedFileBusyError?'pending':terminal?'failed':'retry',error instanceof StagedFileBusyError?5:Math.min(300,2**Math.min(job.failures+1,8)),
          job.kind==='candidate_analysis'?'Candidate details could not be processed. Playback remains available; known analysis task IDs were retained.':
            terminal?'Chunk source or media mapping is invalid. Check the original file and broadcast.':'Background operation failed. Source files and provider checkpoints were retained.');}
        catch {console.warn('Job outcome could not be confirmed; it will be redelivered after its lease.',{jobId:job.id});}
      }
    } finally {clearInterval(heartbeat);if(releaseFile)await releaseFile().catch(()=>console.warn('Staged-file lock cleanup needs inspection.',{jobId:job.id}));}
    return true;
  }
  private async uploadChunk(job:Job,persistence:Persistence,guard:()=>Promise<void>) {
    const input=chunkJobInputSchema.parse(job.input);const file=chunkJobFile(this.options.dataDir,input.fileId);
    let info:Awaited<ReturnType<typeof stat>>;
    try {info=await stat(file);}catch{throw new HttpError(422,'Staged chunk is missing.');}
    if(!info.isFile()||info.size!==input.bytes)throw new HttpError(422,'Staged chunk size changed.');
    const hash=createHash('sha256');for await(const bytes of createReadStream(file))hash.update(bytes);
    if(hash.digest('hex')!==input.sha256)throw new HttpError(422,'Staged chunk content changed.');
    const broadcast=await persistence.broadcasts.get(job.broadcast_id!);
    const chunk=broadcast?.chunks.find(c=>c.indexedAssetId===input.indexedAssetId&&c.filename===input.filename);
    if(!chunk)throw new HttpError(409,'Chunk mapping is missing.');
    try {await probeChunkMedia(file,chunk.endSeconds-chunk.startSeconds);}catch{throw new HttpError(415,'Staged chunk media is invalid.');}
    await guard();
    const uploaded=await this.options.storage(job.owner_id).upload(job.broadcast_id!,input.indexedAssetId,file,input.sha256,input.bytes);
    await guard();
    await this.options.queue.completeChunk(job,uploaded.reference);
    // Only this worker's staged copy is removed, after atomic attachment + ack.
    try {await rm(file,{force:true});}catch{console.warn('Completed job left a staged copy for cleanup.',{jobId:job.id});}
  }
}
