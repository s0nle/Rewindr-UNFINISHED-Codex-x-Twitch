import { z } from 'zod';
import type { SupabaseClient } from '@supabase/supabase-js';
import type { ChunkStorageRef, Recording } from './types.js';
import { recordingSchema, storageRefSchema } from './metadata.js';
import { HttpError } from './http-errors.js';

export const JOB_QUEUE = 'rewindr_jobs';
export const chunkJobInputSchema = z.object({ indexedAssetId:z.string().min(1).max(200),filename:z.string().min(5).max(200),
  fileId:z.uuid(),sha256:z.string().regex(/^[a-f0-9]{64}$/),bytes:z.number().int().min(1).max(50_000_000) }).strict();
export const jobSchema = z.object({id:z.uuid(),owner_id:z.uuid(),queue_name:z.string(),kind:z.enum(['recording','chunk_upload','ingest_chunk','candidate_analysis']),
  recording_id:z.uuid().nullable(),broadcast_id:z.uuid().nullable(),input:z.record(z.string(),z.unknown()),
  state:z.enum(['queued','running','retrying','completed','failed','needs_review']),attempts:z.number().int().nonnegative(),failures:z.number().int().nonnegative(),
  lease_token:z.uuid().nullable(),lease_until:z.string().nullable(),created_at:z.string(),updated_at:z.string(),available_at:z.string(),error:z.string().nullable()});
export type Job = z.infer<typeof jobSchema>;
export type JobOutcome = 'completed'|'pending'|'retry'|'failed'|'needs_review';
export interface QueueStatus { queue:string;pending:number;failed:number;needsReview:number;workerOnline:boolean;workerLastSeenAt:string|null }
export interface JobQueue {
  readonly ownerId:string; readonly queueName:string;
  get(id:string):Promise<Job|undefined>;
  enqueue(kind:'recording'|'chunk_upload',resourceId:string,input?:Record<string,unknown>):Promise<Job>;
  claim(workerId:string,leaseSeconds:number):Promise<Job|undefined>;
  renew(job:Job,leaseSeconds:number):Promise<void>;
  checkpoint(job:Job,record:Recording):Promise<void>;
  completeChunk(job:Job,reference:ChunkStorageRef):Promise<void>;
  finish(job:Job,outcome:JobOutcome,delaySeconds?:number,error?:string):Promise<void>;
  retry(id:string):Promise<Job>;
  status():Promise<QueueStatus>;
}
export class LeaseLostError extends Error { constructor(){super('Job lease is no longer owned by this worker.');} }
function failure(error:{code?:string}|null):never {
  if(error?.code==='P0002')throw new LeaseLostError();
  if(error?.code==='23514'||error?.code==='23503')throw new HttpError(409,'Job identity/state conflict. Check the target or reconcile needs_review before retrying.');
  throw new HttpError(503,'Background jobs are unavailable. Check Supabase and apply the job migration.');
}
export function visibleJob(job:Job) {
  return {jobId:job.id,kind:job.kind,state:job.state,recordingId:job.recording_id,broadcastId:job.broadcast_id,
    indexedAssetId:job.kind==='chunk_upload'?chunkJobInputSchema.parse(job.input).indexedAssetId:undefined,
    chunkId:job.kind==='ingest_chunk'?z.object({chunkId:z.uuid()}).parse(job.input).chunkId:undefined,
    analysisId:job.kind==='candidate_analysis'?z.object({analysisId:z.uuid()}).parse(job.input).analysisId:undefined,
    attempts:job.attempts,consecutiveFailures:job.failures,maxConsecutiveFailures:5,error:job.error,
    createdAt:job.created_at,updatedAt:job.updated_at,availableAt:job.available_at};
}
export class SupabaseJobQueue implements JobQueue {
  constructor(private client:SupabaseClient,readonly ownerId:string,readonly queueName=JOB_QUEUE) {
    z.uuid().parse(ownerId);if(!/^rewindr_(jobs|verify_[a-f0-9]{32})$/.test(queueName))throw Error('Invalid Rewindr queue name.');
  }
  private async rpc(name:string,args:Record<string,unknown>) {
    const {data,error}=await this.client.rpc(name,args);if(error)failure(error);return data;
  }
  async get(id:string) {
    if(!z.uuid().safeParse(id).success)return undefined;
    const {data,error}=await this.client.from('rewindr_jobs').select('*').eq('owner_id',this.ownerId).eq('queue_name',this.queueName).eq('id',id).maybeSingle();
    if(error)failure(error);return data?jobSchema.parse(data):undefined;
  }
  async enqueue(kind:'recording'|'chunk_upload',resourceId:string,input:Record<string,unknown>={}) {
    if(kind==='chunk_upload')chunkJobInputSchema.parse(input);
    return jobSchema.parse(await this.rpc('rewindr_enqueue_job',{p_owner_id:this.ownerId,p_kind:kind,p_resource_id:z.uuid().parse(resourceId),p_input:input,p_queue_name:this.queueName}));
  }
  async claim(workerId:string,leaseSeconds:number) {
    const data=await this.rpc('rewindr_claim_job',{p_queue_name:this.queueName,p_worker_id:z.uuid().parse(workerId),p_lease_seconds:leaseSeconds});
    return data?jobSchema.parse(data):undefined;
  }
  async renew(job:Job,leaseSeconds:number) {await this.rpc('rewindr_renew_job',{p_job_id:job.id,p_lease_token:job.lease_token,p_lease_seconds:leaseSeconds});}
  async checkpoint(job:Job,record:Recording) {await this.rpc('rewindr_checkpoint_recording',{p_job_id:job.id,p_lease_token:job.lease_token,p_record:recordingSchema.parse(record)});}
  async completeChunk(job:Job,reference:ChunkStorageRef) {await this.rpc('rewindr_complete_chunk_job',{p_job_id:job.id,p_lease_token:job.lease_token,p_storage:storageRefSchema.parse(reference)});}
  async finish(job:Job,outcome:JobOutcome,delaySeconds=0,error?:string) {await this.rpc('rewindr_finish_job',{p_job_id:job.id,p_lease_token:job.lease_token,p_outcome:outcome,p_delay_seconds:delaySeconds,p_error:error??null});}
  async retry(id:string) {
    const existing=await this.get(id);if(!existing)throw new HttpError(404,'Job not found.');
    return jobSchema.parse(await this.rpc('rewindr_retry_job',{p_owner_id:this.ownerId,p_job_id:id}));
  }
  async status():Promise<QueueStatus> {
    return z.object({queue:z.string(),pending:z.number().int(),failed:z.number().int(),needsReview:z.number().int(),workerOnline:z.boolean(),workerLastSeenAt:z.string().nullable()})
      .parse(await this.rpc('rewindr_queue_status',{p_owner_id:this.ownerId,p_queue_name:this.queueName}));
  }
}
