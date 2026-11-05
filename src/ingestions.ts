import type { SupabaseClient } from '@supabase/supabase-js';
import { z } from 'zod';
import type { Job } from './jobs.js';
import { JOB_QUEUE,LeaseLostError } from './jobs.js';
import { ingestionCreationSchema,ingestionSchema,ingestChunkSchema,ingestCheckpointSchema,type CreateIngestion,type CreateLiveIngestion,type IngestCheckpoint,type IngestChunk,type Ingestion } from './ingestion-types.js';
import { HttpError } from './http-errors.js';
const chunkViewSchema=z.object({id:z.uuid(),ordinal:z.number().int(),filename:z.string(),startSeconds:z.number(),endSeconds:z.number(),phase:z.string(),stored:z.boolean(),
  jobId:z.uuid(),jobState:z.string(),attempts:z.number().int(),error:z.string().nullable()});
export type IngestChunkView=z.infer<typeof chunkViewSchema>;
export interface AcceptIngestChunk {fileId:string;ordinal:number;filename:string;startSeconds:number;endSeconds:number;sha256:string;bytes:number}
export interface IngestionRepository {
  create(input:CreateIngestion|CreateLiveIngestion):Promise<Ingestion>;
  get(id:string):Promise<Ingestion|undefined>;
  status(id:string):Promise<ReturnType<typeof ingestionStatus>|undefined>;
  accept(id:string,chunk:AcceptIngestChunk):Promise<IngestChunk>;
  chunk(id:string):Promise<IngestChunk|undefined>;
  checkpoint(job:Job,value:IngestCheckpoint,complete?:boolean):Promise<void>;
  seal(id:string):Promise<void>;
}
function through(chunks:IngestChunkView[],available:(c:IngestChunkView)=>boolean) {
  let end=0;for(const chunk of [...chunks].sort((a,b)=>a.startSeconds-b.startSeconds)){if(chunk.startSeconds>end+0.1||!available(chunk))break;end=Math.max(end,chunk.endSeconds);}return end;
}
export function ingestionStatus(ingestion:Ingestion,chunks:IngestChunkView[]) {
  const gaps:{startSeconds:number;endSeconds:number}[]=[];let cursor=0;
  if(ingestion.mode==='live')for(const chunk of [...chunks].sort((a,b)=>a.startSeconds-b.startSeconds)){if(chunk.startSeconds>cursor+0.1)gaps.push({startSeconds:cursor,endSeconds:chunk.startSeconds});cursor=chunk.endSeconds;}
  const needsAttention=chunks.some(c=>c.jobState==='failed'||c.jobState==='needs_review');
  const ready=Boolean(ingestion.sealed_at)&&(chunks.length>0||ingestion.mode==='live')&&chunks.every(c=>c.phase==='ready'&&c.jobState==='completed');
  return {ingestionId:ingestion.id,broadcastId:ingestion.id,title:ingestion.title,indexId:ingestion.index_id,sourceDurationSeconds:ingestion.source_duration_seconds,
    mode:ingestion.mode,gaps,sourceOffsetSeconds:ingestion.source_offset_seconds,sealedAt:ingestion.sealed_at,status:needsAttention?'attention_required':ready?'ready':ingestion.sealed_at?'processing':'accepting',
    latestCapturedSeconds:Math.max(0,...chunks.map(c=>c.endSeconds)),latestStoredSeconds:Math.max(0,...chunks.filter(c=>c.stored).map(c=>c.endSeconds)),latestSearchableSeconds:Math.max(0,...chunks.filter(c=>c.phase==='ready').map(c=>c.endSeconds)),
    capturedThroughSeconds:through(chunks,()=>true),storedThroughSeconds:through(chunks,c=>c.stored),searchableThroughSeconds:through(chunks,c=>c.phase==='ready'),
    readyChunkCount:chunks.filter(c=>c.phase==='ready').length,totalAcceptedChunks:chunks.length,chunks};
}
export class SupabaseIngestions implements IngestionRepository {
  constructor(private client:SupabaseClient,readonly ownerId:string,private queueName=JOB_QUEUE){z.uuid().parse(ownerId);}
  private async rpc(name:string,args:Record<string,unknown>) {
    const {data,error}=await this.client.rpc(name,args);
    if(error?.code==='P0002')throw new LeaseLostError();
    if(error?.code==='P0004')throw new HttpError(429,'Daily ingestion or upload-byte limit reached. Usage resets at 00:00 UTC.');
    if(error?.code==='23514'||error?.code==='23503'||error?.code==='23505')throw new HttpError(409,'Ingestion identity or state conflict. Check the source, chunk identity, and pending jobs.');
    if(error)throw new HttpError(503,'Ingestion metadata is unavailable. Apply the ingestion migration and check Supabase.');
    return data;
  }
  async create(input:CreateIngestion|CreateLiveIngestion){const value=ingestionCreationSchema.parse(input);return ingestionSchema.parse(await this.rpc('mode' in value?'rewindr_create_live_ingestion':'rewindr_create_ingestion',{p_owner_id:this.ownerId,p_input:value}));}
  async get(id:string) {
    if(!z.uuid().safeParse(id).success)return undefined;
    const {data,error}=await this.client.from('rewindr_ingestions').select('*').eq('owner_id',this.ownerId).eq('id',id).maybeSingle();
    if(error)throw new HttpError(503,'Ingestion metadata is unavailable.');return data?ingestionSchema.parse(data):undefined;
  }
  async status(id:string) {
    if(!z.uuid().safeParse(id).success)return undefined;
    const data=await this.rpc('rewindr_get_ingestion',{p_owner_id:this.ownerId,p_id:id});if(!data)return undefined;
    const decoded=z.object({ingestion:ingestionSchema,chunks:z.array(chunkViewSchema)}).parse(data);return ingestionStatus(decoded.ingestion,decoded.chunks);
  }
  async accept(id:string,chunk:AcceptIngestChunk){return ingestChunkSchema.parse(await this.rpc('rewindr_accept_ingest_chunk',{p_owner_id:this.ownerId,p_ingestion_id:id,p_chunk:chunk,p_queue_name:this.queueName}));}
  async chunk(id:string) {
    const {data,error}=await this.client.from('rewindr_ingest_chunks').select('*').eq('owner_id',this.ownerId).eq('id',z.uuid().parse(id)).maybeSingle();
    if(error)throw new HttpError(503,'Ingestion metadata is unavailable.');return data?ingestChunkSchema.parse(data):undefined;
  }
  async checkpoint(job:Job,value:IngestCheckpoint,complete=false) {await this.rpc('rewindr_checkpoint_ingest_chunk',{p_job_id:job.id,p_lease_token:job.lease_token,p_checkpoint:ingestCheckpointSchema.parse(value),p_complete:complete});}
  async seal(id:string){await this.rpc('rewindr_seal_ingestion',{p_owner_id:this.ownerId,p_id:id});}
}
