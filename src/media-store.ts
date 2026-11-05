import {z} from 'zod';
import type {SupabaseClient} from '@supabase/supabase-js';
import {broadcastSchema} from './metadata.js';
import {HttpError} from './http-errors.js';
import {LeaseLostError} from './jobs.js';
export const exportStorageSchema=z.object({bucket:z.string().regex(/^[a-z0-9][a-z0-9_-]{1,62}$/),path:z.string(),sha256:z.string().regex(/^[a-f0-9]{64}$/),bytes:z.number().int().positive().max(50000000)}).strict();
export type ExportStorage=z.infer<typeof exportStorageSchema>;
export const exportRowSchema=z.object({id:z.uuid(),owner_id:z.uuid(),broadcast_id:z.uuid(),title:z.string(),snapshot:broadcastSchema,start_ms:z.coerce.number().int().nonnegative(),end_ms:z.coerce.number().int().positive(),storage:exportStorageSchema.nullable(),ready_at:z.string().nullable(),expires_at:z.string(),created_at:z.string()});
export type ExportRow=z.infer<typeof exportRowSchema>;
export const mediaTaskSchema=z.object({id:z.uuid(),owner_id:z.uuid(),kind:z.enum(['export','purge']),input:z.record(z.string(),z.unknown()),state:z.enum(['queued','running','retrying','completed','failed']),failures:z.number().int(),attempts:z.number().int(),completed_objects:z.number().int().nonnegative(),lease_token:z.uuid().nullable(),lease_until:z.string().nullable(),available_at:z.string(),error:z.string().nullable(),created_at:z.string(),updated_at:z.string()});
export type MediaTask=z.infer<typeof mediaTaskSchema>;
export const publicationSchema=z.object({channel_id:z.string(),owner_id:z.uuid(),export_id:z.uuid().nullable(),version:z.coerce.number().int().nonnegative(),updated_at:z.string()});
export type Publication=z.infer<typeof publicationSchema>;
export interface MediaRepository{
  export(id:string):Promise<ExportRow|undefined>;exports():Promise<ExportRow[]>;begin(id:string,broadcastId:string,title:string,startMs:number,endMs:number):Promise<ExportRow>;
  task(id:string):Promise<MediaTask|undefined>;tasks():Promise<MediaTask[]>;claim():Promise<MediaTask|undefined>;guard(task:MediaTask):Promise<void>;progress(task:MediaTask,count:number):Promise<void>;checkpoint(task:MediaTask,storage:ExportStorage):Promise<void>;finish(task:MediaTask,error?:string,terminal?:boolean):Promise<void>;retry(id:string):Promise<void>;
  publication(channel:string):Promise<Publication|undefined>;publish(channel:string,id:string|null,version:number):Promise<Publication>;
  purge(id:string,broadcastId:string,confirm:string):Promise<MediaTask>;usage():Promise<{metric:string;amount:number}[]>;
}
export class SupabaseMedia implements MediaRepository{
  constructor(private client:SupabaseClient,readonly owner:string,private claimOwner?:string){z.uuid().parse(owner);if(claimOwner)z.uuid().parse(claimOwner);}
  async rpc(name:string,args:Record<string,unknown>){const {data,error}=await this.client.rpc(name,args);if(error){if(error.code==='P0002')throw new LeaseLostError();if(error.code==='P0004')throw new HttpError(429,'Channel usage limit reached. Check usage or wait until the next UTC day.');if(['23514','23503','23505'].includes(error.code))throw new HttpError(409,'Media state changed, active work remains, or this request conflicts. Refresh and retry.');throw new HttpError(503,'Media storage is temporarily unavailable. Check migrations and retry.');}return data;}
  private async rows(table:string,filters:Record<string,string>={}){let q=this.client.from(table).select('*').eq('owner_id',this.owner);for(const [key,value] of Object.entries(filters))q=q.eq(key,value);const {data,error}=await q.order(table==='rewindr_publications'?'updated_at':'created_at',{ascending:false}).limit(100);if(error)throw new HttpError(503,'Media records unavailable.');return data;}
  async export(id:string){if(!z.uuid().safeParse(id).success)return;const rows=await this.rows('rewindr_exports',{id});return rows?.[0]?exportRowSchema.parse(rows[0]):undefined;}
  async exports(){return z.array(exportRowSchema).parse(await this.rows('rewindr_exports'));}
  async begin(id:string,broadcastId:string,title:string,startMs:number,endMs:number){return exportRowSchema.parse(await this.rpc('rewindr_begin_export',{p_owner_id:this.owner,p_id:id,p_broadcast_id:broadcastId,p_title:title,p_start_ms:startMs,p_end_ms:endMs}));}
  async task(id:string){if(!z.uuid().safeParse(id).success)return;const rows=await this.rows('rewindr_media_tasks',{id});return rows?.[0]?mediaTaskSchema.parse(rows[0]):undefined;}
  async tasks(){return z.array(mediaTaskSchema).parse(await this.rows('rewindr_media_tasks'));}
  async claim(){const row=await this.rpc('rewindr_claim_media',{p_owner_id:this.claimOwner??null});return row?mediaTaskSchema.parse(row):undefined;}
  async guard(task:MediaTask){await this.rpc('rewindr_media_guard',{p_id:task.id,p_token:task.lease_token});}
  async progress(task:MediaTask,count:number){await this.rpc('rewindr_progress_media',{p_id:task.id,p_token:task.lease_token,p_completed:count});}
  async checkpoint(task:MediaTask,storage:ExportStorage){await this.rpc('rewindr_export_checkpoint',{p_id:task.id,p_token:task.lease_token,p_storage:storage});}
  async finish(task:MediaTask,error?:string,terminal=false){await this.rpc('rewindr_finish_media',{p_id:task.id,p_token:task.lease_token,p_error:error??null,p_terminal:terminal});}
  async retry(id:string){await this.rpc('rewindr_retry_media',{p_owner_id:this.owner,p_id:id});}
  async publication(channel:string){const rows=await this.rows('rewindr_publications',{channel_id:channel});return rows?.[0]?publicationSchema.parse(rows[0]):undefined;}
  async publish(channel:string,id:string|null,version:number){return publicationSchema.parse(await this.rpc('rewindr_set_publication',{p_owner_id:this.owner,p_channel:channel,p_export_id:id,p_version:version}));}
  async purge(id:string,broadcastId:string,confirm:string){return mediaTaskSchema.parse(await this.rpc('rewindr_begin_purge',{p_owner_id:this.owner,p_id:id,p_broadcast_id:broadcastId,p_confirm:confirm}));}
  async usage(){return z.array(z.object({metric:z.string(),amount:z.coerce.number().nonnegative()})).parse(await this.rpc('rewindr_usage_today',{p_owner_id:this.owner}));}
}
