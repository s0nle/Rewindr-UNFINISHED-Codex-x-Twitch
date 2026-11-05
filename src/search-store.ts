import {z} from 'zod';
import type {SupabaseClient} from '@supabase/supabase-js';
import type {Broadcast,SearchSettings} from './types.js';
import type {Job} from './jobs.js';
import {LeaseLostError,JOB_QUEUE} from './jobs.js';
import {HttpError} from './http-errors.js';
import {searchRowSchema,searchStateSchema,analysisRowSchema,analysisCheckpointSchema,assessmentSchema,ANALYSIS_REVISION,
  type SearchRow,type SearchState,type AnalysisRow,type AnalysisPart,type AnalysisCheckpoint,type Assessment} from './search-types.js';
export interface SearchRepository {
  reserveInitial?(id:string,broadcastId:string):Promise<void>;
  create(id:string,source:Broadcast,question:string,settings:SearchSettings&{candidateLimit:number},state:SearchState):Promise<SearchRow>;
  get(id:string):Promise<SearchRow|undefined>;
  reserve(id:string,version:number,requestId:string):Promise<{owned:boolean;replayed:boolean;search:SearchRow}>;
  commit(id:string,requestId:string,state:SearchState):Promise<SearchRow>;
  fail(id:string,requestId:string):Promise<void>;
  close(id:string):Promise<void>;
  analyses(id:string):Promise<{candidateId:string;analysis:AnalysisRow}[]>;
  requestAnalysis(search:SearchRow,candidateId:string,key:string,plan:AnalysisPart[],model:'pegasus1.5'|'pegasus1.6',available:boolean):Promise<AnalysisRow>;
  analysis(id:string):Promise<AnalysisRow|undefined>;
  active(id:string):Promise<boolean>;
  checkpoint(job:Job,value:AnalysisCheckpoint,status?:'processing'|'ready'|'failed'|'needs_review'|'skipped',result?:Assessment,error?:string):Promise<void>;
}
export class SupabaseSearches implements SearchRepository {
  constructor(private client:SupabaseClient,readonly ownerId:string,private queueName=JOB_QUEUE){z.uuid().parse(ownerId);}
  private async rpc(name:string,args:Record<string,unknown>) {
    const {data,error}=await this.client.rpc(name,args);
    if(error?.code==='P0002')throw new LeaseLostError();if(error?.code==='P0003')throw new HttpError(410,'This search session expired. Start a new search.');
    if(error?.code==='P0004')throw new HttpError(429,'Daily search or analysis limit reached. Usage resets at 00:00 UTC.');
    if(['23514','23503','23505'].includes(error?.code??''))throw new HttpError(409,'Search state changed. Refresh this session before retrying.');
    if(error)throw new HttpError(503,'Search session storage is unavailable. Apply the search/analysis migration and check Supabase.');return data;
  }
  async create(id:string,source:Broadcast,question:string,settings:SearchSettings&{candidateLimit:number},state:SearchState){return searchRowSchema.parse(await this.rpc('rewindr_create_search',{p_owner_id:this.ownerId,p_id:id,p_snapshot:source,p_question:question,p_settings:settings,p_state:searchStateSchema.parse(state)}));}
  async reserveInitial(id:string,broadcastId:string){await this.rpc('rewindr_search_budget',{p_owner_id:this.ownerId,p_id:id,p_broadcast_id:broadcastId});}
  async get(id:string){if(!z.uuid().safeParse(id).success)return undefined;const {data,error}=await this.client.from('rewindr_searches').select('*').eq('owner_id',this.ownerId).eq('id',id).maybeSingle();if(error)throw new HttpError(503,'Search session storage is unavailable.');if(data&&!await this.rpc('rewindr_search_archive_available',{p_owner_id:this.ownerId,p_id:id}))throw new HttpError(410,'This archive was deleted or is being removed.');return data?searchRowSchema.parse(data):undefined;}
  async reserve(id:string,version:number,requestId:string){return z.object({owned:z.boolean(),replayed:z.boolean(),search:searchRowSchema}).parse(await this.rpc('rewindr_reserve_recovery',{p_owner_id:this.ownerId,p_id:id,p_version:version,p_request_id:requestId}));}
  async commit(id:string,requestId:string,state:SearchState){return searchRowSchema.parse(await this.rpc('rewindr_commit_recovery',{p_owner_id:this.ownerId,p_id:id,p_request_id:requestId,p_state:searchStateSchema.parse(state)}));}
  async fail(id:string,requestId:string){await this.rpc('rewindr_fail_recovery',{p_owner_id:this.ownerId,p_id:id,p_request_id:requestId});}
  async close(id:string){await this.rpc('rewindr_close_search',{p_owner_id:this.ownerId,p_id:id});}
  async analyses(id:string){return z.array(z.object({candidateId:z.string(),analysis:analysisRowSchema})).parse(await this.rpc('rewindr_search_analysis_rows',{p_owner_id:this.ownerId,p_id:id}));}
  async requestAnalysis(search:SearchRow,candidateId:string,key:string,plan:AnalysisPart[],model:'pegasus1.5'|'pegasus1.6',available:boolean){return analysisRowSchema.parse(await this.rpc('rewindr_request_analysis',{
    p_owner_id:this.ownerId,p_search_id:search.id,p_version:search.version,p_candidate_id:candidateId,p_cache_key:key,p_model:model,p_revision:ANALYSIS_REVISION,p_plan:plan,p_available:available,p_queue_name:this.queueName}));}
  async analysis(id:string){const {data,error}=await this.client.from('rewindr_analyses').select('*').eq('owner_id',this.ownerId).eq('id',z.uuid().parse(id)).maybeSingle();if(error)throw new HttpError(503,'Candidate analysis storage is unavailable.');return data?analysisRowSchema.parse(data):undefined;}
  async active(id:string){return z.boolean().parse(await this.rpc('rewindr_analysis_active',{p_owner_id:this.ownerId,p_id:id}));}
  async checkpoint(job:Job,value:AnalysisCheckpoint,status:'processing'|'ready'|'failed'|'needs_review'|'skipped'='processing',result?:Assessment,error?:string){await this.rpc('rewindr_checkpoint_analysis',{
    p_job_id:job.id,p_lease_token:job.lease_token,p_checkpoint:analysisCheckpointSchema.parse(value),p_status:status,p_result:result?assessmentSchema.parse(result):null,p_error:error??null});}
}
