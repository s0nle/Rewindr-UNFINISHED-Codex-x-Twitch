import {createHash,randomBytes} from 'node:crypto';
import type {FastifyInstance} from 'fastify';
import type {SupabaseClient} from '@supabase/supabase-js';
import type {TwitchApi} from './twitch-api.js';
import {SupabaseIngestions} from './ingestions.js';
import {SupabaseJobQueue,visibleJob} from './jobs.js';
import {registerIngestionRoutes} from './ingestion-routes.js';
import {HttpError} from './http-errors.js';
import {SupabaseMedia} from './media-store.js';
export const tokenHash=(token:string)=>createHash('sha256').update(token).digest('hex');
export interface HostedOptions{client:SupabaseClient;api:TwitchApi;dataDir:string;indexId:string;maxChannels:number;allowlist:string[]}
export class HostedChannels{
  constructor(readonly options:HostedOptions){}
  async onboard(channel:string){
    if(this.options.allowlist.length&&!this.options.allowlist.includes(channel))throw new HttpError(403,'This channel is not enabled for hosted onboarding.');
    const user=await this.options.api.user(channel),token='rwa_'+randomBytes(32).toString('base64url');
    const store=new SupabaseMedia(this.options.client,'00000000-0000-4000-8000-000000000001');
    await store.rpc('rewindr_onboard_channel',{p_channel:channel,p_user:user,p_token_hash:tokenHash(token),p_max_channels:this.options.maxChannels});
    return {token,indexId:this.options.indexId,expiresInDays:30};
  }
  async revoke(owner:string){await new SupabaseMedia(this.options.client,owner).rpc('rewindr_revoke_agent',{p_owner_id:owner});}
  async owner(token:string){if(!/^rwa_[A-Za-z0-9_-]{43}$/.test(token))throw new HttpError(401,'Valid capture credential required.');const {data,error}=await this.options.client.rpc('rewindr_agent_owner',{p_hash:tokenHash(token)});if(error)throw new HttpError(503,'Capture authorization unavailable.');if(typeof data!=='string')throw new HttpError(401,'Capture credential expired or revoked.');return data;}
  async register(app:FastifyInstance,provider:Parameters<typeof registerIngestionRoutes>[1]['provider']){
    const ownerFor=async(req:{headers:{authorization?:string}})=>{const header=req.headers.authorization;if(!header?.startsWith('Bearer '))throw new HttpError(401,'Capture credential required.');return this.owner(header.slice(7));};
    app.addHook('onRequest',async req=>{await ownerFor(req);});
    app.get('/account',async req=>({ownerId:await ownerFor(req)}));
    await registerIngestionRoutes(app,{dataDir:this.options.dataDir,provider,allowedIndex:this.options.indexId,ingestions:async req=>new SupabaseIngestions(this.options.client,await ownerFor(req))});
    app.get<{Params:{id:string}}>('/jobs/:id',async req=>{const queue=new SupabaseJobQueue(this.options.client,await ownerFor(req)),job=await queue.get(req.params.id);if(!job||job.kind!=='ingest_chunk')throw new HttpError(404,'Capture job not found.');return visibleJob(job);});
  }
}
