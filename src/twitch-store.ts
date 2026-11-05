import type {SupabaseClient} from '@supabase/supabase-js';
import {z} from 'zod';
import {HttpError} from './http-errors.js';
export const twitchUserSchema=z.object({id:z.string().regex(/^\d{1,30}$/),login:z.string().min(1).max(100),display_name:z.string().min(1).max(200),profile_image_url:z.string().url().startsWith('https://')});
export type TwitchUser=z.infer<typeof twitchUserSchema>;
export const twitchLinkSchema=z.object({owner_id:z.uuid(),twitch_user_id:z.string().regex(/^\d{1,30}$/),login:z.string(),display_name:z.string(),profile_image_url:z.string(),verified_at:z.string()});
export type TwitchLink=z.infer<typeof twitchLinkSchema>;
export interface TwitchRepository {begin(owner:string,stateHash:string,browserHash:string):Promise<void>;consume(stateHash:string,browserHash:string):Promise<string>;link(owner:string,user:TwitchUser):Promise<TwitchLink>;unlink(owner:string):Promise<void>;byOwner(owner:string):Promise<TwitchLink|undefined>;byChannel(channel:string):Promise<TwitchLink|undefined>}
export class SupabaseTwitch implements TwitchRepository {
  constructor(private client:SupabaseClient){}
  private async rpc(name:string,args:Record<string,unknown>){const {data,error}=await this.client.rpc(name,args);if(error){if(['23514','23505','23503'].includes(error.code))throw new HttpError(409,'Twitch connection state changed or this channel is already linked. Restart connection, or disconnect the existing link first.');throw new HttpError(503,'Twitch connection storage is unavailable. Apply the Twitch migration.');}return data;}
  async begin(owner:string,stateHash:string,browserHash:string){await this.rpc('rewindr_twitch_begin',{p_owner_id:owner,p_state_hash:stateHash,p_browser_hash:browserHash});}
  async consume(stateHash:string,browserHash:string){return z.uuid().parse(await this.rpc('rewindr_twitch_consume',{p_state_hash:stateHash,p_browser_hash:browserHash}));}
  async link(owner:string,user:TwitchUser){return twitchLinkSchema.parse(await this.rpc('rewindr_twitch_link',{p_owner_id:owner,p_user:twitchUserSchema.parse(user)}));}
  async unlink(owner:string){await this.rpc('rewindr_twitch_unlink',{p_owner_id:owner});}
  private async find(field:'owner_id'|'twitch_user_id',value:string){const {data,error}=await this.client.from('rewindr_twitch_links').select('*').eq(field,value).maybeSingle();if(error)throw new HttpError(503,'Twitch connection storage is unavailable.');return data?twitchLinkSchema.parse(data):undefined;}
  byOwner(owner:string){return this.find('owner_id',z.uuid().parse(owner));}
  byChannel(channel:string){if(!/^\d{1,30}$/.test(channel))return Promise.resolve(undefined);return this.find('twitch_user_id',channel);}
}
