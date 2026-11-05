import {z} from 'zod';
import type {TwitchOAuthConfig} from './twitch-config.js';
import {twitchUserSchema,type TwitchUser} from './twitch-store.js';
import {HttpError} from './http-errors.js';
const tokenSchema=z.object({access_token:z.string().min(1),expires_in:z.number().positive(),token_type:z.string()});
const validationSchema=z.object({client_id:z.string(),expires_in:z.number(),user_id:z.string().nullish(),login:z.string().nullish()});
export class TwitchApi {
  private cached?:{token:string;expires:number;validated:number};private pending?:Promise<string>;
  constructor(readonly config:TwitchOAuthConfig,private fetcher:typeof fetch=fetch){}
  private async request(url:string,init:RequestInit={}){let response:Response;try{response=await this.fetcher(url,{...init,signal:AbortSignal.timeout(15000),redirect:'error'});}catch{throw new HttpError(503,'Twitch is temporarily unreachable. Retry shortly.');}
    if(!response.ok){await response.body?.cancel();throw new HttpError(response.status===401?401:502,'Twitch could not complete the request. Retry or reconnect.');}return response.json();}
  private async token(parameters:Record<string,string>){return tokenSchema.parse(await this.request('https://id.twitch.tv/oauth2/token',{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded'},body:new URLSearchParams({client_id:this.config.clientId,client_secret:this.config.clientSecret,...parameters})}));}
  private async validate(token:string){const value=validationSchema.parse(await this.request('https://id.twitch.tv/oauth2/validate',{headers:{Authorization:'OAuth '+token}}));if(value.client_id!==this.config.clientId||value.expires_in<=0)throw new HttpError(401,'Twitch token identity was invalid.');return value;}
  async identity(code:string):Promise<TwitchUser>{let token:string|undefined;
    try{token=(await this.token({grant_type:'authorization_code',code,redirect_uri:this.config.redirectUri})).access_token;const identity=await this.validate(token);if(!identity.user_id)throw new HttpError(401,'Twitch did not identify a user.');
      const users=z.object({data:z.array(twitchUserSchema)}).parse(await this.request('https://api.twitch.tv/helix/users?id='+encodeURIComponent(identity.user_id),{headers:{Authorization:'Bearer '+token,'Client-Id':this.config.clientId}}));const user=users.data[0];if(!user||user.id!==identity.user_id)throw new HttpError(401,'Twitch user identity did not match.');return user;
    }finally{if(token)await this.request('https://id.twitch.tv/oauth2/revoke',{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded'},body:new URLSearchParams({client_id:this.config.clientId,token})}).catch(()=>{});}
  }
  private appToken():Promise<string>{if(this.pending)return this.pending;this.pending=(async()=>{
    if(this.cached&&this.cached.expires>Date.now()+60000){if(Date.now()-this.cached.validated>=3600_000){try{await this.validate(this.cached.token);this.cached.validated=Date.now();}catch(e){if(!(e instanceof HttpError)||e.statusCode!==401)throw e;this.cached=undefined;}}if(this.cached)return this.cached.token;}
    const token=await this.token({grant_type:'client_credentials'});await this.validate(token.access_token);this.cached={token:token.access_token,expires:Date.now()+token.expires_in*1000,validated:Date.now()};return token.access_token;
  })().finally(()=>{this.pending=undefined;});return this.pending;}
  startValidation(){const timer=setInterval(()=>{if(this.cached)void this.appToken().catch(()=>{});},30*60_000);timer.unref();return()=>{clearInterval(timer);this.cached=undefined;};}
  async user(channelId:string){if(!/^\d{1,30}$/.test(channelId))throw new HttpError(400,'Invalid Twitch channel.');for(let attempt=0;;attempt++)try{const token=await this.appToken();const result=z.object({data:z.array(twitchUserSchema)}).parse(await this.request('https://api.twitch.tv/helix/users?id='+channelId,{headers:{Authorization:'Bearer '+token,'Client-Id':this.config.clientId}}));const user=result.data.find(u=>u.id===channelId);if(!user)throw new HttpError(404,'Twitch channel not found.');return user;}catch(e){if(e instanceof HttpError&&e.statusCode===401&&attempt===0){this.cached=undefined;continue;}throw e;}}
  async channel(channelId:string){if(!/^\d{1,30}$/.test(channelId))throw new HttpError(400,'Invalid Twitch channel.');
    for(let attempt=0;;attempt++){try{const token=await this.appToken();const result=z.object({data:z.array(z.object({id:z.string(),user_id:z.string(),title:z.string(),game_name:z.string(),started_at:z.string(),viewer_count:z.number().int().nonnegative()}))}).parse(await this.request('https://api.twitch.tv/helix/streams?user_id='+channelId,{headers:{Authorization:'Bearer '+token,'Client-Id':this.config.clientId}}));const stream=result.data.find(s=>s.user_id===channelId);return {online:Boolean(stream),stream:stream?{id:stream.id,title:stream.title,game:stream.game_name,startedAt:stream.started_at,viewers:stream.viewer_count}:null};}
      catch(e){if(e instanceof HttpError&&e.statusCode===401&&attempt===0){this.cached=undefined;continue;}throw e;}}
  }
}
