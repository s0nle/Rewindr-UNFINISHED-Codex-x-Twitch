import {createHash,randomBytes} from 'node:crypto';
import type {FastifyInstance} from 'fastify';
import {z} from 'zod';
import type {TwitchRepository} from './twitch-store.js';
import type {TwitchApi} from './twitch-api.js';
import {HttpError} from './http-errors.js';
const hash=(value:string)=>createHash('sha256').update(value).digest('hex');
const cookieName='rewindr_twitch_browser';
export async function registerTwitchLocalRoutes(app:FastifyInstance,options:{ownerId:string;repository?:TwitchRepository;api?:TwitchApi}){
  const configured=()=>{if(!options.repository||!options.api)throw new HttpError(503,'Set Twitch application credentials and use Supabase mode. See docs/twitch.md.');return {repository:options.repository,api:options.api};};
  app.get('/twitch/status',async()=>{
    const link=await options.repository?.byOwner(options.ownerId);let live:null|Awaited<ReturnType<TwitchApi['channel']>>=null,error:string|null=null;
    if(link&&options.api)try{live=await options.api.channel(link.twitch_user_id);}catch{error='Twitch live status is temporarily unavailable.';}
    return {configured:Boolean(options.api&&options.repository),linked:Boolean(link),channel:link?{id:link.twitch_user_id,login:link.login,displayName:link.display_name,profileImageUrl:link.profile_image_url,verifiedAt:link.verified_at}:null,live,error,
      connectUrl:options.api?new URL('/twitch/connect',options.api.config.redirectUri).href:null};
  });
  app.get('/twitch/connect',async(req,reply)=>{
    const {repository,api}=configured(),target=new URL(api.config.redirectUri);
    if(req.headers.host!==target.host)return reply.redirect(target.origin+'/twitch/connect');
    const state=randomBytes(32).toString('base64url'),browser=randomBytes(32).toString('base64url');await repository.begin(options.ownerId,hash(state),hash(browser));
    reply.header('set-cookie',`${cookieName}=${browser}; HttpOnly; SameSite=Lax; Path=/twitch; Max-Age=600`).header('referrer-policy','no-referrer');
    const url=new URL('https://id.twitch.tv/oauth2/authorize');url.search=new URLSearchParams({client_id:api.config.clientId,redirect_uri:api.config.redirectUri,response_type:'code',scope:'',state,force_verify:'true'}).toString();
    return reply.redirect(url.href);
  });
  app.get('/twitch/callback',async(req,reply)=>{
    const {repository,api}=configured();const input=z.object({state:z.string().regex(/^[A-Za-z0-9_-]{43}$/),code:z.string().min(1).max(2000).optional(),error:z.string().optional()}).safeParse(req.query);
    const browser=(req.headers.cookie||'').split(';').map(s=>s.trim()).find(s=>s.startsWith(cookieName+'='))?.slice(cookieName.length+1);
    reply.header('set-cookie',`${cookieName}=; HttpOnly; SameSite=Lax; Path=/twitch; Max-Age=0`).header('referrer-policy','no-referrer');
    if(!input.success||!browser||!/^[A-Za-z0-9_-]{43}$/.test(browser))throw new HttpError(400,'Twitch connection expired. Start Connect Twitch again.');
    const owner=await repository.consume(hash(input.data.state),hash(browser));if(owner!==options.ownerId)throw new HttpError(403,'Twitch connection owner did not match.');
    if(input.data.error)return reply.redirect('/?twitch=declined');if(!input.data.code)throw new HttpError(400,'Twitch authorization code is missing.');
    const user=await api.identity(input.data.code);await repository.link(owner,user);return reply.redirect('/?twitch=linked');
  });
  app.post('/twitch/disconnect',async()=>{const {repository}=configured();await repository.unlink(options.ownerId);return {linked:false};});
}
