import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';
import {existsSync} from 'node:fs';
import {resolve} from 'node:path';
import {z} from 'zod';
import type {Persistence} from './persistence.js';
import type {ChunkStorage} from './chunk-storage.js';
import type {SearchRepository} from './search-store.js';
import type {VideoSearchProvider} from './types.js';
import type {TwitchRepository} from './twitch-store.js';
import type {TwitchExtensionConfig} from './twitch-config.js';
import type {TwitchApi} from './twitch-api.js';
import {verifyBroadcasterToken,verifyExtensionToken} from './twitch-jwt.js';
import {HttpError} from './http-errors.js';
import {SearchService,type SearchView} from './search-service.js';
import {broadcastSearchSchema} from './chunk-search.js';
import {signedPlayback} from './playback.js';
import {registerMediaLifecycleRoutes} from './media-lifecycle-routes.js';
import type {MediaRepository} from './media-store.js';
import type {ExportStore} from './export-storage.js';
import type {HostedChannels} from './hosted.js';
import multipart from '@fastify/multipart';
import type {IngestionRepository} from './ingestions.js';
export interface TwitchScope {persistence:Persistence;searches:SearchRepository;storage:ChunkStorage;media?:MediaRepository;exports?:ExportStore;ingestions?:IngestionRepository}
const view=(value:SearchView)=>({...value,candidates:value.candidates.map(c=>({...c,sourceUrl:null,playbackUrl:null,replayAvailable:Boolean(c.manifestUrl)}))});
export async function buildTwitchEbs(options:{config:TwitchExtensionConfig;links:TwitchRepository;scope:(owner:string)=>TwitchScope;provider?:VideoSearchProvider;api?:TwitchApi;hosted?:HostedChannels;ready?:()=>Promise<void>;analysisConfigured:boolean;analysisModel:'pegasus1.5'|'pegasus1.6';logger?:boolean}){
  const app=Fastify({bodyLimit:16384,requestTimeout:180000,logger:options.logger?{serializers:{req:(req:{method:string;url:string})=>({method:req.method,url:req.url.split('?')[0]})}}:false,trustProxy:false});
  await app.register(multipart,{limits:{files:1,fields:0,parts:1,fileSize:50000000,fieldNameSize:64,headerPairs:100}});
  const allowedHosts=new Set([new URL(options.config.publicUrl).host,`127.0.0.1:${options.config.port}`,`localhost:${options.config.port}`]);
  app.addHook('onRequest',async(req,reply)=>{
    if(!allowedHosts.has(req.headers.host||''))throw new HttpError(403,'Unexpected Extension backend host.');
    const origin=req.headers.origin;if(origin&&!options.config.origins.includes(origin))throw new HttpError(403,'Extension origin is not allowed.');
    reply.header('cache-control','no-store').header('x-content-type-options','nosniff').header('referrer-policy','no-referrer');
    if(origin)reply.header('access-control-allow-origin',origin).header('vary','Origin');
    if(req.method==='OPTIONS'){
      if(!origin||!['/extension/','/viewer/'].some(p=>req.url.startsWith(p)))throw new HttpError(403,'Invalid Extension preflight.');
      reply.header('access-control-allow-methods','GET, POST, OPTIONS').header('access-control-allow-headers','Authorization, Content-Type, Ngrok-Skip-Browser-Warning');return reply.code(204).send();
    }
  });
  app.setErrorHandler((error,_req,reply)=>{const e=error as {statusCode?:number};const status=e.statusCode&&e.statusCode>=400&&e.statusCode<=599?e.statusCode:500;reply.code(status).send({error:error instanceof HttpError?error.message:status===404?'Not found.':'Extension operation failed. Retry shortly.'});});
  app.get('/health',async()=>({status:'ok',service:'rewindr-extension'}));
  let readyUntil=0,readyOk=false,readyPending:Promise<void>|undefined;
  app.get('/ready',async()=>{if(Date.now()>=readyUntil)await(readyPending??(readyPending=(async()=>{try{await options.ready?.();readyOk=true;}catch{readyOk=false;}finally{readyUntil=Date.now()+5000;readyPending=undefined;}})()));if(!readyOk)throw new HttpError(503,'Database or private media storage is unavailable.');return {status:'ok'};});
  const requests=new Map<string,{since:number;count:number}>();
  await app.register(async privateRoutes=>{
    privateRoutes.decorateRequest('rewindrChannel',null);
    privateRoutes.addHook('preHandler',async(req)=>{
      const header=req.headers.authorization;if(!header?.startsWith('Bearer '))throw new HttpError(401,'A Twitch Extension token is required.');
      const identity=verifyBroadcasterToken(header.slice(7),options.config.secrets,options.config.clientId);
      const key=identity.channelId+(req.method==='POST'?':write':':read'),now=Date.now();let budget=requests.get(key);
      if(!budget||now-budget.since>60000){budget={since:now,count:0};if(requests.size>=5000){for(const [id,b] of requests)if(now-b.since>60000)requests.delete(id);if(requests.size>=5000&&!requests.has(key))throw new HttpError(503,'Extension is busy. Retry shortly.');}requests.set(key,budget);}
      if(++budget.count>(req.method==='POST'?30:180))throw new HttpError(429,'Too many requests. Retry shortly.');
      (req as typeof req&{rewindrChannel:string}).rewindrChannel=identity.channelId;
    });
    const channel=(req:unknown)=>(req as {rewindrChannel:string}).rewindrChannel;
    const scoped=async(req:unknown)=>{const link=await options.links.byChannel(channel(req));if(!link)throw new HttpError(403,'Connect this Twitch channel in the local Rewindr dashboard first.');return options.scope(link.owner_id);};
    const service=(scope:TwitchScope)=>new SearchService(scope.searches,options.provider,{storageEnabled:true,analysisConfigured:options.analysisConfigured,model:options.analysisModel});
    await registerMediaLifecycleRoutes(privateRoutes,async req=>{const s=await scoped(req);if(!s.media||!s.exports)throw new HttpError(503,'Media lifecycle is not configured.');return {media:s.media,exports:s.exports,persistence:s.persistence,channel:channel(req)};});
    privateRoutes.post('/onboard',async req=>{if(!options.hosted)throw new HttpError(503,'Hosted onboarding is disabled. Use the local connection tab.');return options.hosted.onboard(channel(req));});
    privateRoutes.post('/agent/revoke',async req=>{const link=await options.links.byChannel(channel(req));if(!link||!options.hosted)throw new HttpError(403,'Channel not linked for hosted capture.');await options.hosted.revoke(link.owner_id);return {revoked:true};});
    privateRoutes.get('/account',async req=>{const link=await options.links.byChannel(channel(req));let live:null|Awaited<ReturnType<TwitchApi['channel']>>=null,statusError:string|null=null;
      if(link&&options.api)try{live=await options.api.channel(link.twitch_user_id);}catch{statusError='Twitch live status is temporarily unavailable.';}
      return {linked:Boolean(link),channelId:channel(req),channel:link?{id:link.twitch_user_id,login:link.login,displayName:link.display_name,profileImageUrl:link.profile_image_url}:null,live,statusError,hostedEnabled:Boolean(options.hosted)};});
    privateRoutes.get('/broadcasts',async req=>({broadcasts:(await (await scoped(req)).persistence.broadcasts.all()).sort((a,b)=>b.createdAt.localeCompare(a.createdAt)).slice(0,100).map(b=>({id:b.id,title:b.title,readyChunkCount:b.chunks.length,scopeEndSeconds:Math.max(0,...b.chunks.map(c=>c.endSeconds)),sourceOffsetSeconds:b.sourceOffsetSeconds,createdAt:b.createdAt}))}));
    privateRoutes.get<{Params:{id:string}}>('/broadcasts/:id/status',async req=>{const s=await scoped(req),b=await s.persistence.broadcasts.get(req.params.id);if(!b)throw new HttpError(404,'Broadcast not found.');return {id:b.id,title:b.title,readyChunkCount:b.chunks.length,scopeEndSeconds:Math.max(0,...b.chunks.map(c=>c.endSeconds)),sourceOffsetSeconds:b.sourceOffsetSeconds,ingestion:b.ingestionId?await s.ingestions?.status(b.ingestionId):null};});
    privateRoutes.post<{Params:{id:string}}>('/broadcasts/:id/searches',async(req,reply)=>{const parsed=broadcastSearchSchema.extend({requestId:z.uuid()}).strict().safeParse(req.body);if(!parsed.success)throw new HttpError(400,'Provide requestId, question and supported search settings.');const scope=await scoped(req),source=await scope.persistence.broadcasts.get(req.params.id);if(!source)throw new HttpError(404,'Broadcast not found.');const {requestId,question,...settings}=parsed.data;return reply.code(201).send(view(await service(scope).start(source,requestId,question,settings)));});
    privateRoutes.get<{Params:{id:string}}>('/searches/:id',async req=>view(await service(await scoped(req)).get(req.params.id)));
    privateRoutes.post<{Params:{id:string}}>('/searches/:id/close',async req=>service(await scoped(req)).close(req.params.id));
    privateRoutes.post<{Params:{id:string}}>('/searches/:id/analyses',async req=>{const body=z.object({version:z.number().int().nonnegative()}).strict().safeParse(req.body);if(!body.success)throw new HttpError(400,'Provide the displayed version.');return view(await service(await scoped(req)).requestAnalyses(req.params.id,body.data.version));});
    privateRoutes.post<{Params:{id:string}}>('/searches/:id/recover',async(req,reply)=>{const body=z.object({version:z.number().int().nonnegative(),requestId:z.uuid()}).strict().safeParse(req.body);if(!body.success)throw new HttpError(400,'Provide the displayed version and requestId.');const result=await service(await scoped(req)).recover(req.params.id,body.data.version,body.data.requestId);return reply.code(result.phase==='recovering'?202:200).send(view(result));});
    privateRoutes.get<{Params:{id:string}}>('/broadcasts/:id/playback',async req=>{const scope=await scoped(req),source=await scope.persistence.broadcasts.get(req.params.id);if(!source)throw new HttpError(404,'Broadcast not found.');const range=z.object({startSeconds:z.coerce.number().finite().nonnegative(),endSeconds:z.coerce.number().finite().positive()}).strict().safeParse(req.query);if(!range.success)throw new HttpError(400,'Provide a valid playback range.');return signedPlayback(source,range.data.startSeconds,range.data.endSeconds,scope.storage);});
  },{prefix:'/extension'});
  if(options.hosted)await app.register(async routes=>options.hosted!.register(routes,options.provider),{prefix:'/agent'});
  const viewerBudgets=new Map<string,{since:number;count:number}>();
  const viewerLinks=new Map<string,{id:string;version:number;until:number;signed:{url:string;expiresAt:string}}>();
  app.get('/viewer/selection',async req=>{
    const header=req.headers.authorization;if(!header?.startsWith('Bearer '))throw new HttpError(401,'A Twitch Extension token is required.');
    const auth=verifyExtensionToken(header.slice(7),options.config.secrets,options.config.clientId),key=auth.channelId+':'+auth.opaqueId,now=Date.now();
    let budget=viewerBudgets.get(key);if(!budget||now-budget.since>60000){if(viewerBudgets.size>=10000){for(const [k,v] of viewerBudgets)if(now-v.since>60000)viewerBudgets.delete(k);if(viewerBudgets.size>=10000&&!viewerBudgets.has(key))throw new HttpError(503,'Viewer service busy.');}budget={since:now,count:0};viewerBudgets.set(key,budget);}
    if(++budget.count>60)throw new HttpError(429,'Refresh less frequently.');
    const link=await options.links.byChannel(auth.channelId);if(!link)return {version:0,selection:null};const scope=options.scope(link.owner_id);if(!scope.media||!scope.exports)return {version:0,selection:null};
    const publication=await scope.media.publication(auth.channelId),row=publication?.export_id?await scope.media.export(publication.export_id):undefined;
    if(!row?.ready_at||!row.storage||Date.parse(row.expires_at)<=now||!await scope.persistence.broadcasts.get(row.broadcast_id))return {version:publication?.version??0,selection:null};
    const cached=viewerLinks.get(auth.channelId);let signed=cached?.id===row.id&&cached.version===publication!.version&&cached.until>now?cached.signed:undefined;
    if(!signed){signed=await scope.exports.sign(row.storage,false,60);if(viewerLinks.size>=1000&&!viewerLinks.has(auth.channelId))viewerLinks.delete(viewerLinks.keys().next().value!);viewerLinks.set(auth.channelId,{id:row.id,version:publication!.version,until:now+40000,signed});}
    return {version:publication!.version,selection:{id:row.id,title:row.title,durationSeconds:(row.end_ms-row.start_ms)/1000,url:signed.url,expiresAt:signed.expiresAt}};
  });
  const root=resolve('dist/extension');if(existsSync(root))await app.register(fastifyStatic,{root,prefix:'/extension-ui/'});
  return app;
}
