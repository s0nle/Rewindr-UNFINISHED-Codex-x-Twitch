import type {FastifyInstance,FastifyRequest} from 'fastify';
import {z} from 'zod';
import type {MediaRepository,ExportRow} from './media-store.js';
import type {ExportStore} from './export-storage.js';
import type {Persistence} from './persistence.js';
import {playbackPlan} from './playback.js';
import {HttpError} from './http-errors.js';
export interface MediaScope{media:MediaRepository;exports:ExportStore;persistence:Persistence;channel?:string}
export async function exportView(row:ExportRow,scope:MediaScope,sign=true){
  if(sign&&row.ready_at&&!await scope.persistence.broadcasts.get(row.broadcast_id))throw new HttpError(410,'This archive is being removed.');
  const task=row.ready_at?undefined:await scope.media.task(row.id),expired=Date.parse(row.expires_at)<=Date.now();
  return {id:row.id,broadcastId:row.broadcast_id,title:row.title,startSeconds:row.start_ms/1000,endSeconds:row.end_ms/1000,expiresAt:row.expires_at,state:expired?'expired':row.ready_at?'ready':task?.state??'failed',error:task?.error??null,...(sign&&!expired&&row.ready_at&&row.storage?{preview:await scope.exports.sign(row.storage),download:await scope.exports.sign(row.storage,true)}:{})};
}
export async function registerMediaLifecycleRoutes(app:FastifyInstance,scopeFor:(req:FastifyRequest)=>Promise<MediaScope>){
  app.get('/exports',async req=>{const scope=await scopeFor(req);return {exports:await Promise.all((await scope.media.exports()).slice(0,30).map(row=>exportView(row,scope,false)))};});
  app.post('/exports',async(req,reply)=>{
    const body=z.object({requestId:z.uuid(),broadcastId:z.uuid(),title:z.string().trim().min(1).max(100),startSeconds:z.number().finite().nonnegative(),endSeconds:z.number().finite().positive()}).strict().safeParse(req.body);
    if(!body.success)throw new HttpError(400,'Provide a title, broadcast and clip boundaries.');const v=body.data,start=Math.round(v.startSeconds*1000),end=Math.round(v.endSeconds*1000);
    if(end-start<1000||end-start>180000)throw new HttpError(400,'Clips must last 1–180 seconds.');
    const scope=await scopeFor(req),source=await scope.persistence.broadcasts.get(v.broadcastId);if(!source)throw new HttpError(404,'Broadcast not found.');const plan=playbackPlan(source,start/1000,end/1000);
    if(!plan||plan.segments.length>8||plan.segments.reduce((sum,s)=>sum+s.chunk.storage!.bytes,0)>250000000)throw new HttpError(409,'The range contains missing footage or exceeds the eight-section/250 MB source limit.');
    const row=await scope.media.begin(v.requestId,v.broadcastId,v.title,start,end);return reply.code(202).send(await exportView(row,scope));
  });
  app.get<{Params:{id:string}}>('/exports/:id',async req=>{const scope=await scopeFor(req),row=await scope.media.export(req.params.id);if(!row)throw new HttpError(404,'Export not found.');return exportView(row,scope);});
  app.post<{Params:{id:string}}>('/media-tasks/:id/retry',async req=>{const scope=await scopeFor(req);await scope.media.retry(req.params.id);return {queued:true};});
  app.get('/media-tasks',async req=>{const scope=await scopeFor(req);return {tasks:(await scope.media.tasks()).map(t=>({id:t.id,kind:t.kind,state:t.state,error:t.error,attempts:t.attempts,createdAt:t.created_at,broadcastId:t.input.broadcastId??null}))};});
  app.get('/publication',async req=>{const scope=await scopeFor(req);if(!scope.channel)return {version:0,exportId:null,connected:false};const value=await scope.media.publication(scope.channel);return {version:value?.version??0,exportId:value?.export_id??null,connected:true};});
  app.post('/publication',async req=>{const body=z.object({exportId:z.uuid().nullable(),version:z.number().int().nonnegative()}).strict().safeParse(req.body);if(!body.success)throw new HttpError(400,'Provide the current selection version and an export ID or null.');const scope=await scopeFor(req);if(!scope.channel)throw new HttpError(409,'Connect a Twitch channel before publishing.');const value=await scope.media.publish(scope.channel,body.data.exportId,body.data.version);return {version:value.version,exportId:value.export_id,connected:true};});
  app.get('/usage',async req=>({usage:await(await scopeFor(req)).media.usage(),limits:{search:100,analysis:60,export:20,ingestion:4,upload_bytes:4000000000},reset:'00:00 UTC'}));
  app.post<{Params:{id:string}}>('/broadcasts/:id/delete',async(req,reply)=>{const body=z.object({requestId:z.uuid(),confirmTitle:z.string().min(1).max(200)}).strict().safeParse(req.body);if(!body.success)throw new HttpError(400,'Provide requestId and confirmTitle.');const scope=await scopeFor(req);const task=await scope.media.purge(body.data.requestId,req.params.id,body.data.confirmTitle);return reply.code(202).send({taskId:task.id,state:task.state,providerAssetsRetained:true,localOriginalsRetained:true});});
}
