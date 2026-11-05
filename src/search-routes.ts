import type {FastifyInstance} from 'fastify';
import {z} from 'zod';
import {broadcastSearchSchema} from './chunk-search.js';
import type {BroadcastRepository} from './persistence.js';
import type {SearchService} from './search-service.js';
import {HttpError} from './http-errors.js';
const startSchema=broadcastSearchSchema.extend({requestId:z.uuid()}).strict();
export async function registerSearchRoutes(app:FastifyInstance,options:{broadcasts:BroadcastRepository;service?:SearchService}) {
  const service=()=>{if(!options.service)throw new HttpError(503,'Search sessions require Supabase mode and the search/analysis migration.');return options.service;};
  app.get('/broadcasts',async()=>({broadcasts:(await options.broadcasts.all()).sort((a,b)=>b.createdAt.localeCompare(a.createdAt)).slice(0,100).map(b=>({id:b.id,title:b.title,readyChunkCount:b.chunks.length,
    scopeEndSeconds:Math.max(0,...b.chunks.map(c=>c.endSeconds)),sourceOffsetSeconds:b.sourceOffsetSeconds,createdAt:b.createdAt}))}));
  app.post<{Params:{id:string}}>('/broadcasts/:id/searches',async(req,reply)=>{
    const input=startSchema.safeParse(req.body);if(!input.success)throw new HttpError(400,'Provide requestId, question and supported search settings.');
    const source=await options.broadcasts.get(req.params.id);if(!source)throw new HttpError(404,'Broadcast not found.');
    const {requestId,question,...settings}=input.data;return reply.code(201).send(await service().start(source,requestId,question,settings));
  });
  app.get<{Params:{id:string}}>('/searches/:id',req=>service().get(req.params.id));
  app.post<{Params:{id:string}}>('/searches/:id/close',req=>service().close(req.params.id));
  app.post<{Params:{id:string}}>('/searches/:id/recover',async(req,reply)=>{
    const body=z.object({version:z.number().int().nonnegative(),requestId:z.uuid()}).strict().safeParse(req.body);if(!body.success)throw new HttpError(400,'Provide the displayed version and a recovery requestId.');
    const view=await service().recover(req.params.id,body.data.version,body.data.requestId);return reply.code(view.phase==='recovering'?202:200).send(view);
  });
  app.post<{Params:{id:string}}>('/searches/:id/analyses',async req=>{
    const body=z.object({version:z.number().int().nonnegative()}).strict().safeParse(req.body);if(!body.success)throw new HttpError(400,'Provide the displayed search version.');
    return service().requestAnalyses(req.params.id,body.data.version);
  });
}
