import type { FastifyInstance } from 'fastify';
import type { JobQueue } from './jobs.js';
import { visibleJob } from './jobs.js';
import { HttpError } from './http-errors.js';
export async function registerJobRoutes(app:FastifyInstance,queue?:JobQueue) {
  app.get<{Params:{id:string}}>('/jobs/:id',async req=>{
    if(!queue)throw new HttpError(503,'Durable jobs require Supabase mode.');
    const job=await queue.get(req.params.id);if(!job)throw new HttpError(404,'Job not found.');return visibleJob(job);
  });
  app.post<{Params:{id:string}}>('/jobs/:id/retry',async req=>{
    if(!queue)throw new HttpError(503,'Durable jobs require Supabase mode.');
    if(req.headers['x-creator-authorized']!=='true')throw new HttpError(400,'Send X-Creator-Authorized: true before retrying.');
    return visibleJob(await queue.retry(req.params.id));
  });
}
