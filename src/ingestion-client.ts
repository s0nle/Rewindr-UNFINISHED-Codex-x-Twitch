import {openAsBlob} from 'node:fs';
import {setTimeout as wait} from 'node:timers/promises';
import {z} from 'zod';
import type {CreateIngestion,CreateLiveIngestion} from './ingestion-types.js';
import type {ClosedChunk} from './segmenter.js';
export class IngestionInputError extends Error {}
export function captureBinding(saved:string|undefined,next:string,used:boolean){if(saved&&saved!==next||!saved&&used&&next!=='local')throw new IngestionInputError('This workspace belongs to a different capture backend/channel. Keep it and start a fresh workspace for the new destination.');return next;}
const acceptedSchema=z.object({jobId:z.uuid(),chunkId:z.uuid()});
const jobView=z.object({jobId:z.uuid(),state:z.enum(['queued','running','retrying','completed','failed','needs_review'])});
export class IngestionClient {
  private hosted?:{origin:string;token:string};
  constructor(readonly port:number,private signal?:AbortSignal,remote={url:process.env.REWINDR_API_URL,token:process.env.REWINDR_AGENT_TOKEN}){if(!Number.isInteger(port)||port<1||port>65535)throw new IngestionInputError('Invalid API port.');if(remote.url||remote.token){try{const url=new URL(remote.url!);if(url.protocol!=='https:'||url.username||url.password||url.pathname!=='/'||url.search||url.hash||!/^rwa_[A-Za-z0-9_-]{43}$/.test(remote.token||''))throw Error();this.hosted={origin:url.origin,token:remote.token!};}catch{throw new IngestionInputError('Hosted capture requires an HTTPS REWINDR_API_URL origin and valid REWINDR_AGENT_TOKEN.');}}}
  private async request<T>(path:string,init:()=>Promise<RequestInit>,schema:z.ZodType<T>,timeout=20000):Promise<T> {
    for(let attempt=0;;attempt++) {
      this.signal?.throwIfAborted();
      try {
        const request=await init();const response=await fetch(this.hosted?`${this.hosted.origin}/agent${path}`:`http://127.0.0.1:${this.port}${path}`,{...request,headers:{...request.headers,...(this.hosted?{Authorization:'Bearer '+this.hosted.token}:{})},redirect:'error',signal:AbortSignal.any([AbortSignal.timeout(timeout),...(this.signal?[this.signal]:[])])});
        if(!response.ok){await response.body?.cancel();if([429,503].includes(response.status)&&attempt<2){await wait(500*2**attempt,undefined,{signal:this.signal});continue;}
          throw new IngestionInputError(`Backend operation failed (${response.status}) at ${path}. Check the index, source identity and backend status.`);}
        return schema.parse(await response.json());
      }catch(error){if(error instanceof IngestionInputError||this.signal?.aborted||attempt>=2)throw error;await wait(500*2**attempt,undefined,{signal:this.signal});}
    }
  }
  create(input:CreateIngestion|CreateLiveIngestion){return this.request('/ingestions',async()=>({method:'POST',headers:{'content-type':'application/json','x-creator-authorized':'true'},body:JSON.stringify(input)}),z.object({ingestionId:z.uuid(),broadcastId:z.uuid()}));}
  async binding(){if(!this.hosted)return 'local';const account=await this.request('/account',async()=>({}),z.object({ownerId:z.uuid()}));return this.hosted.origin+':'+account.ownerId;}
  accept(id:string,chunk:ClosedChunk){return this.request(`/ingestions/${id}/chunks/${chunk.ordinal}`,async()=>{
    const form=new FormData();form.set('chunk',await openAsBlob(chunk.path,{type:'video/mp4'}),chunk.filename);
    return {method:'POST',headers:{'x-creator-authorized':'true','x-chunk-start-seconds':String(chunk.startSeconds),'x-chunk-end-seconds':String(chunk.endSeconds)},body:form};
  },acceptedSchema,180000);}
  job(id:string){return this.request(`/jobs/${id}`,async()=>({}),jobView);}
  seal(id:string){return this.request(`/ingestions/${id}/complete`,async()=>({method:'POST',headers:{'x-creator-authorized':'true'}}),z.object({status:z.string(),searchableThroughSeconds:z.number()}));}
  status(id:string){return this.request(`/ingestions/${id}`,async()=>({}),z.object({status:z.string(),searchableThroughSeconds:z.number()}));}
  liveStatus(id:string){return this.request(`/ingestions/${id}`,async()=>({}),z.object({mode:z.literal('live'),sealedAt:z.string().nullable(),status:z.string(),latestSearchableSeconds:z.number(),chunks:z.array(z.object({ordinal:z.number(),stored:z.boolean(),jobId:z.uuid(),jobState:z.string()}))}));}
}
