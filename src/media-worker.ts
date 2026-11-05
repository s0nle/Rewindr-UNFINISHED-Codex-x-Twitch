import {createHash} from 'node:crypto';
import {createReadStream} from 'node:fs';
import {mkdir,mkdtemp,rm,stat,writeFile} from 'node:fs/promises';
import {join,resolve,sep} from 'node:path';
import {setTimeout as wait} from 'node:timers/promises';
import {z} from 'zod';
import type {MediaRepository,MediaTask,ExportStorage} from './media-store.js';
import type {ExportStore} from './export-storage.js';
import type {ChunkStorage} from './chunk-storage.js';
import {playbackPlan} from './playback.js';
import {encodeStoredClip} from './export-encoder.js';
import {HttpError} from './http-errors.js';
import {LeaseLostError} from './jobs.js';
import {sweepMediaWork} from './media-sweeper.js';
export class MediaWorker{
  private controller=new AbortController();
  constructor(private options:{root:string;queue:MediaRepository;repository:(owner:string)=>MediaRepository;chunks:(owner:string)=>ChunkStorage;exports:(owner:string)=>ExportStore;pollMs?:number}){}
  stop(){this.controller.abort();}
  async run(){let nextSweep=0;while(!this.controller.signal.aborted){try{if(Date.now()>=nextSweep){nextSweep=Date.now()+1800000;await sweepMediaWork(this.options.root,this.options.repository,true);}if(await this.runOnce())continue;}catch{console.error('Media queue unavailable; retrying shortly.');}await wait(this.options.pollMs??3000,undefined,{signal:this.controller.signal}).catch(()=>{});}}
  async runOnce(){
    if(this.controller.signal.aborted)return false;const task=await this.options.queue.claim();if(!task||this.controller.signal.aborted)return false;
    const repository=this.options.repository(task.owner_id),storage=this.options.exports(task.owner_id),abort=new AbortController();
    const onStop=()=>abort.abort();this.controller.signal.addEventListener('abort',onStop,{once:true});
    let renewal:Promise<void>|undefined,lost=false,safeUntil=Date.parse(task.lease_until||'');
    const renew=async()=>{for(let attempt=0;;attempt++)try{await repository.guard(task);safeUntil=Date.now()+295000;return;}catch(e){if(e instanceof HttpError&&e.statusCode===503&&attempt<2&&Date.now()<safeUntil-15000){await wait(250*2**attempt);continue;}lost=true;abort.abort();throw e;}};
    const guard=()=>{if(lost)return Promise.reject(new LeaseLostError());return renewal??(renewal=renew().finally(()=>{renewal=undefined;}));};
    const heartbeat=setInterval(()=>{void guard().catch(()=>{});},30000);heartbeat.unref();
    try{
      await guard();
      if(task.kind==='export')await this.export(task,repository,storage,abort.signal,guard);
      else{
        const input=z.object({objects:z.array(z.object({bucket:z.string(),path:z.string()}))}).parse(task.input);
        for(let i=task.completed_objects;i<input.objects.length;i++){const item=input.objects[i]!;abort.signal.throwIfAborted();await guard();await storage.remove(item.bucket,item.path);await repository.progress(task,i+1);}
      }
      await guard();await repository.finish(task);
    }catch(e){
      if(!lost&&!this.controller.signal.aborted&&!(e instanceof LeaseLostError))await repository.finish(task,e instanceof HttpError&&e.statusCode<500?e.message:'Media operation failed; source and storage checkpoints retained.',e instanceof HttpError&&[409,413,415,422].includes(e.statusCode)).catch(()=>console.warn('Media outcome unconfirmed; lease redelivery will resume it.'));
    }finally{clearInterval(heartbeat);this.controller.signal.removeEventListener('abort',onStop);}
    return true;
  }
  private async export(task:MediaTask,repository:MediaRepository,storage:ExportStore,signal:AbortSignal,guard:()=>Promise<void>){
    const row=await repository.export(task.id);if(!row)throw new HttpError(409,'Export source was removed.');if(Date.parse(row.expires_at)<=Date.now())throw new HttpError(422,'Export expired before processing.');
    if(row.storage&&await storage.exists(row.storage))return;
    const plan=playbackPlan(row.snapshot,row.start_ms/1000,row.end_ms/1000);
    if(!plan||plan.segments.length>8||plan.segments.reduce((sum,s)=>sum+s.chunk.storage!.bytes,0)>250000000)throw new HttpError(422,'Clip source is missing, has a gap, or exceeds export limits.');
    const parent=resolve(this.options.root,'media-work');await mkdir(parent,{recursive:true});const directory=await mkdtemp(join(parent,task.id+'-'));
    try{
      await writeFile(join(directory,'task.json'),JSON.stringify({taskId:task.id,owner:task.owner_id,leaseToken:task.lease_token}),{flag:'wx'});
      const chunks=this.options.chunks(task.owner_id);if(!chunks.download)throw new HttpError(503,'Stored source downloader is unavailable.');
      const parts=[];
      for(const [i,segment] of plan.segments.entries()){
        signal.throwIfAborted();await guard();const file=join(directory,`source_${i}.mp4`);
        await chunks.download(row.broadcast_id,segment.chunk.storageKey||segment.chunk.indexedAssetId,segment.chunk.storage!,file);
        parts.push({file,start:segment.startSeconds,end:segment.endSeconds});
      }
      const file=await encodeStoredClip(parts,directory,signal,guard),hash=createHash('sha256');for await(const bytes of createReadStream(file))hash.update(bytes);
      const reference:ExportStorage={bucket:storage.bucket,path:`${task.owner_id}/exports/${task.id}.mp4`,sha256:hash.digest('hex'),bytes:(await stat(file)).size};
      await guard();await repository.checkpoint(task,reference);await guard();await storage.upload(reference,file);
    }finally{
      if(!resolve(directory).startsWith(parent+sep))throw Error('Unsafe media workspace');await rm(directory,{recursive:true,force:true});
    }
  }
}
