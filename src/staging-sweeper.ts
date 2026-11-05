import {lstat,readdir,readFile,realpath,rm} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {z} from 'zod';
import type {SupabaseClient} from '@supabase/supabase-js';
import {lockStagedFile,StagedFileBusyError} from './staged-media.js';
const rowSchema=z.object({fileId:z.uuid(),state:z.enum(['orphan','completed','recoverable','keep']),updatedAt:z.string().nullable(),replicas:z.array(z.object({bucket:z.string(),path:z.string()}))});
export type StagedFileStatus=z.infer<typeof rowSchema>;
export function stagingCatalog(client:SupabaseClient){return {
  status:async(ids:string[])=>{const {data,error}=await client.rpc('rewindr_staged_file_status',{p_file_ids:ids});if(error)throw Error('Staging reference lookup failed');return z.array(rowSchema).parse(data);},
  replicaExists:async(replica:{bucket:string;path:string})=>{const {data,error}=await client.storage.from(replica.bucket).exists(replica.path);if(error)throw Error('Staging replica check failed');return data===true;}
};}
const running=new Set<string>();
// Run only from the single API process that owns DATA_DIR's api.lock.
export async function sweepStagedFiles(options:{root:string;status:(ids:string[])=>Promise<StagedFileStatus[]>;replicaExists:(replica:{bucket:string;path:string})=>Promise<boolean>;
  now?:number;orphanMs?:number;terminalMs?:number;apply?:boolean}){
  const root=await realpath(options.root),directory=join(root,'chunk-jobs'),now=options.now??Date.now();
  const orphanMs=options.orphanMs??24*3600_000,terminalMs=options.terminalMs??7*24*3600_000;
  if(orphanMs<3600_000||terminalMs<orphanMs)throw Error('Staging grace periods are too short');
  const summary={scanned:0,eligible:0,removed:0,retained:0,staleLocks:0};if(running.has(root))return summary;running.add(root);
  try{
    try{const info=await lstat(directory);if(!info.isDirectory()||info.isSymbolicLink()||resolve(await realpath(directory)).toLowerCase()!==resolve(directory).toLowerCase())throw Error('Invalid staging directory');}
    catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return summary;throw e;}
    const entries=await readdir(directory,{withFileTypes:true});
    // Workers never replace existing locks. One API sweeper can therefore reclaim
    // a dead process's old lock without racing another stale-lock reclaimer.
    if(options.apply!==false)for(const entry of entries){if(!/^[a-f0-9-]{36}\.mp4\.use\.(?:lock|\d+\.[a-f0-9-]{36}\.tmp)$/.test(entry.name)||!entry.isFile()||entry.isSymbolicLink())continue;
      const path=join(directory,entry.name),info=await lstat(path);if(now-info.mtimeMs<5*60_000)continue;
      let pid:number,released=false;try{const value=JSON.parse(await readFile(path,'utf8'));pid=Number(value.pid);released=value.released===true;}catch{continue;}if(!Number.isSafeInteger(pid)||pid<=0)continue;
      if(released){await rm(path,{force:true});summary.staleLocks++;continue;}
      try{process.kill(pid,0);}catch(e){if((e as NodeJS.ErrnoException).code==='ESRCH'){await rm(path,{force:true});summary.staleLocks++;}}
    }
    const files: {id:string;path:string;info:Awaited<ReturnType<typeof lstat>>}[]=[];
    for(const entry of entries){if(!entry.isFile()||entry.isSymbolicLink()||!z.uuid().safeParse(entry.name.slice(0,-4)).success||!entry.name.endsWith('.mp4'))continue;
      const path=join(directory,entry.name),info=await lstat(path);summary.scanned++;if(now-info.mtimeMs<orphanMs){summary.retained++;continue;}files.push({id:entry.name.slice(0,-4),path,info});}
    const eligible=(file:typeof files[number],row:StagedFileStatus|undefined)=>row&&row.state!=='keep'&&now-Math.max(Number(file.info.mtimeMs),row.updatedAt?Date.parse(row.updatedAt):0)>=(row.state==='recoverable'?terminalMs:orphanMs);
    for(let offset=0;offset<files.length;offset+=100){const batch=files.slice(offset,offset+100),rows=await options.status(batch.map(f=>f.id));
      for(const file of batch){const row=rows.find(r=>r.fileId===file.id);if(!eligible(file,row)){summary.retained++;continue;}
        let release:(()=>Promise<void>)|undefined;
        try{release=await lockStagedFile(root,file.id);const current=(await options.status([file.id])).find(r=>r.fileId===file.id);
          if(!eligible(file,current)){summary.retained++;continue;}
          let replicas=true;for(const replica of current!.replicas)if(!await options.replicaExists(replica)){replicas=false;break;}
          if(current!.state!=='orphan'&&(!current!.replicas.length||!replicas)){summary.retained++;continue;}
          const fresh=await lstat(file.path);if(!fresh.isFile()||fresh.isSymbolicLink()||fresh.ino!==file.info.ino||fresh.size!==file.info.size||fresh.mtimeMs!==file.info.mtimeMs){summary.retained++;continue;}
          summary.eligible++;if(options.apply!==false){await rm(file.path);summary.removed++;}
        }catch(e){if(!(e instanceof StagedFileBusyError)&&(e as NodeJS.ErrnoException).code!=='ENOENT')throw e;summary.retained++;}
        finally{await release?.();}
      }
    }
    return summary;
  }finally{running.delete(root);}
}
export function startStagingSweeper(root:string,client:SupabaseClient,log:(summary:unknown)=>void=console.log){
  let stopped=false,pending:Promise<void>|undefined;const catalog=stagingCatalog(client);
  const run=()=>{if(stopped||pending)return;pending=sweepStagedFiles({root,...catalog}).then(summary=>{if(summary.removed||summary.staleLocks)log({stagingSweep:summary});}).catch(()=>console.warn('Staging cleanup unavailable; files retained and the next sweep will retry.')).finally(()=>{pending=undefined;});};
  run();const timer=setInterval(run,15*60_000);timer.unref();return async()=>{stopped=true;clearInterval(timer);await pending;};
}
