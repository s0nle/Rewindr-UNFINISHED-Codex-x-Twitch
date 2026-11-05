import {readdir,readFile,lstat,realpath,rm} from 'node:fs/promises';
import {resolve,join,sep} from 'node:path';
import {z} from 'zod';
import type {MediaRepository} from './media-store.js';
const marker=z.object({taskId:z.uuid(),owner:z.uuid(),leaseToken:z.uuid()});
export async function sweepMediaWork(root:string,repository:(owner:string)=>MediaRepository,apply=false,now=Date.now()){
  const parent=resolve(root,'media-work'),report={eligible:0,removed:0,preserved:0};let dirs;try{if((await lstat(parent)).isSymbolicLink()||resolve(await realpath(parent))!==parent)throw Error('Unsafe media-work root');dirs=await readdir(parent,{withFileTypes:true});}catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return report;throw e;}
  for(const dir of dirs){if(!dir.isDirectory()||!/^([a-f0-9-]{36})-[A-Za-z0-9]+$/.test(dir.name))continue;const path=join(parent,dir.name);
    try{const info=await lstat(path);if(info.isSymbolicLink()||!resolve(await realpath(path)).startsWith(parent+sep)||now-info.mtimeMs<86400000){report.preserved++;continue;}
      const meta=marker.parse(JSON.parse(await readFile(join(path,'task.json'),'utf8')));if(!dir.name.startsWith(meta.taskId+'-'))throw Error();const task=await repository(meta.owner).task(meta.taskId);
      if(task&&task.owner_id!==meta.owner)throw Error();if(task?.state==='running'&&Date.parse(task.lease_until||'')>now){report.preserved++;continue;}
      report.eligible++;if(apply){await rm(path,{recursive:true,force:true});report.removed++;}
    }catch{report.preserved++;}
  }return report;
}
