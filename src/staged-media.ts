import {join} from 'node:path';
import {z} from 'zod';
import {randomUUID} from 'node:crypto';
import {mkdir,open,readFile,writeFile,rm,link} from 'node:fs/promises';
import {HttpError} from './http-errors.js';
export function chunkJobFile(root:string,fileId:string){return join(root,'chunk-jobs',`${z.uuid().parse(fileId)}.mp4`);}
export class StagedFileBusyError extends HttpError {constructor(){super(503,'Staged media is in use; retry later.');}}
export async function lockStagedFile(root:string,fileId:string){
  const path=chunkJobFile(root,fileId)+'.use.lock',token=randomUUID();await mkdir(join(root,'chunk-jobs'),{recursive:true});
  const temporary=chunkJobFile(root,fileId)+`.use.${process.pid}.${token}.tmp`;
  try{const handle=await open(temporary,'wx');try{await handle.writeFile(JSON.stringify({pid:process.pid,token}));}finally{await handle.close();}
    // Publish fully written lock metadata atomically, without replacing another lock.
    await link(temporary,path);
    return async()=>{const value=JSON.parse(await readFile(path,'utf8'));if(value.token!==token)throw Error('Staged-file lock changed');await writeFile(path,JSON.stringify({...value,released:true}),{mode:0o600});await rm(path);};
  }catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')throw new StagedFileBusyError();throw error;}
  finally{await rm(temporary,{force:true}).catch(()=>console.warn('Staged-file lock left temporary metadata for cleanup.'));}
}
