import {mkdir,readFile,open,rm} from 'node:fs/promises';
import {resolve} from 'node:path';
export async function acquireIngestionLock(root:string){
  await mkdir(root,{recursive:true});const path=resolve(root,'api.lock');
  try{const pid=Number(await readFile(path,'utf8'));if(!Number.isSafeInteger(pid)||pid<=0)throw Error('Invalid ingestion lock; inspect it before removal.');let alive=true;try{process.kill(pid,0);}catch(e){if((e as NodeJS.ErrnoException).code==='ESRCH')alive=false;else throw e;}if(alive)throw Error('Another ingestion API owns this DATA_DIR. Stop the local API before enabling hosted ingestion.');await rm(path);}catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;}
  const file=await open(path,'wx',0o600);try{await file.writeFile(String(process.pid));}finally{await file.close();}
  return async()=>{if(await readFile(path,'utf8').catch(()=>null)===String(process.pid))await rm(path,{force:true});};
}
