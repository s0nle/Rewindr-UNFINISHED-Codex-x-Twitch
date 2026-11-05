import {createHash,randomUUID} from 'node:crypto';
import {mkdir,open,readFile,realpath,rm,stat} from 'node:fs/promises';
import {basename,join,resolve} from 'node:path';
import {setTimeout as wait} from 'node:timers/promises';
import {z} from 'zod';
import {inspectIngestionSource} from './media.js';
import {atomicJsonOrText,splitRecording} from './segmenter.js';
import {IngestionClient,IngestionInputError,captureBinding} from './ingestion-client.js';
const sessionSchema=z.object({version:z.literal(1),id:z.uuid(),source:z.string(),fingerprint:z.string(),duration:z.number().min(4).max(43200),
  title:z.string().min(1).max(200),indexId:z.string().optional(),backendKey:z.string().optional(),offset:z.number().finite().nonnegative(),generated:z.boolean(),
  chunks:z.record(z.string(),z.object({sha256:z.string(),jobId:z.uuid().optional()}))});
export type IngestionSession=z.infer<typeof sessionSchema>;
async function fingerprint(source:string){const info=await stat(source);if(!info.isFile())throw new IngestionInputError('Source must be a regular video file.');return createHash('sha256').update(JSON.stringify({path:source,size:info.size,mtimeMs:info.mtimeMs})).digest('hex');}
export async function runRecordedIngestion(options:{source?:string;resume?:string;workspace?:string;dataDir?:string;title?:string;indexId?:string;offset?:number;
  prepareOnly?:boolean;creatorAuthorized?:boolean;port?:number;signal?:AbortSignal;maxPending?:number;pollMs?:number;jobTimeoutMs?:number;log?:(message:string)=>void}) {
  const log=options.log??console.log;
  if(!options.prepareOnly&&!options.creatorAuthorized)throw new IngestionInputError('Pass --creator-authorized before uploading or indexing footage.');
  let directory:string;let session:IngestionSession;
  if(options.resume) {
    directory=await realpath(resolve(options.resume));session=sessionSchema.parse(JSON.parse(await readFile(join(directory,'session.json'),'utf8')));
    if(options.source||options.workspace||options.title||options.offset!==undefined)throw new IngestionInputError('Resume uses saved source/title/offset; do not override them.');
    if(await fingerprint(session.source)!==session.fingerprint)throw new IngestionInputError('The original source changed. Keep this workspace and start a new ingestion.');
    if(options.indexId&&session.indexId&&options.indexId!==session.indexId)throw new IngestionInputError('Resume cannot change an existing index.');
    if(options.indexId)session.indexId=options.indexId;
  } else {
    if(!options.source)throw new IngestionInputError('Provide --file or --resume.');
    const source=await realpath(resolve(options.source));const media=await inspectIngestionSource(source);const id=randomUUID();
    directory=resolve(options.workspace||join(options.dataDir||'.local/rewindr','ingestion-workspaces',id));
    // A new run owns a fresh directory; it cannot overwrite an unrelated folder.
    await mkdir(directory,{recursive:false}).catch(async error=>{
      if((error as NodeJS.ErrnoException).code==='ENOENT'){await mkdir(resolve(directory,'..'),{recursive:true});await mkdir(directory);return;}
      throw new IngestionInputError('Workspace already exists or is unavailable. Use --resume for an existing ingestion.');
    });
    session={version:1,id,source,fingerprint:await fingerprint(source),duration:media.durationSeconds,title:(options.title||basename(source)).slice(0,200),
      indexId:options.indexId,offset:options.offset??0,generated:false,chunks:{}};
    sessionSchema.parse(session);await atomicJsonOrText(join(directory,'session.json'),JSON.stringify(session,null,2));
  }
  log(`Workspace: ${directory}`);
  const lockPath=join(directory,'run.lock');
  try{const pid=Number(await readFile(lockPath,'utf8'));if(!Number.isSafeInteger(pid)||pid<=0)throw new IngestionInputError('Invalid workspace lock; inspect it before removal.');let live=true;try{process.kill(pid,0);}catch(error){if((error as NodeJS.ErrnoException).code==='ESRCH')live=false;else throw error;}
    if(live)throw new IngestionInputError('Another ingestion process holds this workspace.');await rm(lockPath);
  }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
  const lock=await open(lockPath,'wx');await lock.writeFile(String(process.pid));await lock.close();
  const save=()=>atomicJsonOrText(join(directory,'session.json'),JSON.stringify(session,null,2));
  try {
    if(!options.prepareOnly&&!session.indexId)throw new IngestionInputError('Provide --index-id or set TWELVELABS_INDEX_ID.');
    const client=options.prepareOnly?undefined:new IngestionClient(options.port??3000,options.signal);
    if(client){session.backendKey=captureBinding(session.backendKey,await client.binding(),Object.values(session.chunks).some(c=>Boolean(c.jobId)));await save();await client.create({id:session.id,title:session.title,indexId:session.indexId!,sourceDurationSeconds:session.duration,sourceOffsetSeconds:session.offset,sourceFingerprint:session.fingerprint});}
    const pending=new Set<string>();
    const drainOne=async()=>{
      const jobId=pending.values().next().value;if(!jobId||!client)return;
      const deadline=Date.now()+(options.jobTimeoutMs??10*60*1000);
      for(;;) {
        options.signal?.throwIfAborted();const job=await client.job(jobId);
        if(job.state==='completed'){pending.delete(jobId);return;}
        if(job.state==='failed'||job.state==='needs_review')throw new IngestionInputError(`Job ${jobId} stopped (${job.state}). Inspect /jobs/${jobId} before resuming.`);
        if(Date.now()>deadline)throw new IngestionInputError(`Job ${jobId} is still pending. Start/check the worker, then resume this workspace.`);
        await wait(options.pollMs??1000,undefined,{signal:options.signal});
      }
    };
    await splitRecording({source:session.source,directory:join(directory,'chunks'),durationSeconds:session.duration,signal:options.signal,onChunk:async chunk=>{
      const previous=session.chunks[chunk.filename];
      if(previous&&previous.sha256!==chunk.sha256)throw new IngestionInputError(`${chunk.filename} changed. Restore its original bytes before resuming.`);
      session.chunks[chunk.filename]={sha256:chunk.sha256,jobId:previous?.jobId};await save();
      if(client) {
        let jobId=previous?.jobId;
        if(jobId){const existing=await client.job(jobId);if(existing.state==='needs_review')throw new IngestionInputError(`Job ${jobId} requires provider reconciliation.`);
          if(existing.state==='completed'){log(`${chunk.filename}: already searchable.`);return;}
          if(existing.state==='failed')jobId=undefined;
        }
        if(!jobId){jobId=(await client.accept(session.id,chunk)).jobId;session.chunks[chunk.filename]!.jobId=jobId;await save();}
        pending.add(jobId);log(`${chunk.filename}: closed and queued (${chunk.startSeconds}–${chunk.endSeconds}s).`);
        if(pending.size>=(options.maxPending??4))await drainOne();
      } else log(`${chunk.filename}: prepared (${chunk.startSeconds}–${chunk.endSeconds}s).`);
    }});
    session.generated=true;await save();
    if(client){await client.seal(session.id);while(pending.size)await drainOne();const status=await client.status(session.id);
      log(`Ingestion ${session.id}: ${status.status}; searchable through ${status.searchableThroughSeconds}s. Search: /broadcasts/${session.id}/search`);
    }else log('Preparation complete. No backend, Storage or TwelveLabs requests were made. Resume this workspace with --creator-authorized to ingest it.');
    return {workspace:directory,session};
  } finally {await rm(lockPath,{force:true});}
}
