import {createHash,randomUUID} from 'node:crypto';
import {copyFile,mkdir,open,readFile,readdir,realpath,rm,stat} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {setTimeout as wait} from 'node:timers/promises';
import {z} from 'zod';
import {captureLiveAttempt,closedLiveSegments,type LiveInput,type LiveSegment} from './live-segmenter.js';
import {atomicJsonOrText,fileIdentity,type ClosedChunk} from './segmenter.js';
import {probeChunkMedia} from './media.js';
import {IngestionClient,IngestionInputError,captureBinding} from './ingestion-client.js';
import {createSessionWriter} from './session-writer.js';
import {activeCaptureClock} from './live-clock.js';
const pieceSchema=z.object({ordinal:z.number().int().min(0).max(719),filename:z.string().regex(/^chunk_\d{4}\.mp4$/),startSeconds:z.number().nonnegative(),endSeconds:z.number().positive(),
  bytes:z.number().positive(),sha256:z.string().regex(/^[a-f0-9]{64}$/),jobId:z.uuid().optional(),pruned:z.boolean().default(false)});
const sessionSchema=z.object({version:z.literal(1),id:z.uuid(),title:z.string().min(1).max(200),indexId:z.string().min(1).max(200),offset:z.number().nonnegative(),fingerprint:z.string(),
  originMs:z.number(),backendKey:z.string().optional(),maxSeconds:z.number().min(5).max(43200),kind:z.enum(['rtmp','hls']),rtmpPort:z.number().int().min(1024).max(65535),inputEnv:z.string().optional(),
  phase:z.enum(['capturing','interrupted','stopped','sealed']),ended:z.boolean(),activeSeconds:z.number().finite().nonnegative().optional(),inputLimit:z.enum(['chunk_limit','timeline_limit']).optional(),
  attempts:z.array(z.object({directory:z.string().regex(/^attempt_\d{4}$/),startedMs:z.number(),consumed:z.number().int().nonnegative(),base:z.number().nonnegative().optional()})),
  chunks:z.array(pieceSchema).max(720),skipped:z.array(z.object({startSeconds:z.number(),endSeconds:z.number(),reason:z.literal('under_four_seconds')})),
  quarantined:z.array(z.object({attempt:z.string().regex(/^attempt_\d{4}$/),filename:z.string().regex(/^raw_\d{6}\.mp4$/),
    startSeconds:z.number().finite().nonnegative(),endSeconds:z.number().finite().positive(),reason:z.enum(['invalid_media','oversize','missing'])})).default([])});
export type LiveCaptureSession=z.infer<typeof sessionSchema>;
async function spoolSize(directory:string):Promise<number>{let total=0;for(const item of await readdir(directory,{withFileTypes:true})){const path=join(directory,item.name);if(item.isDirectory())total+=await spoolSize(path);else if(item.name.endsWith('.mp4')){try{total+=(await stat(path)).size;}catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;}}}return total;}
export async function runLiveCapture(options:{indexId?:string;title?:string;workspace?:string;resume?:string;finish?:boolean;inputEnv?:string;rtmpPort?:number;maxSeconds?:number;offset?:number;
  creatorAuthorized?:boolean;signal?:AbortSignal;port?:number;dataDir?:string;maxSpoolBytes?:number;keepLocal?:boolean;log?:(message:string)=>void;segmentSeconds?:number;maxReconnects?:number;pollMs?:number}){
  if(!options.creatorAuthorized)throw new IngestionInputError('Pass --creator-authorized to capture, upload and index this stream.');
  const log=options.log??console.log,maxSpool=options.maxSpoolBytes??1_000_000_000;
  if(!Number.isSafeInteger(maxSpool)||maxSpool<50_000_000||maxSpool>10_000_000_000)throw new IngestionInputError('Spool budget must be 50 MB–10 GB.');
  let directory:string,session:LiveCaptureSession;
  if(options.resume){directory=await realpath(resolve(options.resume));session=sessionSchema.parse(JSON.parse(await readFile(join(directory,'live-session.json'),'utf8')));
    if(options.workspace||options.indexId||options.title||options.maxSeconds!==undefined||options.offset!==undefined||options.inputEnv||options.rtmpPort!==undefined)throw new IngestionInputError('Resume uses the saved source configuration; only runtime flags may be changed.');
  }else{
    if(options.finish||!options.indexId)throw new IngestionInputError('Provide --index-id (or TWELVELABS_INDEX_ID); --finish requires --resume.');
    const id=randomUUID();directory=resolve(options.workspace||join(options.dataDir||'.local/rewindr','live-workspaces',id));await mkdir(resolve(directory,'..'),{recursive:true});
    await mkdir(directory).catch(()=>{throw new IngestionInputError('Workspace exists or is unavailable. Use --resume for an existing live capture.');});
    session=sessionSchema.parse({version:1,id,title:options.title||'Live stream',indexId:options.indexId,offset:options.offset??0,fingerprint:createHash('sha256').update(id).digest('hex'),originMs:Date.now(),maxSeconds:options.maxSeconds??3600,
      kind:options.inputEnv?'hls':'rtmp',inputEnv:options.inputEnv,rtmpPort:options.rtmpPort??1935,phase:'capturing',ended:false,attempts:[],chunks:[],skipped:[]});
  }
  session.activeSeconds??=[...session.chunks,...session.skipped,...session.quarantined].reduce((sum,c)=>sum+Math.max(0,c.endSeconds-c.startSeconds),0);
  let lastClockSave=0;const clock=activeCaptureClock(session),usedSeconds=clock.used,accountTime=clock.checkpoint;
  const writer=createSessionWriter(()=>{accountTime();return atomicJsonOrText(join(directory,'live-session.json'),JSON.stringify(session,null,2));}),save=writer.save;
  // A PID lock prevents two producers from changing the same file/ordinal mapping.
  const lockPath=join(directory,'capture.lock');
  try{const pid=Number(await readFile(lockPath,'utf8'));if(!Number.isSafeInteger(pid)||pid<=0)throw new IngestionInputError('Invalid capture lock. Inspect it before removal.');
    let alive=true;try{process.kill(pid,0);}catch(e){if((e as NodeJS.ErrnoException).code==='ESRCH')alive=false;else throw e;}if(alive)throw new IngestionInputError('Another capture process owns this workspace.');await rm(lockPath);
  }catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;}
  const lock=await open(lockPath,'wx');await lock.writeFile(String(process.pid));await lock.close();
  const client=new IngestionClient(options.port??3000);const controller=new AbortController();const stop=()=>controller.abort();options.signal?.addEventListener('abort',stop,{once:true});
  let pumping=false,pump:Promise<void>|undefined,pumpError:unknown;
  const endInput=async(reason:'chunk_limit'|'timeline_limit')=>{if(session.inputLimit)return;session.inputLimit=reason;session.ended=true;controller.abort();await save();log(`Live input limit reached (${reason}); remaining raw files are retained while accepted chunks drain and seal.`);};
  const input=():LiveInput=>session.kind==='rtmp'?{kind:'rtmp',port:session.rtmpPort,path:`live/${session.id}`}:{kind:'hls',url:process.env[session.inputEnv!]||''};
  const ingestAvailable=async()=>{
    const status=await client.liveStatus(session.id);
    for(const chunk of session.chunks){
      const remote=status.chunks.find(c=>c.ordinal===chunk.ordinal);
      if(remote){if(chunk.jobId&&remote.jobId!==chunk.jobId)throw new IngestionInputError('Live chunk job mapping changed.');chunk.jobId=remote.jobId;
        if(['failed','needs_review'].includes(remote.jobState))throw new IngestionInputError(`Job ${remote.jobId} needs attention. Inspect /jobs/${remote.jobId}; capture files are retained.`);
        if(remote.stored&&!options.keepLocal&&!chunk.pruned){const path=join(directory,'chunks',chunk.filename);
          try{if((await fileIdentity(path)).sha256!==chunk.sha256)throw new IngestionInputError('A local capture file changed; it was retained instead of pruned.');}catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;}
          await rm(path,{force:true});chunk.pruned=true;await save();}
      }
    }
    let pending=status.chunks.filter(c=>!c.stored).length;
    for(const chunk of session.chunks){if(chunk.jobId)continue;if(pending>=4)break;
      const path=join(directory,'chunks',chunk.filename),identity=await fileIdentity(path);if(identity.sha256!==chunk.sha256||identity.bytes!==chunk.bytes)throw new IngestionInputError('A captured file changed. Restore its original bytes.');
      chunk.jobId=(await client.accept(session.id,{...chunk,path})).jobId;pending++;await save();log(`${chunk.filename}: queued (${chunk.startSeconds.toFixed(2)}–${chunk.endSeconds.toFixed(2)}s).`);
    }
  };
  const publish=async(attempt:LiveCaptureSession['attempts'][number],segment:LiveSegment,firstMediaAt?:number)=>{
    if(session.inputLimit)return;
    if(session.chunks.length>=720){await endInput('chunk_limit');return;}
    if(attempt.base===undefined){attempt.base=Math.max(session.chunks.at(-1)?.endSeconds??0,((firstMediaAt??attempt.startedMs)-session.originMs)/1000);await save();}
    const previous=session.chunks.at(-1)?.endSeconds??0,startSeconds=Math.max(previous,attempt.base+segment.startSeconds),endSeconds=attempt.base+segment.endSeconds;
    if(endSeconds-startSeconds<4){session.skipped.push({startSeconds,endSeconds,reason:'under_four_seconds'});attempt.consumed++;await save();return;}
    if(endSeconds>43200){await endInput('timeline_limit');return;}
    const quarantine=async(reason:'invalid_media'|'oversize'|'missing')=>{
      if(reason!=='missing'){
        const folder=join(directory,attempt.directory,'quarantine'),destination=join(folder,segment.filename);await mkdir(folder,{recursive:true});
        // Preserve the original until the rejection ledger and consumed position are durable.
        try{await copyFile(segment.path,destination,1);}catch(e){
          if((e as NodeJS.ErrnoException).code!=='EEXIST'||(await fileIdentity(segment.path)).sha256!==(await fileIdentity(destination)).sha256)throw e;
        }
      }
      session.quarantined.push({attempt:attempt.directory,filename:segment.filename,startSeconds,endSeconds,reason});attempt.consumed++;await save();
      if(reason!=='missing')try{await rm(segment.path,{force:true});}catch{log('Quarantine copy saved; its original was retained for cleanup.');}
      log(`${attempt.directory}/${segment.filename}: quarantined (${reason}); later footage can continue.`);
    };
    let info;try{info=await stat(segment.path);}catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;await quarantine('missing');return;}
    if(!info.isFile())throw new IngestionInputError('Capture segment path is not a regular file.');
    if(info.size>50_000_000){await quarantine('oversize');return;}
    try{const media=await probeChunkMedia(segment.path,endSeconds-startSeconds);if(media.durationSeconds<4)throw Error('Invalid media');}
    catch(e){
      // Missing FFprobe, permissions and disk errors are operational failures, not bad footage.
      const code=(e as NodeJS.ErrnoException).code;if(typeof code==='string')throw e;
      await quarantine('invalid_media');return;
    }
    const identity=await fileIdentity(segment.path);
    const ordinal=session.chunks.length,filename=`chunk_${String(ordinal).padStart(4,'0')}.mp4`,destination=join(directory,'chunks',filename);await mkdir(join(directory,'chunks'),{recursive:true});
    try{await copyFile(segment.path,destination,1);}catch(e){if((e as NodeJS.ErrnoException).code!=='EEXIST'||(await fileIdentity(destination)).sha256!==identity.sha256)throw e;}
    session.chunks.push({ordinal,filename,startSeconds,endSeconds,...identity,pruned:false});attempt.consumed++;await save();await rm(segment.path,{force:true});
    log(`${filename}: closed and validated; waiting for upload.`);
  };
  try{
    if(session.phase==='sealed'){await ingestAvailable();log(`Live capture ${session.id} was already sealed; durable local copies were checked for cleanup.`);return {workspace:directory,session};}
    if(session.ended&&!options.finish)throw new IngestionInputError('Capture has ended. Resume with --finish to drain and seal it.');
    await save();log(`Workspace: ${directory}`);log(`Broadcast: ${session.id}`);
    session.backendKey=captureBinding(session.backendKey,await client.binding(),session.chunks.some(c=>Boolean(c.jobId)));await save();
    await client.create({mode:'live',id:session.id,title:session.title,indexId:session.indexId,sourceOffsetSeconds:session.offset,sourceFingerprint:session.fingerprint});
    if((await client.liveStatus(session.id)).sealedAt){session.phase='sealed';session.ended=true;await save();return {workspace:directory,session};}
    for(const attempt of session.attempts){if(session.inputLimit)break;const rows=await closedLiveSegments(join(directory,attempt.directory));for(let i=attempt.consumed;i<rows.length&&!session.inputLimit;i++)await publish(attempt,rows[i]!);}
    if(session.chunks.length>=720)await endInput('chunk_limit');
    if((session.chunks.at(-1)?.endSeconds??0)>=43200)await endInput('timeline_limit');
    pumping=true;pump=(async()=>{while(pumping){try{await ingestAvailable();}catch(e){pumpError=e;controller.abort();return;}await wait(options.pollMs??1000);}})();
    if(!options.finish&&!session.inputLimit){
      clock.start();
      if(options.signal?.aborted)controller.abort();let failures=0;session.phase='capturing';await save();
      if(session.kind==='rtmp')log(`OBS secondary output: rtmp://127.0.0.1:${session.rtmpPort}/live/${session.id}`);else log(`Reading HLS from environment variable ${session.inputEnv}; its URL is not saved or printed.`);
      while(!controller.signal.aborted){
        const remaining=session.maxSeconds-usedSeconds();if(remaining<=0)break;
        const attempt={directory:`attempt_${String(session.attempts.length).padStart(4,'0')}`,startedMs:Date.now(),consumed:0} as LiveCaptureSession['attempts'][number];session.attempts.push(attempt);await save();
        const deadline=setTimeout(stop,remaining*1000);deadline.unref();let spoolExceeded=false;
        let result;try{result=await captureLiveAttempt({input:input(),directory:join(directory,attempt.directory),maxSeconds:remaining,signal:controller.signal,segmentSeconds:options.segmentSeconds,
          onSegment:(segment,firstMediaAt)=>publish(attempt,segment,firstMediaAt),onTick:async()=>{if(performance.now()-lastClockSave>=5000){lastClockSave=performance.now();await save();}if(await spoolSize(directory)>maxSpool){spoolExceeded=true;controller.abort();}}});}finally{clearTimeout(deadline);}
        if(spoolExceeded)throw new IngestionInputError('Live spool reached its disk budget. Resume with --finish after the backend/worker recovers.');
        if(controller.signal.aborted||usedSeconds()>=session.maxSeconds)break;
        failures=result.segments?0:failures+1;if(failures>(options.maxReconnects??5))throw new IngestionInputError('Live input could not reconnect. Closed chunks were retained. Resume this workspace or finish it.');
        log('Live input disconnected; retrying. Any missing interval remains a timeline gap.');
        await wait(Math.max(0,Math.min((session.maxSeconds-usedSeconds())*1000,15000,1000*2**Math.min(failures,4))),undefined,{signal:controller.signal}).catch(()=>{});
      }
    }
    clock.pause();pumping=false;await pump;pump=undefined;if(pumpError)throw pumpError;
    session.ended=true;session.phase='stopped';await save();
    const deadline=Date.now()+120000;
    while(session.chunks.some(c=>!c.jobId)){await ingestAvailable();if(Date.now()>deadline)throw new IngestionInputError('Upload backlog remains. Start/check API and worker, then resume with --finish.');await wait(options.pollMs??1000);}
    await client.seal(session.id);session.phase='sealed';await save();
    log(`Live capture sealed. ${session.chunks.length} chunks accepted; ${session.skipped.length} short tails and ${session.quarantined.length} quarantined segments retained in the workspace. Indexing can continue in the worker.`);
    return {workspace:directory,session};
  }catch(e){clock.pause();session.phase='interrupted';await save();throw e;}
  finally{pumping=false;await pump;options.signal?.removeEventListener('abort',stop);try{await writer.flush();}finally{await rm(lockPath,{force:true});}}
}
