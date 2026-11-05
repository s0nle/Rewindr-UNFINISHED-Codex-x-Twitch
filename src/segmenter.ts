import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createReadStream } from 'node:fs';
import { mkdir,rename,rm,stat,writeFile } from 'node:fs/promises';
import { createHash,randomUUID } from 'node:crypto';
import { join,resolve } from 'node:path';
import ffmpeg from 'ffmpeg-static';
import { planChunkIntervals } from './ingestion-types.js';
import { inspectIngestionSource,probeChunkMedia } from './media.js';
const exec=promisify(execFile);
export async function fileIdentity(path:string) {
  const info=await stat(path);if(!info.isFile())throw Error('Input must be a regular file.');
  const hash=createHash('sha256');for await(const bytes of createReadStream(path))hash.update(bytes);
  return {bytes:info.size,sha256:hash.digest('hex')};
}
export type ClosedChunk=ReturnType<typeof planChunkIntervals>[number]&{path:string;bytes:number;sha256:string};
export async function splitRecording(options:{source:string;directory:string;durationSeconds:number;onChunk:(chunk:ClosedChunk)=>Promise<void>;signal?:AbortSignal;segmentSeconds?:number}) {
  const source=resolve(options.source);const directory=resolve(options.directory);await mkdir(directory,{recursive:true});
  const actual=await inspectIngestionSource(source);
  if(Math.abs(actual.durationSeconds-options.durationSeconds)>0.1)throw Error('Source duration changed; start a new ingestion workspace.');
  const plan=planChunkIntervals(options.durationSeconds,options.segmentSeconds);
  const manifest:string[]=[];
  for(const chunk of plan) {
    options.signal?.throwIfAborted();
    const path=join(directory,chunk.filename);let exists=false;
    try {await stat(path);exists=true;}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
    if(!exists) {
      const partial=join(directory,`${chunk.filename}.${randomUUID()}.partial.mp4`);
      try {
        await exec(process.env.FFMPEG_PATH||ffmpeg as unknown as string,[
          '-nostdin','-hide_banner','-loglevel','error','-filter_threads','2','-protocol_whitelist','file','-threads','2','-ss',String(chunk.startSeconds),'-i',source,'-t',String(chunk.endSeconds-chunk.startSeconds),
          '-map','0:v:0','-map','0:a:0?','-map_metadata','-1','-vf',"fps=30,scale=w='min(1280,iw)':h='min(1280,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2",
          '-c:v','libx264','-threads','2','-preset','veryfast','-crf','23','-pix_fmt','yuv420p','-g','60','-maxrate','4M','-bufsize','8M',
          '-c:a','aac','-b:a','128k','-ac','2','-ar','48000','-movflags','+faststart','-n',partial
        ],{signal:options.signal,timeout:10*60*1000,maxBuffer:64*1024,windowsHide:true});
        await probeChunkMedia(partial,chunk.endSeconds-chunk.startSeconds);
        await rename(partial,path);
      } finally {await rm(partial,{force:true});}
    }
    const media=await probeChunkMedia(path,chunk.endSeconds-chunk.startSeconds);
    if(media.durationSeconds<4)throw Error('Encoded chunk is too short for Marengo; retain the workspace for inspection.');
    const identity=await fileIdentity(path);
    if(identity.bytes>50_000_000)throw Error('Encoded chunk exceeds the 50 MB upload limit.');
    // A closed, validated MP4 is renamed before it appears in the manifest.
    manifest.push(`${chunk.filename},${chunk.startSeconds},${chunk.endSeconds}`);
    await atomicJsonOrText(join(directory,'manifest.csv'),manifest.join('\n')+'\n');
    await options.onChunk({...chunk,path,...identity});
  }
  return plan;
}
export async function atomicJsonOrText(path:string,text:string) {
  const temp=`${path}.${randomUUID()}.tmp`;
  try{await writeFile(temp,text,{flag:'wx',mode:0o600});await rename(temp,path);}finally{await rm(temp,{force:true});}
}
