import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {writeFile,stat} from 'node:fs/promises';
import {join} from 'node:path';
import ffmpeg from 'ffmpeg-static';
import ffprobe from 'ffprobe-static';
import {HttpError} from './http-errors.js';
const exec=promisify(execFile);
export async function encodeStoredClip(parts:{file:string;start:number;end:number}[],directory:string,signal:AbortSignal,guard:()=>Promise<void>){
  if(!parts.length||parts.length>8)throw new HttpError(422,'Choose a clip spanning at most eight stored sections.');
  const binary=process.env.FFMPEG_PATH||ffmpeg as unknown as string;
  for(const [i,part] of parts.entries()){
    await guard();signal.throwIfAborted();
    const {stdout}=await exec(process.env.FFPROBE_PATH||ffprobe.path,['-v','error','-protocol_whitelist','file','-show_streams','-of','json',part.file],{windowsHide:true,timeout:30000,signal});
    const hasAudio=(JSON.parse(stdout).streams as {codec_type:string}[]).some(s=>s.codec_type==='audio');
    await exec(binary,['-nostdin','-v','error','-y','-protocol_whitelist','file','-ss',String(part.start),'-i',part.file,...(hasAudio?[]:['-f','lavfi','-i','anullsrc=r=48000:cl=stereo']),'-t',String(part.end-part.start),'-map','0:v:0','-map',hasAudio?'0:a:0':'1:a:0','-vf','scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p','-c:v','libx264','-preset','veryfast','-threads','2','-b:v','1500k','-maxrate','1800k','-bufsize','3600k','-c:a','aac','-b:a','96k','-ar','48000','-ac','2','-map_metadata','-1','-movflags','+faststart',join(directory,`part_${i}.mp4`)],{windowsHide:true,timeout:180000,maxBuffer:1024*1024,signal});
  }
  await guard();await writeFile(join(directory,'parts.txt'),parts.map((_,i)=>`file 'part_${i}.mp4'`).join('\n'));
  const file=join(directory,'clip.mp4');await exec(binary,['-nostdin','-v','error','-y','-protocol_whitelist','file','-f','concat','-safe','1','-i',join(directory,'parts.txt'),'-c','copy','-movflags','+faststart',file],{windowsHide:true,timeout:60000,maxBuffer:1024*1024,signal});
  const {stdout}=await exec(process.env.FFPROBE_PATH||ffprobe.path,['-v','error','-show_entries','format=duration','-of','json',file],{windowsHide:true,timeout:30000,signal});
  const duration=Number(JSON.parse(stdout).format?.duration),expected=parts.reduce((sum,p)=>sum+p.end-p.start,0),bytes=(await stat(file)).size;
  if(!Number.isFinite(duration)||Math.abs(duration-expected)>0.35||bytes<1||bytes>50000000)throw new HttpError(422,'Export did not match the requested duration or size limit.');
  return file;
}
