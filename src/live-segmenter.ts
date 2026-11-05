import {spawn} from 'node:child_process';
import {mkdir,readFile} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {setTimeout as wait} from 'node:timers/promises';
import ffmpeg from 'ffmpeg-static';
export type LiveInput={kind:'rtmp';port:number;path:string}|{kind:'hls';url:string};
export type LiveSegment={path:string;filename:string;startSeconds:number;endSeconds:number};
export function validateLiveInput(input:LiveInput){
  if(input.kind==='rtmp'){
    if(!Number.isInteger(input.port)||input.port<1024||input.port>65535||!/^live\/[a-f0-9-]{36}$/.test(input.path))throw Error('Invalid local RTMP receiver.');
    return `rtmp://127.0.0.1:${input.port}/${input.path}`;
  }
  const url=new URL(input.url);
  if(!((url.protocol==='https:')||(url.protocol==='http:'&&url.hostname==='127.0.0.1'))||url.username||url.password||!url.pathname.endsWith('.m3u8'))throw Error('Use an HTTPS HLS playlist URL (HTTP is allowed only for 127.0.0.1 verification).');
  return url.href;
}
export async function closedLiveSegments(directory:string):Promise<LiveSegment[]>{
  let content:string;try{content=await readFile(join(directory,'closed.csv'),'utf8');}catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return [];throw e;}
  const lines=content.split('\n');lines.pop(); // A partial trailing row is never a completion marker.
  return lines.filter(Boolean).map(line=>{
    const match=/^(raw_\d{6}\.mp4),([0-9]+(?:\.[0-9]+)?),([0-9]+(?:\.[0-9]+)?)\r?$/.exec(line);
    if(!match)throw Error('Capture manifest contains an invalid segment row.');
    const startSeconds=Number(match[2]),endSeconds=Number(match[3]);if(!Number.isFinite(endSeconds)||endSeconds<=startSeconds||endSeconds-startSeconds>66)throw Error('Invalid captured interval.');
    return {filename:match[1]!,path:join(directory,match[1]!),startSeconds,endSeconds};
  });
}
export async function captureLiveAttempt(options:{input:LiveInput;directory:string;maxSeconds:number;signal?:AbortSignal;segmentSeconds?:number;
  onSegment:(segment:LiveSegment,firstMediaAt:number|undefined)=>Promise<void>;onTick?:()=>Promise<void>}){
  const url=validateLiveInput(options.input),directory=resolve(options.directory),segmentSeconds=options.segmentSeconds??60;
  if(!Number.isFinite(options.maxSeconds)||options.maxSeconds<0.1||options.maxSeconds>43200||!Number.isInteger(segmentSeconds)||segmentSeconds<5||segmentSeconds>60)throw Error('Invalid live capture duration.');
  await mkdir(directory,{recursive:true});options.signal?.throwIfAborted();
  const args=['-hide_banner','-loglevel','error'];
  if(options.input.kind==='rtmp')args.push('-protocol_whitelist','rtmp,tcp','-listen','1','-timeout','60');
  else args.push('-protocol_whitelist','http,https,tcp,tls,crypto','-rw_timeout','15000000','-live_start_index','-1');
  args.push('-i',url,'-map','0:v:0','-map','0:a:0?','-t',String(options.maxSeconds),'-vf','scale=w=1280:h=720:force_original_aspect_ratio=decrease:force_divisible_by=2',
    '-c:v','libx264','-preset','veryfast','-crf','25','-maxrate','3500k','-bufsize','7000k','-pix_fmt','yuv420p','-r','30',
    '-force_key_frames',`expr:gte(t,n_forced*${segmentSeconds})`,'-c:a','aac','-b:a','96k','-af','aresample=async=1:first_pts=0',
    '-f','segment','-segment_time',String(segmentSeconds),'-segment_time_delta','0.02','-reset_timestamps','1','-segment_format_options','movflags=+faststart',
    '-segment_list','closed.csv','-segment_list_type','csv','-segment_list_size','0','-progress','pipe:1','-nostats','raw_%06d.mp4');
  const child=spawn(process.env.FFMPEG_PATH||ffmpeg as unknown as string,args,{cwd:directory,windowsHide:true,stdio:['pipe','pipe','pipe']});
  child.stdin.on('error',()=>{});child.stderr.resume(); // Never log URLs, tokens or provider error bodies.
  let ended=false,spawnError:unknown,firstMediaAt:number|undefined,buffer='',consumed=0,stopping=false,killTimer:ReturnType<typeof setTimeout>|undefined;
  child.stdout.setEncoding('utf8');child.stdout.on('data',(data:string)=>{buffer+=data;const lines=buffer.split('\n');buffer=lines.pop()!;for(const line of lines){const match=/^out_time_us=(\d+)/.exec(line);if(firstMediaAt===undefined&&match&&Number(match[1])>0)firstMediaAt=Date.now()-Number(match[1])/1000;}});
  const done=new Promise<number|null>(resolve=>{child.once('error',error=>{spawnError=error;});child.once('close',code=>{ended=true;resolve(code);});});
  const stop=()=>{if(stopping)return;stopping=true;child.stdin.write('q\n');killTimer=setTimeout(()=>{child.kill();},5000);killTimer.unref();};
  options.signal?.addEventListener('abort',stop,{once:true});
  const consume=async()=>{const rows=await closedLiveSegments(directory);while(consumed<rows.length){await options.onSegment(rows[consumed]!,firstMediaAt);consumed++;}};
  try{while(!ended){await consume();await options.onTick?.();await wait(200);}const code=await done;if(spawnError)throw Error('FFmpeg could not start. Check FFMPEG_PATH.');await consume();return {code,segments:consumed,stopped:stopping};}
  finally{options.signal?.removeEventListener('abort',stop);if(!ended){stop();await done;}if(killTimer)clearTimeout(killTimer);}
}
