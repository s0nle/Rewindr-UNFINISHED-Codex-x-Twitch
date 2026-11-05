import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,readFile,rm,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import Fastify from 'fastify';
import {createServer} from 'node:net';
import {setTimeout as wait} from 'node:timers/promises';
import ffmpeg from 'ffmpeg-static';
import {captureLiveAttempt,closedLiveSegments,validateLiveInput,type LiveSegment} from '../src/live-segmenter.js';
import {probeChunkMedia} from '../src/media.js';
async function port(){const server=createServer();await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));const value=(server.address() as {port:number}).port;await new Promise<void>(resolve=>server.close(()=>resolve()));return value;}
async function listening(value:number){for(let i=0;i<60;i++){const server=createServer();const occupied=await new Promise<boolean>((resolve,reject)=>{server.once('error',(e:NodeJS.ErrnoException)=>e.code==='EADDRINUSE'?resolve(true):reject(e));server.listen(value,'127.0.0.1',()=>server.close(()=>resolve(false)));});if(occupied)return;await wait(100);}throw Error('RTMP receiver did not bind');}
test('capture inputs stay local by default and only closed, complete CSV rows become segments',async t=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-live-manifest-'));t.after(async()=>{assert.ok(root.startsWith(join(tmpdir(),'rewindr-live-manifest-')));await rm(root,{recursive:true,force:true});});
  assert.throws(()=>validateLiveInput({kind:'hls',url:'file:///secret.m3u8'}));assert.throws(()=>validateLiveInput({kind:'hls',url:'http://example.com/live.m3u8'}));
  assert.throws(()=>validateLiveInput({kind:'rtmp',port:1935,path:'../other'}));
  assert.equal(validateLiveInput({kind:'hls',url:'https://example.com/live.m3u8?token=secret'}),'https://example.com/live.m3u8?token=secret');
  await writeFile(join(root,'closed.csv'),'raw_000000.mp4,0,60\nraw_000001.mp4,60,');assert.equal((await closedLiveSegments(root)).length,1);
  await writeFile(join(root,'closed.csv'),'../escape.mp4,0,60\n');await assert.rejects(()=>closedLiveSegments(root));
});
test('real RTMP input publishes finalized straight-cut H.264/AAC files while the publisher is still live',async t=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-live-rtmp-')),value=await port(),id=randomUUID(),controller=new AbortController();let publisher:ReturnType<typeof spawn>|undefined;
  t.after(async()=>{controller.abort();publisher?.kill();assert.ok(root.startsWith(join(tmpdir(),'rewindr-live-rtmp-')));await rm(root,{recursive:true,force:true});});
  const segments:LiveSegment[]=[];let publisherEnded=false,early=false;
  const capture=captureLiveAttempt({input:{kind:'rtmp',port:value,path:`live/${id}`},directory:root,maxSeconds:20,segmentSeconds:5,signal:controller.signal,
    onSegment:async(segment,clock)=>{assert.ok(clock);const media=await probeChunkMedia(segment.path,segment.endSeconds-segment.startSeconds);assert.equal(media.videoCodec,'h264');segments.push(segment);if(!publisherEnded)early=true;}});
  await listening(value);
  publisher=spawn(ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-re','-f','lavfi','-i','testsrc2=s=360x360:r=10','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','12','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-c:a','aac','-f','flv',`rtmp://127.0.0.1:${value}/live/${id}`],{windowsHide:true,stdio:['ignore','ignore','pipe']});
  let stderr='';publisher.stderr!.on('data',bytes=>{stderr+=String(bytes);});const code=await new Promise<number|null>((resolve,reject)=>{publisher!.once('error',reject);publisher!.once('close',code=>{publisherEnded=true;resolve(code);});});assert.equal(code,0,stderr);
  await capture;assert.equal(early,true,'Must publish before the whole broadcast ends');assert.ok(segments.length>=2);assert.ok(Math.abs(segments[0]!.endSeconds-segments[1]!.startSeconds)<0.1);
  assert.ok((await readFile(segments[0]!.path)).byteLength>0);
});

test('optional HLS input reads a playlist over HTTP and creates validated private-workspace MP4 segments',async t=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-live-hls-')),app=Fastify();
  t.after(async()=>{await app.close();assert.ok(root.startsWith(join(tmpdir(),'rewindr-live-hls-')));await rm(root,{recursive:true,force:true});});
  await promisify(execFile)(ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=s=360x360:r=10','-t','9','-c:v','libx264','-preset','ultrafast','-force_key_frames','expr:gte(t,n_forced*3)','-f','hls','-hls_time','3','-hls_playlist_type','vod','-hls_segment_filename',join(root,'part_%03d.ts'),join(root,'live.m3u8')],{windowsHide:true,timeout:30000});
  app.get('/live.m3u8',async(_req,reply)=>reply.type('application/vnd.apple.mpegurl').send(await readFile(join(root,'live.m3u8'))));
  app.get<{Params:{name:string}}>('/:name',async(req,reply)=>{if(!/^part_\d{3}\.ts$/.test(req.params.name))return reply.code(404).send();return reply.type('video/mp2t').send(await readFile(join(root,req.params.name)));});
  const address=await app.listen({host:'127.0.0.1',port:0}),chunks:LiveSegment[]=[];
  const result=await captureLiveAttempt({input:{kind:'hls',url:`${address}/live.m3u8?token=synthetic-test`},directory:join(root,'capture'),maxSeconds:20,segmentSeconds:5,onSegment:async segment=>{await probeChunkMedia(segment.path,segment.endSeconds-segment.startSeconds);chunks.push(segment);}});
  assert.equal(result.code,0);assert.ok(chunks.length>=1);
});
