import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,rm,stat,readFile,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import ffmpeg from 'ffmpeg-static';
import {planChunkIntervals} from '../src/ingestion-types.js';
import {splitRecording,fileIdentity} from '../src/segmenter.js';
import {runRecordedIngestion} from '../src/recorded-ingestion.js';
import {parseChunkManifest} from '../src/manifest.js';
let root:string,source:string;
before(async()=>{root=await mkdtemp(join(tmpdir(),'rewindr-segment-tests-'));source=join(root,'source (safe).mp4');
  await promisify(execFile)(ffmpeg as unknown as string,['-hide_banner','-loglevel','error','-f','lavfi','-i','color=c=blue:s=360x360:r=5','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','63','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac',source],{windowsHide:true,timeout:30000});});
after(async()=>{if(!root.startsWith(join(tmpdir(),'rewindr-segment-tests-')))throw Error('Bad cleanup');await rm(root,{recursive:true,force:true});});
test('straight-cut planning preserves the full source and merges undersized tails without overlap',()=>{
  for(const duration of [4,4.05,60,63.99,64.2,120,121.3,124.05,3600,21600]){
    const plan=planChunkIntervals(duration);assert.equal(plan[0]!.startSeconds,0);assert.equal(plan.at(-1)!.endSeconds,duration);
    plan.forEach((c,i)=>{assert.ok(c.endSeconds-c.startSeconds>=4&&c.endSeconds-c.startSeconds<=65);if(i)assert.equal(c.startSeconds,plan[i-1]!.endSeconds);});
  }
  assert.equal(planChunkIntervals(63).length,1);assert.equal(planChunkIntervals(121.3).at(-1)!.endSeconds,121.3);
  for(const bad of [0,3.9,Infinity,NaN,43201])assert.throws(()=>planChunkIntervals(bad));
});

test('chunk planning rounds duration once and uses integer microseconds for every minute boundary',()=>{
  for(let minute=1;minute<720;minute++){
    const boundary=minute*60+4.1;
    assert.equal(planChunkIntervals(boundary-0.000001).length,minute);
    assert.equal(planChunkIntervals(boundary).length,minute+1);
    assert.equal(planChunkIntervals(boundary+0.000001).length,minute+1);
    assert.equal(planChunkIntervals(Number(`${minute*60+4}.0999994`)).length,minute);
    assert.equal(planChunkIntervals(Number(`${minute*60+4}.0999995`)).length,minute+1);
  }
  assert.equal(planChunkIntervals(64.0999996).length,2);
  assert.equal(planChunkIntervals(64.1000004).at(-1)!.endSeconds,64.1);
});
test('FFmpeg closes and validates a merged tail before publishing its manifest, and resume reuses its exact bytes',async()=>{
  const directory=join(root,'closed-chunks');const original=await fileIdentity(source);let emitted=0;
  await splitRecording({source,directory,durationSeconds:63,onChunk:async chunk=>{emitted++;assert.ok(!chunk.path.endsWith('.partial.mp4'));assert.ok(chunk.bytes>0);
    assert.deepEqual(parseChunkManifest(await readFile(join(directory,'manifest.csv'),'utf8')),[{filename:chunk.filename,startSeconds:0,endSeconds:63}]);}});
  assert.equal(emitted,1);const before=await stat(join(directory,'chunk_0000.mp4'));const identity=await fileIdentity(join(directory,'chunk_0000.mp4'));
  await splitRecording({source,directory,durationSeconds:63,onChunk:async chunk=>{assert.equal(chunk.sha256,identity.sha256);}});
  assert.equal((await stat(join(directory,'chunk_0000.mp4'))).mtimeMs,before.mtimeMs);assert.deepEqual(await fileIdentity(source),original);
  assert.ok(!(await readdir(directory)).some(name=>name.includes('partial')));
});
test('prepare-only and resume need no API and retain source/chunks without starting indexing',async()=>{
  const workspace=join(root,'prepared-workspace');const messages:string[]=[];
  const prepared=await runRecordedIngestion({source,workspace,prepareOnly:true,port:9,log:m=>messages.push(m)});
  assert.equal(prepared.session.generated,true);assert.equal(prepared.session.chunks['chunk_0000.mp4']!.jobId,undefined);
  const resumed=await runRecordedIngestion({resume:workspace,prepareOnly:true,port:9,log:()=>{}});assert.equal(resumed.session.id,prepared.session.id);
  assert.ok(messages.some(message=>message.includes('No backend')));assert.equal(await stat(join(workspace,'run.lock')).then(()=>true,()=>false),false);
  await assert.rejects(runRecordedIngestion({source,workspace:join(root,'unauthorized'),creatorAuthorized:false,log:()=>{}}),/creator-authorized/);
});
test('cancelled preparation publishes no unfinished file or manifest entry',async()=>{
  const controller=new AbortController();controller.abort();let emitted=false;
  await assert.rejects(splitRecording({source,directory:join(root,'cancelled'),durationSeconds:63,signal:controller.signal,onChunk:async()=>{emitted=true;}}));assert.equal(emitted,false);
});
