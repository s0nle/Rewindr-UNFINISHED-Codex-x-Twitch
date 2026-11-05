import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import Fastify from 'fastify';
import {runLiveCapture} from '../src/live-capture.js';
import {activeCaptureClock} from '../src/live-clock.js';
test('active capture budget excludes stopped-process downtime and persists elapsed time across resumes',()=>{
  let now=1000;const state={activeSeconds:25},clock=activeCaptureClock(state,()=>now);clock.start();now+=10_000;clock.checkpoint();assert.equal(state.activeSeconds,35);
  clock.pause();now+=12*3600_000;assert.equal(clock.used(),35);clock.start();now+=2000;clock.pause();assert.equal(state.activeSeconds,37);
  const resumed=activeCaptureClock(JSON.parse(JSON.stringify(state)),()=>now);now+=3600_000;assert.equal(resumed.used(),37);resumed.start();now+=1000;assert.equal(resumed.used(),38);
});
test('a full 720-chunk workspace drains and seals with extra manifest rows retained as end-of-input',async t=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-live-full-')),app=Fastify(),id=randomUUID();let seals=0;
  t.after(async()=>{await app.close();assert.ok(root.startsWith(join(tmpdir(),'rewindr-live-full-')));await rm(root,{recursive:true,force:true});});
  const chunks=Array.from({length:720},(_,i)=>({ordinal:i,filename:`chunk_${String(i).padStart(4,'0')}.mp4`,startSeconds:i*60,endSeconds:(i+1)*60,bytes:1000,sha256:'a'.repeat(64),jobId:randomUUID(),pruned:true}));
  await mkdir(join(root,'attempt_0000'));await writeFile(join(root,'attempt_0000','closed.csv'),'raw_000000.mp4,43200,43260\nraw_000001.mp4,43260,43320\n');await writeFile(join(root,'attempt_0000','raw_000000.mp4'),'retained extra');
  await writeFile(join(root,'live-session.json'),JSON.stringify({version:1,id,title:'Full',indexId:'test',offset:0,fingerprint:'a'.repeat(64),originMs:Date.now()-10*3600_000,maxSeconds:3600,kind:'rtmp',rtmpPort:1935,phase:'interrupted',ended:false,attempts:[{directory:'attempt_0000',startedMs:Date.now(),base:0,consumed:0}],chunks,skipped:[]}));
  app.post('/ingestions',async()=>({ingestionId:id,broadcastId:id}));app.get(`/ingestions/${id}`,async()=>({mode:'live',sealedAt:null,status:'accepting',latestSearchableSeconds:43200,chunks:chunks.map(c=>({ordinal:c.ordinal,stored:true,jobId:c.jobId,jobState:'completed'}))}));
  app.post(`/ingestions/${id}/complete`,async()=>{seals++;return {status:'ready',searchableThroughSeconds:43200};});
  const address=await app.listen({host:'127.0.0.1',port:0});const result=await runLiveCapture({resume:root,finish:true,creatorAuthorized:true,keepLocal:true,port:Number(new URL(address).port),log:()=>{}});
  assert.equal(seals,1);assert.equal(result.session.phase,'sealed');assert.equal(result.session.inputLimit,'chunk_limit');assert.equal(result.session.chunks.length,720);assert.equal(result.session.attempts[0]!.consumed,0);assert.equal(await readFile(join(root,'attempt_0000','raw_000000.mp4'),'utf8'),'retained extra');
});
