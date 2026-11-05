import {before,after,test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,createHash,createHmac} from 'node:crypto';
import {mkdtemp,rm,copyFile,stat,readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import ffmpeg from 'ffmpeg-static';
import ffprobe from 'ffprobe-static';
import {PGlite} from '@electric-sql/pglite';
import {createClient} from '@supabase/supabase-js';
import {applyTestMigrations} from './queue-shim.js';
import {SupabaseMedia,type ExportStorage} from '../src/media-store.js';
import {SupabasePersistence} from '../src/supabase-persistence.js';
import {SupabaseTwitch} from '../src/twitch-store.js';
import {SupabaseSearches} from '../src/search-store.js';
import {MediaWorker} from '../src/media-worker.js';
import {buildTwitchEbs} from '../src/twitch-ebs.js';
import {chunkObjectPath,type ChunkStorage} from '../src/chunk-storage.js';
import type {ExportStore} from '../src/export-storage.js';
import type {Broadcast} from '../src/types.js';
import {clipClock,parseClipTime} from '../src/clip-time.js';
import {IngestionClient,captureBinding} from '../src/ingestion-client.js';
import {HostedChannels} from '../src/hosted.js';
import type {TwitchApi} from '../src/twitch-api.js';
const exec=promisify(execFile);let db:PGlite;
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});after(async()=>{await db?.close();});
const client=createClient('https://test.supabase.co','key',{auth:{persistSession:false},global:{fetch:async(input,init)=>{
  const url=new URL(String(input));try{
    if(url.pathname.includes('/rpc/')){const fn=url.pathname.split('/').at(-1)!;assert.match(fn,/^rewindr_[a-z0-9_]+$/);const args=JSON.parse(String(init?.body)) as Record<string,unknown>,keys=Object.keys(args);keys.forEach(k=>assert.match(k,/^p_[a-z_]+$/));return Response.json((await db.query<{v:unknown}>(`select ${fn}(${keys.map((k,i)=>`${k}=>$${i+1}`).join(',')}) v`,keys.map(k=>args[k]!==null&&typeof args[k]==='object'?JSON.stringify(args[k]):args[k]))).rows[0]!.v??null);}
    const table=url.pathname.split('/').at(-1)!;assert.match(table,/^rewindr_[a-z_]+$/);const filters=[...url.searchParams].filter(([k])=>['id','owner_id','channel_id','twitch_user_id'].includes(k));return Response.json((await db.query(`select * from ${table}${filters.length?' where '+filters.map(([k],i)=>`${k}=$${i+1}`).join(' and '):''} order by ${table==='rewindr_publications'?'updated_at':table==='rewindr_twitch_links'?'verified_at':'created_at'} desc`,filters.map(([,v])=>v.replace(/^eq\./,'')))).rows);
  }catch(e){return Response.json({code:(e as {code?:string}).code||'TEST',message:'Rejected'},{status:400});}
}}});
async function fixture(t:{after:(fn:()=>Promise<void>)=>void},encode=false,hosted=false){
  const owner=randomUUID(),other=randomUUID(),root=await mkdtemp(join(tmpdir(),'rewindr-media-test-')),sourceId=randomUUID(),links=new SupabaseTwitch(client),media=new SupabaseMedia(client,owner),persistence=new SupabasePersistence(root,client,owner);
  await db.query("insert into rewindr_broadcasters(id,label) values($1,'media test'),($2,'other')",[owner,other]);await links.link(owner,{id:'123',login:'test',display_name:'Test',profile_image_url:'https://example.test/avatar.png'});await links.link(other,{id:'456',login:'other',display_name:'Other',profile_image_url:'https://example.test/other.png'});
  const source:Broadcast={id:sourceId,title:'Synthetic archive',indexId:'synthetic',createdAt:new Date().toISOString(),sourceOffsetSeconds:7200,chunks:[]};const files:string[]=[];
  for(let i=0;i<2;i++){
    const file=join(root,`source${i}.mp4`);if(encode){await exec(ffmpeg as unknown as string,['-y','-v','error','-f','lavfi','-i',`color=c=${i?'blue':'red'}:s=${i?'480x360':'360x360'}:r=30`,...(i?[]:['-f','lavfi','-i','sine=frequency=440:sample_rate=48000']),'-t','4','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p',...(i?[]:['-c:a','aac']),file],{windowsHide:true});files.push(file);}
    const bytes=encode?(await stat(file)).size:1000,hash=encode?createHash('sha256').update(await readFile(file)).digest('hex'):'a'.repeat(64);
    source.chunks.push({filename:`chunk_${i}.mp4`,indexedAssetId:`video${i}`,startSeconds:i*4,endSeconds:(i+1)*4,storage:{bucket:'rewindr-chunks',path:chunkObjectPath(owner,sourceId,`video${i}`,hash),sha256:hash,bytes,uploadedAt:new Date().toISOString()}});
  }
  await persistence.broadcasts.put(source);for(const c of source.chunks)await persistence.broadcasts.attachStorage!(sourceId,c.indexedAssetId,c.storage!);
  const objects=new Map<string,ExportStorage>(),removed:string[]=[];let failUpload=false,failDelete=false;let uploads=0,deleteCalls=0,failDeleteCall=-1;
  const store:ExportStore={bucket:'rewindr-exports',init:async()=>{},exists:async ref=>objects.has(ref.path),upload:async(ref,file)=>{uploads++;assert.equal(createHash('sha256').update(await readFile(file)).digest('hex'),ref.sha256);objects.set(ref.path,ref);if(failUpload){failUpload=false;throw Error('Uncertain upload response');}await copyFile(file,join(root,'rendered.mp4'));},sign:async(ref,_download,ttl)=>({url:`https://storage.example.test/${ref.path}?ttl=${ttl||300}`,expiresAt:new Date(Date.now()+(ttl||300)*1000).toISOString()}),remove:async(_bucket,path)=>{deleteCalls++;if(failDelete||deleteCalls===failDeleteCall){failDelete=false;throw Error('Temporary Storage deletion error');}assert.ok(path.startsWith(owner+'/'));removed.push(path);objects.delete(path);}};
  const unexpected=async():Promise<never>=>{throw Error('Unexpected operation');};const chunks:ChunkStorage={bucket:'rewindr-chunks',init:async()=>{},upload:unexpected,sign:unexpected,download:async(_bid,key,_ref,destination)=>{await copyFile(files[Number(key.replace('video',''))]!,destination);}};
  const worker=new MediaWorker({root,queue:media,repository:id=>new SupabaseMedia(client,id),chunks:()=>chunks,exports:()=>store});
  const secret=randomBytes(32),headers=(channel='123',role='broadcaster')=>{const input=[{alg:'HS256'},{exp:Math.floor(Date.now()/1000)+600,role,channel_id:channel,opaque_user_id:'U'+channel}].map(v=>Buffer.from(JSON.stringify(v)).toString('base64url')).join('.');return {host:'ebs.test',origin:'https://extension.ext-twitch.tv',authorization:'Bearer '+input+'.'+createHmac('sha256',secret).update(input).digest('base64url')};};
  const host=hosted?new HostedChannels({client,api:{user:async(id:string)=>({id,login:'streamer',display_name:'Streamer',profile_image_url:'https://example.test/avatar.png'})} as TwitchApi,dataDir:root,indexId:'synthetic',maxChannels:2,allowlist:[]}):undefined;
  const app=await buildTwitchEbs({config:{clientId:'extension',secrets:[secret],publicUrl:'https://ebs.test',origins:['https://extension.ext-twitch.tv'],port:3001},hosted:host,provider:hosted?{name:'twelvelabs',validateIndex:async()=>{},ensureIndex:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,search:unexpected}:undefined,links,analysisConfigured:false,analysisModel:'pegasus1.5',scope:id=>({persistence:new SupabasePersistence(root,client,id),storage:chunks,searches:new SupabaseSearches(client,id),media:new SupabaseMedia(client,id),exports:store})});
  t.after(async()=>{worker.stop();await app.close();await db.query('delete from rewindr_broadcasters where id=any($1::uuid[])',[[owner,other]]);assert.ok(root.startsWith(join(tmpdir(),'rewindr-media-test-')));await rm(root,{recursive:true,force:true});});
  const create=async()=>{const response=await app.inject({method:'POST',url:'/extension/exports',headers:headers(),payload:{requestId:randomUUID(),broadcastId:sourceId,title:'Selected moment',startSeconds:3,endSeconds:6}});assert.equal(response.statusCode,202,response.body);return response.json<{id:string;state:string}>();};
  return {owner,other,root,media,app,worker,store,source,headers,create,objects,removed,get uploads(){return uploads;},failUpload:()=>{failUpload=true;},failDelete:()=>{failDelete=true;},failSecondDelete:()=>{failDeleteCall=2;}};
}
test('clip time parsing preserves the broadcast clock, rejects malformed minute fields and rounds carries',()=>{assert.equal(parseClipTime('02:00:03.5'),7203.5);assert.equal(parseClipTime('63.5'),63.5);assert.ok(Number.isNaN(parseClipTime('01:99:00')));assert.ok(Number.isNaN(parseClipTime('1.5:03')));assert.equal(clipClock(59.99),'00:01:00.0');});
test('cross-chunk export produces a real playable MP4 with normalized silent/audio sections and accurate duration',async t=>{
  const s=await fixture(t,true),created=await s.create();assert.equal(created.state,'queued');assert.equal(await s.worker.runOnce(),true);
  const row=await s.media.export(created.id);assert.ok(row?.ready_at);assert.equal(s.uploads,1);const result=await s.app.inject({url:`/extension/exports/${created.id}`,headers:s.headers()});assert.equal(result.json().state,'ready');assert.ok(result.json().download.url);
  const {stdout}=await exec(ffprobe.path,['-v','error','-show_format','-show_streams','-of','json',join(s.root,'rendered.mp4')],{windowsHide:true});const info=JSON.parse(stdout);assert.ok(Math.abs(Number(info.format.duration)-3)<0.2);assert.equal(info.streams[0].width,1280);assert.ok(info.streams.some((v:{codec_type:string})=>v.codec_type==='audio'));
  assert.equal((await s.app.inject({url:`/extension/exports/${created.id}`,headers:s.headers('456')})).statusCode,404);
});
test('uncertain export upload resumes from immutable checkpoint without another upload or encoding',async t=>{
  const s=await fixture(t,true),created=await s.create();s.failUpload();await s.worker.runOnce();assert.equal((await s.media.task(created.id))?.state,'retrying');
  await db.query('update rewindr_media_tasks set available_at=clock_timestamp() where id=$1',[created.id]);await s.worker.runOnce();assert.ok((await s.media.export(created.id))?.ready_at);assert.equal(s.uploads,1);
});
test('viewer receives only an approved rendered clip; hide, stale versions, wrong roles/channels and disconnect are enforced',async t=>{
  const s=await fixture(t),e=await s.create(),lease=(await s.media.claim())!,ref={bucket:'rewindr-exports',path:`${s.owner}/exports/${e.id}.mp4`,sha256:'b'.repeat(64),bytes:1000};await s.media.checkpoint(lease,ref);await s.media.finish(lease);
  const viewer=s.headers('123','viewer');assert.equal((await s.app.inject({url:'/viewer/selection',headers:viewer})).json().selection,null);
  const publish=await s.app.inject({method:'POST',url:'/extension/publication',headers:s.headers(),payload:{exportId:e.id,version:0}});assert.equal(publish.statusCode,200,publish.body);
  const selection=await s.app.inject({url:'/viewer/selection',headers:viewer});assert.equal(selection.json().selection.title,'Selected moment');assert.match(selection.json().selection.url,/ttl=60/);assert.doesNotMatch(selection.body,/question|indexId|snapshot|sourceOffset|video0/);
  assert.equal((await s.app.inject({url:'/viewer/selection',headers:s.headers('456','viewer')})).json().selection,null);
  assert.equal((await s.app.inject({method:'POST',url:'/extension/publication',headers:viewer,payload:{exportId:null,version:1}})).statusCode,403);
  assert.equal((await s.app.inject({method:'POST',url:'/extension/publication',headers:s.headers(),payload:{exportId:null,version:0}})).statusCode,409);
  assert.equal((await s.app.inject({method:'POST',url:'/extension/publication',headers:s.headers(),payload:{exportId:null,version:1}})).statusCode,200);assert.equal((await s.app.inject({url:'/viewer/selection',headers:viewer})).json().selection,null);
  await new SupabaseTwitch(client).unlink(s.owner);assert.equal((await s.app.inject({url:`/extension/exports/${e.id}`,headers:s.headers()})).statusCode,403);
});
test('archive deletion hides immediately, fences new work, survives deletion errors and removes only its scoped cloud objects',async t=>{
  const s=await fixture(t),created=await s.create();await assert.rejects(s.media.purge(randomUUID(),s.source.id,s.source.title),{statusCode:409});
  const lease=(await s.media.claim())!;await s.media.finish(lease,'test fixture terminal',true);
  const task=await s.media.purge(randomUUID(),s.source.id,s.source.title);assert.equal(await new SupabasePersistence(s.root,client,s.owner).broadcasts.get(s.source.id),undefined);await assert.rejects(s.media.begin(randomUUID(),s.source.id,'Later',0,1000),{statusCode:409});
  s.failSecondDelete();await s.worker.runOnce();assert.equal((await s.media.task(task.id))?.state,'retrying');assert.equal((await s.media.task(task.id))?.completed_objects,1);await db.query('update rewindr_media_tasks set available_at=clock_timestamp() where id=$1',[task.id]);await s.worker.runOnce();assert.equal((await s.media.task(task.id))?.state,'completed');assert.equal(s.removed.length,2);assert.equal((await db.query('select * from rewindr_broadcasts where id=$1',[s.source.id])).rows.length,0);
});
test('usage reservations deduplicate identities, enforce caps atomically, and cannot be called by browser roles',async t=>{
  const s=await fixture(t);await s.media.rpc('rewindr_reserve_usage',{p_owner_id:s.owner,p_metric:'search',p_event_id:'cap',p_amount:100});await s.media.rpc('rewindr_reserve_usage',{p_owner_id:s.owner,p_metric:'search',p_event_id:'cap',p_amount:100});await assert.rejects(s.media.rpc('rewindr_reserve_usage',{p_owner_id:s.owner,p_metric:'search',p_event_id:'over'}),{statusCode:429});assert.equal((await s.media.usage()).find(u=>u.metric==='search')!.amount,100);
  await db.exec('set role authenticated');try{for(const table of ['rewindr_exports','rewindr_media_tasks','rewindr_publications','rewindr_agent_credentials'])await assert.rejects(db.query(`select * from ${table}`));await assert.rejects(db.query('select rewindr_claim_media()'));}finally{await db.exec('reset role');}
});
test('hosted credentials are hashed, rotate, expire, revoke and cannot survive a channel unlink',async t=>{
  const s=await fixture(t),user={id:'123',login:'test',display_name:'Test',profile_image_url:'https://example.test/avatar.png'},hash=(v:string)=>createHash('sha256').update(v).digest('hex');
  await s.media.rpc('rewindr_onboard_channel',{p_channel:'123',p_user:user,p_token_hash:hash('first')});assert.equal(await s.media.rpc('rewindr_agent_owner',{p_hash:hash('first')}),s.owner);
  await s.media.rpc('rewindr_onboard_channel',{p_channel:'123',p_user:user,p_token_hash:hash('second')});assert.equal(await s.media.rpc('rewindr_agent_owner',{p_hash:hash('first')}),null);await new SupabaseTwitch(client).unlink(s.owner);assert.equal(await s.media.rpc('rewindr_agent_owner',{p_hash:hash('second')}),null);
  assert.throws(()=>new IngestionClient(3000,undefined,{url:'http://remote.test',token:'rwa_'+'a'.repeat(43)}));assert.throws(()=>new IngestionClient(3000,undefined,{url:'https://remote.test/path',token:'rwa_'+'a'.repeat(43)}));
  assert.equal(captureBinding(undefined,'local',true),'local');assert.throws(()=>captureBinding('https://one:owner','https://two:owner',true));assert.throws(()=>captureBinding(undefined,'https://one:owner',true));
});
test('hosted capture client authenticates multipart ingestion and cannot read another channel or select a different index',async t=>{
  const s=await fixture(t,true,true),onboard=await s.app.inject({method:'POST',url:'/extension/onboard',headers:s.headers(),payload:{}});assert.equal(onboard.statusCode,200,onboard.body);const token=onboard.json().token;
  const original=globalThis.fetch;globalThis.fetch=async(input,init)=>{const request=new Request(input,init);assert.equal(request.url.startsWith('https://ebs.test/agent/'),true);assert.equal(request.headers.get('authorization'),'Bearer '+token);assert.equal(init?.redirect,'error');const headers=Object.fromEntries(request.headers);headers.host='ebs.test';const response=await s.app.inject({method:request.method as 'GET'|'POST',url:new URL(request.url).pathname,headers,payload:request.method==='GET'?undefined:Buffer.from(await request.arrayBuffer())});return new Response(response.body,{status:response.statusCode,headers:{'content-type':'application/json'}});};
  try{
    const capture=new IngestionClient(3000,undefined,{url:'https://ebs.test',token}),input={id:randomUUID(),title:'Hosted capture',indexId:'synthetic',sourceDurationSeconds:8,sourceOffsetSeconds:0,sourceFingerprint:'b'.repeat(64)};
    assert.equal(await capture.binding(),'https://ebs.test:'+s.owner);await capture.create(input);const accepted=await capture.accept(input.id,{ordinal:0,filename:'chunk_0000.mp4',path:join(s.root,'source0.mp4'),startSeconds:0,endSeconds:8,sha256:'a'.repeat(64),bytes:1000} as never).catch(e=>e);
    // A 4-second file cannot masquerade as an 8-second planned interval.
    assert.ok(accepted instanceof Error);
    const validInput={...input,id:randomUUID(),sourceDurationSeconds:4};await capture.create(validInput);const valid=await capture.accept(validInput.id,{ordinal:0,filename:'chunk_0000.mp4',path:join(s.root,'source0.mp4'),startSeconds:0,endSeconds:4} as never);assert.equal((await capture.job(valid.jobId)).state,'queued');await capture.seal(validInput.id);
    const otherToken=(await s.app.inject({method:'POST',url:'/extension/onboard',headers:s.headers('456'),payload:{}})).json().token;
    assert.equal((await s.app.inject({url:`/agent/ingestions/${input.id}`,headers:{host:'ebs.test',authorization:'Bearer '+otherToken}})).statusCode,404);
    assert.equal((await s.app.inject({method:'POST',url:'/agent/ingestions',headers:{host:'ebs.test',authorization:'Bearer '+token,'x-creator-authorized':'true'},payload:{...input,id:randomUUID(),indexId:'another-channel-index'}})).statusCode,403);
    await s.app.inject({method:'POST',url:'/extension/agent/revoke',headers:s.headers(),payload:{}});assert.equal((await s.app.inject({url:`/agent/ingestions/${input.id}`,headers:{host:'ebs.test',authorization:'Bearer '+token}})).statusCode,401);
  }finally{globalThis.fetch=original;}
});
test('expired media leases are fenced, export checkpoints reject other-owner paths, and expired exports purge independently',async t=>{
  const s=await fixture(t),e=await s.create(),old=(await s.media.claim())!;
  await db.query("update rewindr_media_tasks set lease_until=clock_timestamp()-interval '1 second' where id=$1",[old.id]);const next=(await s.media.claim())!;assert.notEqual(next.lease_token,old.lease_token);await assert.rejects(s.media.guard(old));
  const ref={bucket:'rewindr-exports',path:`${s.owner}/exports/${e.id}.mp4`,sha256:'b'.repeat(64),bytes:1000};await assert.rejects(s.media.checkpoint(next,{...ref,path:`${s.other}/exports/${e.id}.mp4`}),{statusCode:409});await s.media.checkpoint(next,ref);await s.media.finish(next);
  await s.media.publish('123',e.id,0);await db.query("update rewindr_exports set expires_at=clock_timestamp()-interval '1 second' where id=$1",[e.id]);assert.equal((await s.app.inject({url:'/viewer/selection',headers:s.headers('123','viewer')})).json().selection,null);
  await s.media.rpc('rewindr_begin_export_expiry',{p_owner_id:s.owner,p_id:randomUUID(),p_export_id:e.id});await s.worker.runOnce();assert.equal(await s.media.export(e.id),undefined);assert.ok(await new SupabasePersistence(s.root,client,s.owner).broadcasts.get(s.source.id));assert.equal((await s.media.publication('123'))?.export_id,null);assert.equal(s.removed.length,1);
});
