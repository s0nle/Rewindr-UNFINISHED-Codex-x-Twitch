import assert from 'node:assert/strict';
import {randomUUID,createHash,randomBytes,createHmac} from 'node:crypto';
import {mkdtemp,rm,readFile,stat,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import ffmpeg from 'ffmpeg-static';
import ffprobe from 'ffprobe-static';
import {databaseClient,safeDatabaseError} from '../src/db-connection.js';
import {configuredSupabaseClient,SupabasePersistence} from '../src/supabase-persistence.js';
import {SupabaseChunkStorage,chunkObjectPath} from '../src/chunk-storage.js';
import {SupabaseExports} from '../src/export-storage.js';
import {SupabaseMedia} from '../src/media-store.js';
import {SupabaseTwitch} from '../src/twitch-store.js';
import {SupabaseSearches} from '../src/search-store.js';
import {MediaWorker} from '../src/media-worker.js';
import {buildTwitchEbs} from '../src/twitch-ebs.js';
import type {Broadcast} from '../src/types.js';
const exec=promisify(execFile);
async function main(){
  const db=await databaseClient();await db.connect();const owner=randomUUID(),channel='999'+BigInt('0x'+owner.replaceAll('-','').slice(0,20)).toString(),root=await mkdtemp(join(tmpdir(),'rewindr-media-verify-'));
  const client=configuredSupabaseClient(),chunks=new SupabaseChunkStorage(client,owner,process.env.SUPABASE_STORAGE_BUCKET||'rewindr-chunks'),exports=new SupabaseExports(client,owner,process.env.SUPABASE_EXPORT_BUCKET||'rewindr-exports'),media=new SupabaseMedia(client,owner,owner),links=new SupabaseTwitch(client),persistence=new SupabasePersistence(root,client,owner);
  const objects:{bucket:string;path:string}[]=[];let app:Awaited<ReturnType<typeof buildTwitchEbs>>|undefined,ownerCreated=false;
  try{
    await chunks.init();await exports.init();await db.query("insert into rewindr_broadcasters(id,label) values($1,'Temporary media verification')",[owner]);ownerCreated=true;
    await links.link(owner,{id:channel,login:'rewindr_verify',display_name:'Synthetic media verification',profile_image_url:'https://example.invalid/avatar.png'});
    const source:Broadcast={id:randomUUID(),title:'Temporary media verification',indexId:'synthetic-no-provider-calls',sourceOffsetSeconds:7200,createdAt:new Date().toISOString(),chunks:[]};
    for(let i=0;i<2;i++){
      const file=join(root,`source_${i}.mp4`);await exec(process.env.FFMPEG_PATH||ffmpeg as unknown as string,['-v','error','-f','lavfi','-i',`color=c=${i?'blue':'red'}:s=360x360:r=30`,'-t','4','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p',file],{windowsHide:true});
      const sha256=createHash('sha256').update(await readFile(file)).digest('hex'),bytes=(await stat(file)).size,indexedAssetId=`synthetic-${i}`;objects.push({bucket:chunks.bucket,path:chunkObjectPath(owner,source.id,indexedAssetId,sha256)});
      const uploaded=await chunks.upload(source.id,indexedAssetId,file,sha256,bytes);source.chunks.push({filename:`chunk_${i}.mp4`,indexedAssetId,startSeconds:i*4,endSeconds:(i+1)*4,storage:uploaded.reference});
    }
    await persistence.broadcasts.put(source);for(const c of source.chunks)await persistence.broadcasts.attachStorage!(source.id,c.indexedAssetId,c.storage!);
    const id=randomUUID();objects.push({bucket:exports.bucket,path:`${owner}/exports/${id}.mp4`});await media.begin(id,source.id,'Synthetic selected replay',3000,6000);
    const worker=new MediaWorker({root,queue:media,repository:id=>new SupabaseMedia(client,id),chunks:()=>chunks,exports:()=>exports});await worker.runOnce();
    const row=await media.export(id);assert.ok(row?.ready_at&&row.storage);const signed=await exports.sign(row.storage);const response=await fetch(signed.url,{signal:AbortSignal.timeout(30000),redirect:'error'});assert.equal(response.ok,true);const rendered=join(root,'rendered.mp4');await writeFile(rendered,Buffer.from(await response.arrayBuffer()));
    const {stdout}=await exec(process.env.FFPROBE_PATH||ffprobe.path,['-v','error','-show_entries','format=duration','-of','json',rendered],{windowsHide:true});assert.ok(Math.abs(Number(JSON.parse(stdout).format.duration)-3)<0.2);
    const secret=randomBytes(32),headers=(role:string)=>{const input=[{alg:'HS256'},{exp:Math.floor(Date.now()/1000)+600,channel_id:channel,opaque_user_id:'U'+channel,role}].map(x=>Buffer.from(JSON.stringify(x)).toString('base64url')).join('.');return {host:'verify.invalid',authorization:'Bearer '+input+'.'+createHmac('sha256',secret).update(input).digest('base64url')};};
    app=await buildTwitchEbs({config:{clientId:'synthetic',secrets:[secret],publicUrl:'https://verify.invalid',origins:[],port:3001},links,analysisConfigured:false,analysisModel:'pegasus1.5',scope:()=>({persistence,storage:chunks,searches:new SupabaseSearches(client,owner),media,exports})});
    assert.equal((await app.inject({method:'POST',url:'/extension/publication',headers:headers('broadcaster'),payload:{exportId:id,version:0}})).statusCode,200);
    const visible=await app.inject({url:'/viewer/selection',headers:headers('viewer')});assert.equal(visible.json().selection.title,'Synthetic selected replay');assert.doesNotMatch(visible.body,/question|snapshot|indexId/);
    await media.publish(channel,null,1);assert.equal((await app.inject({url:'/viewer/selection',headers:headers('viewer')})).json().selection,null);
    await media.purge(randomUUID(),source.id,source.title);await worker.runOnce();assert.equal(await persistence.broadcasts.get(source.id),undefined);assert.equal(await media.export(id),undefined);worker.stop();
    console.log('Real Supabase media verification passed: private source download, FFmpeg boundary export, immutable Storage upload, signed MP4 playback, publish/hide and scoped archive cleanup. Twitch/model inputs were synthetic; no provider indexing or analysis calls were made.');
  }finally{
    await app?.close();let complete=true;for(const object of objects)try{await exports.remove(object.bucket,object.path);}catch{complete=false;}
    try{if(ownerCreated&&complete){await db.query('delete from rewindr_broadcasters where id=$1',[owner]);console.log('Temporary verification metadata and private Storage objects removed.');}else if(ownerCreated){console.error('Verification cleanup incomplete; retained temporary owner for inspection: '+owner);process.exitCode=1;}}finally{await db.end();if(!root.startsWith(join(tmpdir(),'rewindr-media-verify-')))throw Error('Unsafe cleanup');await rm(root,{recursive:true,force:true});}
  }
}
main().catch(error=>{console.error(error instanceof assert.AssertionError?'Media verification assertion failed. Inspect the temporary fixture/worker status.':safeDatabaseError(error));process.exitCode=1;});
