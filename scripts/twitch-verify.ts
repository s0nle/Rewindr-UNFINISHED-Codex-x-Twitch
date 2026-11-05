import assert from 'node:assert/strict';
import {createHmac,randomBytes,randomUUID} from 'node:crypto';
import {resolve} from 'node:path';
import {databaseClient,safeDatabaseError} from '../src/db-connection.js';
import {configuredSupabaseClient,SupabasePersistence} from '../src/supabase-persistence.js';
import {SupabaseTwitch} from '../src/twitch-store.js';
import {SupabaseSearches} from '../src/search-store.js';
import {buildTwitchEbs} from '../src/twitch-ebs.js';
import type {VideoSearchProvider,Broadcast} from '../src/types.js';
import type {ChunkStorage} from '../src/chunk-storage.js';

// Real PostgreSQL/PostgREST, isolated disposable owners, synthetic search/JWT.
// This does not contact Twitch or TwelveLabs or upload any video.
async function main(){
  const db=await databaseClient();await db.connect();const owners=[randomUUID(),randomUUID()],secret=randomBytes(32),client=configuredSupabaseClient(),links=new SupabaseTwitch(client);
  const channels=owners.map(id=>'999'+BigInt('0x'+id.replaceAll('-','').slice(0,20)).toString());let app:Awaited<ReturnType<typeof buildTwitchEbs>>|undefined;
  const unexpected=async():Promise<never>=>{throw Error('Unexpected external operation');};
  try{
    for(const id of owners)await db.query("insert into rewindr_broadcasters(id,label) values($1,'Temporary Twitch verification')",[id]);
    const user=(id:string)=>({id,login:'rewindr_verify',display_name:'Synthetic verification',profile_image_url:'https://example.invalid/avatar.png'});
    const state=randomBytes(32).toString('hex'),browser=randomBytes(32).toString('hex');await links.begin(owners[0]!,state,browser);assert.equal(await links.consume(state,browser),owners[0]);await assert.rejects(links.consume(state,browser));
    await links.link(owners[0]!,user(channels[0]!));await links.link(owners[1]!,user(channels[1]!));await assert.rejects(links.link(owners[1]!,user(channels[0]!)));
    const source:Broadcast={id:randomUUID(),title:'Temporary Twitch verification',indexId:'synthetic-index',sourceOffsetSeconds:0,createdAt:new Date().toISOString(),chunks:Array.from({length:4},(_,i)=>({filename:`chunk_${i}.mp4`,indexedAssetId:`synthetic-${i}`,startSeconds:i*60,endSeconds:(i+1)*60}))};
    const root=resolve(process.env.DATA_DIR||'.local/rewindr'),persistence=new SupabasePersistence(root,client,owners[0]!);await persistence.broadcasts.put(source);
    const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,searchMany:async()=>source.chunks.map((c,i)=>({videoId:c.indexedAssetId,startSeconds:10,endSeconds:14,rank:i+1}))};
    const storage:ChunkStorage={bucket:'unused',init:async()=>{},sign:unexpected,upload:unexpected};
    app=await buildTwitchEbs({config:{clientId:'synthetic',secrets:[secret],publicUrl:'https://ebs.example.invalid',origins:['https://synthetic.ext-twitch.tv'],port:3001},links,provider,analysisConfigured:false,analysisModel:'pegasus1.5',scope:owner=>({persistence:new SupabasePersistence(root,client,owner),searches:new SupabaseSearches(client,owner),storage})});
    const token=(channel:string,role='broadcaster')=>{const input=[{alg:'HS256',typ:'JWT'},{exp:Math.floor(Date.now()/1000)+600,role,channel_id:channel,opaque_user_id:'U'+channel}].map(v=>Buffer.from(JSON.stringify(v)).toString('base64url')).join('.');return input+'.'+createHmac('sha256',secret).update(input).digest('base64url');};
    const headers=(channel:string,role?:string)=>({host:'ebs.example.invalid',origin:'https://synthetic.ext-twitch.tv',authorization:'Bearer '+token(channel,role)});
    const a=headers(channels[0]!),b=headers(channels[1]!);
    const list=await app.inject({url:'/extension/broadcasts',headers:a});assert.equal(list.statusCode,200);assert.equal(list.json().broadcasts[0].id,source.id);
    assert.equal((await app.inject({url:'/extension/broadcasts',headers:b})).json().broadcasts.length,0);
    assert.equal((await app.inject({url:'/extension/broadcasts',headers:headers(channels[0]!,'viewer')})).statusCode,403);
    const start=await app.inject({method:'POST',url:`/extension/broadcasts/${source.id}/searches`,headers:a,payload:{requestId:randomUUID(),question:'A person walks past the camera'}});assert.equal(start.statusCode,201);assert.equal(start.json().candidates.length,3);
    assert.equal((await app.inject({url:`/extension/searches/${start.json().searchId}`,headers:b})).statusCode,404);
    await links.unlink(owners[0]!);assert.equal((await app.inject({url:'/extension/broadcasts',headers:a})).statusCode,403);
    console.log('Twitch storage/EBS verification passed against real PostgreSQL and PostgREST. Identity state, channel isolation, search and disconnect checked using synthetic Twitch/model inputs.');
  }finally{
    await app?.close();try{await db.query('delete from rewindr_broadcasters where id=any($1::uuid[])',[owners]);console.log('Temporary verification owners and dependent records removed.');}finally{await db.end();}
  }
}
main().catch(error=>{console.error(error instanceof assert.AssertionError?'Twitch verification assertion failed; check migration and channel scoping.':safeDatabaseError(error));process.exitCode=1;});
