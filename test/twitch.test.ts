import {test,before,after} from 'node:test';
import assert from 'node:assert/strict';
import {createHmac,randomBytes,randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PGlite} from '@electric-sql/pglite';
import {createClient} from '@supabase/supabase-js';
import {applyTestMigrations} from './queue-shim.js';
import {oauthConfig,extensionConfig} from '../src/twitch-config.js';
import {verifyBroadcasterToken} from '../src/twitch-jwt.js';
import {TwitchApi} from '../src/twitch-api.js';
import {SupabaseTwitch} from '../src/twitch-store.js';
import {SupabasePersistence} from '../src/supabase-persistence.js';
import {SupabaseSearches} from '../src/search-store.js';
import {buildTwitchEbs} from '../src/twitch-ebs.js';
import {buildApp} from '../src/app.js';
import type {Broadcast,VideoSearchProvider} from '../src/types.js';
import type {ChunkStorage} from '../src/chunk-storage.js';
import {chunkObjectPath} from '../src/chunk-storage.js';
import type {SearchView} from '../src/search-service.js';

const secret=randomBytes(32),oldSecret=randomBytes(32),oauth={clientId:'application123',clientSecret:'never-return-this-secret',redirectUri:'http://localhost:3000/twitch/callback'};
const config=extensionConfig({TWITCH_EXTENSION_CLIENT_ID:'extension123',TWITCH_EXTENSION_SECRET:secret.toString('base64'),TWITCH_EBS_PUBLIC_URL:'https://ebs.example.test'});
function jwt(patch:Record<string,unknown>={},key=secret,header:Record<string,unknown>={alg:'HS256',typ:'JWT'}){
  const input=[header,{exp:Math.floor(Date.now()/1000)+600,channel_id:'123',opaque_user_id:'U123',role:'broadcaster',...patch}].map(v=>Buffer.from(JSON.stringify(v)).toString('base64url')).join('.');return input+'.'+createHmac('sha256',key).update(input).digest('base64url');
}
test('Extension configuration is optional for local mode, secrets decode, and public origins are exact',()=>{
  assert.equal(oauthConfig({TWITCH_REDIRECT_URI:oauth.redirectUri}),undefined);
  assert.throws(()=>oauthConfig({TWITCH_CLIENT_ID:'abc'}));
  assert.throws(()=>oauthConfig({TWITCH_CLIENT_ID:'abc',TWITCH_CLIENT_SECRET:'secret',TWITCH_REDIRECT_URI:'https://other.example/twitch/callback'}));
  assert.deepEqual(config.secrets,[secret]);assert.ok(config.origins.includes('https://extension123.ext-twitch.tv'));
  assert.throws(()=>extensionConfig({TWITCH_EXTENSION_CLIENT_ID:'abc',TWITCH_EXTENSION_SECRET:'plain-text-secret',TWITCH_EBS_PUBLIC_URL:'https://ebs.example.test'}));
  assert.throws(()=>extensionConfig({TWITCH_EXTENSION_CLIENT_ID:'abc',TWITCH_EXTENSION_SECRET:secret.toString('base64'),TWITCH_EBS_PUBLIC_URL:'https://ebs.example.test/path'}));
});
test('JWT verifier rejects tampering, expired/unsigned tokens, other roles and inconsistent broadcaster identities',()=>{
  assert.equal(verifyBroadcasterToken(jwt(),[secret],'extension123').channelId,'123');
  assert.equal(verifyBroadcasterToken(jwt({},oldSecret),[secret,oldSecret],'extension123').channelId,'123');
  for(const token of [jwt({},oldSecret),jwt({exp:1}),jwt({nbf:Date.now()/1000+100}),jwt({iat:Date.now()/1000+100}),jwt({opaque_user_id:'U999'}),jwt({user_id:'999'}),jwt({client_id:'wrong'}),jwt({},secret,{alg:'none'}),jwt({},secret,{alg:'HS256',crit:['x']}),'a.b.c',jwt().slice(0,-8)])assert.throws(()=>verifyBroadcasterToken(token,[secret],'extension123'),{statusCode:401});
  for(const role of ['viewer','moderator','external'])assert.throws(()=>verifyBroadcasterToken(jwt({role}),[secret],'extension123'),{statusCode:403});
});
function fakeTwitch(){
  const calls:{url:string;init:RequestInit}[]=[];let wrongClient=false,failStream=false,grants=0;
  const fetcher:typeof fetch=async(input,init={})=>{
    const url=String(input);calls.push({url,init});
    if(url.endsWith('/token')){grants++;const body=new URLSearchParams(String(init.body));assert.equal(body.get('client_secret'),oauth.clientSecret);return Response.json({access_token:body.get('grant_type')==='authorization_code'?'user-token':'app-token-'+grants,refresh_token:'discard-me',expires_in:7200,token_type:'bearer'});}
    if(url.endsWith('/validate'))return Response.json({client_id:wrongClient?'wrong':oauth.clientId,expires_in:7200,...(new Headers(init.headers).get('authorization')==='OAuth user-token'?{user_id:'123',login:'streamer'}:{user_id:null,login:null})});
    if(url.includes('/helix/users')){assert.equal(new Headers(init.headers).get('Client-Id'),oauth.clientId);return Response.json({data:[{id:'123',login:'streamer',display_name:'Streamer',profile_image_url:'https://images.example.test/avatar.png'}]});}
    if(url.includes('/helix/streams')){if(failStream){failStream=false;return new Response('',{status:401});}return Response.json({data:[{id:'stream1',user_id:'123',title:'Synthetic stream',game_name:'Just Chatting',started_at:new Date().toISOString(),viewer_count:10}]});}
    if(url.endsWith('/revoke'))return Response.json({});throw Error('Unexpected Twitch URL');
  };
  return {api:new TwitchApi(oauth,fetcher),calls,wrongClient:()=>{wrongClient=true;},failStream:()=>{failStream=true;},get grants(){return grants;}};
}
test('OAuth validates the app and user, revokes the temporary token and does not return tokens',async()=>{
  const s=fakeTwitch(),user=await s.api.identity('one-use-code');assert.equal(user.id,'123');assert.doesNotMatch(JSON.stringify(user),/token|secret|discard-me/);
  assert.ok(s.calls.some(c=>c.url.endsWith('/revoke')&&new URLSearchParams(String(c.init.body)).get('token')==='user-token'));
  const bad=fakeTwitch();bad.wrongClient();await assert.rejects(bad.api.identity('bad-code'),{statusCode:401});assert.ok(bad.calls.some(c=>c.url.endsWith('/revoke')));assert.ok(!bad.calls.some(c=>c.url.includes('/helix/users')));
});
test('public stream status coalesces app grants, validates app tokens, and refreshes after one 401',async()=>{
  const s=fakeTwitch();const result=await Promise.all([s.api.channel('123'),s.api.channel('123')]);assert.equal(s.grants,1);assert.ok(result.every(r=>r.online));
  s.failStream();assert.equal((await s.api.channel('123')).online,true);assert.equal(s.grants,2);await assert.rejects(s.api.channel('not-numeric'),{statusCode:400});
});

let db:PGlite;
before(async()=>{db=new PGlite();await db.exec('create role anon;create role authenticated;create role service_role bypassrls;');await applyTestMigrations(db);});
after(async()=>{await db?.close();});
const client=createClient('https://test.supabase.co','test-key',{auth:{persistSession:false},global:{fetch:async(input,init)=>{
  const url=new URL(String(input));try{
    if(url.pathname.includes('/rpc/')){
      const name=url.pathname.split('/').at(-1)!;assert.match(name,/^rewindr_[a-z_]+$/);const args=JSON.parse(String(init?.body)) as Record<string,unknown>,keys=Object.keys(args);keys.forEach(k=>assert.match(k,/^p_[a-z_]+$/));
      const result=await db.query<{v:unknown}>(`select ${name}(${keys.map((k,i)=>`${k}=>$${i+1}`).join(',')}) v`,keys.map(k=>args[k]!==null&&typeof args[k]==='object'?JSON.stringify(args[k]):args[k]));return Response.json(result.rows[0]!.v??null);
    }
    const table=url.pathname.split('/').at(-1)!;assert.ok(['rewindr_twitch_links','rewindr_broadcasts','rewindr_searches','rewindr_recordings','rewindr_chunks'].includes(table));const filters=[...url.searchParams].filter(([k])=>['id','owner_id','twitch_user_id'].includes(k));
    const rows=(await db.query(`select * from ${table}${filters.length?' where '+filters.map(([k],i)=>`${k}=$${i+1}`).join(' and '):''}`,filters.map(([,v])=>v.replace(/^eq\./,'')))).rows;return Response.json(rows);
  }catch(e){return Response.json({code:(e as {code?:string}).code??'TEST',message:'Database rejected request'},{status:400});}
}}});
const links=new SupabaseTwitch(client),user={id:'123',login:'streamer',display_name:'Streamer',profile_image_url:'https://images.example.test/avatar.png'};
async function owner(){const id=randomUUID();await db.query("insert into rewindr_broadcasters(id,label) values($1,'Twitch test')",[id]);return id;}
test('identity migration consumes browser-bound state once, expires it, prevents channel takeover, and revokes on disconnect',async()=>{
  const a=await owner(),b=await owner();try{
    await links.begin(a,'a'.repeat(64),'b'.repeat(64));await assert.rejects(links.consume('a'.repeat(64),'c'.repeat(64)));assert.equal(await links.consume('a'.repeat(64),'b'.repeat(64)),a);await assert.rejects(links.consume('a'.repeat(64),'b'.repeat(64)));
    await links.begin(a,'d'.repeat(64),'b'.repeat(64));await db.exec("update rewindr_twitch_oauth_states set expires_at=clock_timestamp()-interval '1 second'");await assert.rejects(links.consume('d'.repeat(64),'b'.repeat(64)));
    await links.link(a,user);await assert.rejects(links.link(b,user),{statusCode:409});await assert.rejects(links.link(a,{...user,id:'456'}),{statusCode:409});
    await links.link(a,{...user,display_name:'Updated'});assert.equal((await links.byChannel('123'))?.display_name,'Updated');
    await db.exec('set role authenticated');try{await assert.rejects(db.query('select * from rewindr_twitch_links'));await assert.rejects(db.query('select rewindr_twitch_unlink($1)',[a]));}finally{await db.exec('reset role');}
    await links.unlink(a);assert.equal(await links.byChannel('123'),undefined);await links.link(b,user);
  }finally{await db.query('delete from rewindr_broadcasters where id=any($1::uuid[])',[[a,b]]);}
});
test('local OAuth keeps capture private, binds callback to the same browser, allows only top-level callback navigation and disconnects',async t=>{
  const id=await owner(),root=await mkdtemp(join(tmpdir(),'rewindr-twitch-test-')),s=fakeTwitch();const app=await buildApp({dataDir:root,twitch:{ownerId:id,repository:links,api:s.api}});
  t.after(async()=>{await app.close();await db.query('delete from rewindr_broadcasters where id=$1',[id]);assert.ok(root.startsWith(join(tmpdir(),'rewindr-twitch-test-')));await rm(root,{recursive:true,force:true});});
  const alias=await app.inject({url:'/twitch/connect',headers:{host:'127.0.0.1:3000'}});assert.equal(alias.headers.location,'http://localhost:3000/twitch/connect');assert.equal(alias.headers['set-cookie'],undefined);
  const start=await app.inject({url:'/twitch/connect',headers:{host:'localhost:3000'}});assert.equal(start.statusCode,302,start.body);const authorization=new URL(start.headers.location!);assert.equal(authorization.origin,'https://id.twitch.tv');assert.equal(authorization.searchParams.get('client_id'),oauth.clientId);
  const state=authorization.searchParams.get('state')!,cookie=String(start.headers['set-cookie']).split(';')[0]!;assert.ok(String(start.headers['set-cookie']).includes('HttpOnly'));
  assert.equal((await app.inject({url:'/twitch/callback?state='+state+'&code=code',headers:{host:'localhost:3000'}})).statusCode,400);
  assert.equal((await app.inject({url:'/twitch/callback?state='+state+'&code=code',headers:{host:'localhost:3000',cookie,'sec-fetch-site':'cross-site','sec-fetch-mode':'cors','sec-fetch-dest':'empty'}})).statusCode,403);
  const callback=await app.inject({url:'/twitch/callback?state='+state+'&code=code',headers:{host:'localhost:3000',cookie,'sec-fetch-site':'cross-site','sec-fetch-mode':'navigate','sec-fetch-dest':'document'}});assert.equal(callback.statusCode,302,callback.body);assert.equal(callback.headers.location,'/?twitch=linked');
  assert.equal((await app.inject({url:'/twitch/callback?state='+state+'&code=code',headers:{host:'localhost:3000',cookie}})).statusCode,409);
  const status=await app.inject('/twitch/status');assert.equal(status.json().linked,true);assert.doesNotMatch(status.body,/user-token|app-token|discard-me|never-return-this-secret/);
  assert.equal((await app.inject({method:'POST',url:'/twitch/disconnect',headers:{origin:'https://hostile.test'}})).statusCode,403);
  assert.equal((await app.inject({url:'/recordings',headers:{'sec-fetch-site':'cross-site'}})).statusCode,403);
  assert.equal((await app.inject({method:'POST',url:'/twitch/disconnect'})).statusCode,200);assert.equal((await app.inject('/twitch/status')).json().linked,false);
});
test('Extension EBS scopes archive/search/replay by verified channel, denies viewers, and never exposes the local API',async t=>{
  const a=await owner(),b=await owner(),root=await mkdtemp(join(tmpdir(),'rewindr-twitch-test-'));await links.link(a,user);await links.link(b,{...user,id:'456'});
  const source:Broadcast={id:randomUUID(),title:'Private A',indexId:'test-index',sourceRecordingId:randomUUID(),createdAt:new Date().toISOString(),sourceOffsetSeconds:0,chunks:Array.from({length:6},(_,i)=>({filename:`chunk_${i}.mp4`,assetId:`asset${i}`,indexedAssetId:`video${i}`,startSeconds:i*60,endSeconds:(i+1)*60,storage:{bucket:'private-chunks',path:`${a}/chunk_${i}.mp4`,bytes:1000,sha256:'a'.repeat(64),uploadedAt:new Date().toISOString()}}))};
  const persistence=new SupabasePersistence(root,client,a);
  await db.query("insert into rewindr_recordings(id,owner_id,filename,bytes,sha256,creator_authorized,authorized_at,created_at,updated_at,phase) values($1,$2,'source.mp4',1000,$3,true,now(),now(),now(),'queued')",[source.sourceRecordingId!,a,'a'.repeat(64)]);
  for(const c of source.chunks)c.storage!.path=chunkObjectPath(a,source.id,c.indexedAssetId,c.storage!.sha256);
  await persistence.broadcasts.put(source);for(const c of source.chunks)await persistence.broadcasts.attachStorage!(source.id,c.indexedAssetId,c.storage!);
  const unexpected=async():Promise<never>=>{throw Error('Unexpected provider operation');},calls:string[]=[];
  const provider:VideoSearchProvider={name:'twelvelabs',ensureIndex:unexpected,search:unexpected,createUpload:unexpected,transfer:unexpected,assetStatus:unexpected,indexAsset:unexpected,indexStatus:unexpected,searchMany:async(_index,ids,question)=>{assert.deepEqual(ids,source.chunks.map(c=>c.indexedAssetId));calls.push(question);return source.chunks.map((c,i)=>({videoId:c.indexedAssetId,startSeconds:10,endSeconds:14,rank:i+1}));}};
  const storage:ChunkStorage={bucket:'private-chunks',init:async()=>{},upload:unexpected,download:unexpected,sign:async(bid,asset)=>({url:`https://storage.example.test/${bid}/${asset}?signed=1`,expiresAt:new Date(Date.now()+300000).toISOString()})};
  const app=await buildTwitchEbs({config,links,provider,analysisConfigured:false,analysisModel:'pegasus1.5',scope:id=>({persistence:new SupabasePersistence(root,client,id),searches:new SupabaseSearches(client,id),storage})});
  t.after(async()=>{await app.close();await db.query('delete from rewindr_broadcasters where id=any($1::uuid[])',[[a,b]]);assert.ok(root.startsWith(join(tmpdir(),'rewindr-twitch-test-')));await rm(root,{recursive:true,force:true});});
  const headers={host:'ebs.example.test',origin:'https://extension123.ext-twitch.tv',authorization:'Bearer '+jwt()};
  assert.equal((await app.inject({url:'/extension/broadcasts',headers:{host:'ebs.example.test'}})).statusCode,401);
  for(const role of ['viewer','moderator','external'])assert.equal((await app.inject({url:'/extension/broadcasts',headers:{...headers,authorization:'Bearer '+jwt({role})}})).statusCode,403);
  assert.equal((await app.inject({url:'/extension/broadcasts',headers:{...headers,origin:'https://hostile.test'}})).statusCode,403);
  const preflight=await app.inject({method:'OPTIONS',url:'/extension/broadcasts',headers:{...headers,authorization:'','access-control-request-headers':'authorization,ngrok-skip-browser-warning'}});
  assert.equal(preflight.statusCode,204);assert.match(String(preflight.headers['access-control-allow-headers']),/ngrok-skip-browser-warning/i);
  assert.equal((await app.inject({url:'/extension/broadcasts',headers:{host:'ebs.example.test','ngrok-skip-browser-warning':'1'}})).statusCode,401);
  for(const url of ['/recordings','/broadcasts','/jobs','/twitch/connect','/ingestions'])assert.equal((await app.inject({url,headers})).statusCode,404);
  const list=await app.inject({url:'/extension/broadcasts',headers});assert.equal(list.statusCode,200,list.body);assert.deepEqual(list.json().broadcasts.map((v:{id:string})=>v.id),[source.id]);assert.doesNotMatch(list.body,/sourceRecordingId|owner_id/);
  const other={...headers,authorization:'Bearer '+jwt({channel_id:'456',opaque_user_id:'U456'})};assert.equal((await app.inject({url:'/extension/broadcasts',headers:other})).json().broadcasts.length,0);
  const requestId=randomUUID(),question='Lady with black floral dress and black sunglasses walks by';
  const start=await app.inject({method:'POST',url:`/extension/broadcasts/${source.id}/searches`,headers,payload:{requestId,question}});assert.equal(start.statusCode,201,start.body);const view=start.json<SearchView>();assert.equal(view.candidates.length,3);assert.deepEqual(calls,[question]);assert.ok(view.candidates.every(c=>c.sourceUrl===null&&c.playbackUrl===null&&c.manifestUrl));
  assert.equal((await app.inject({method:'POST',url:`/extension/broadcasts/${source.id}/searches`,headers,payload:{requestId:randomUUID(),question,ownerId:b}})).statusCode,400);
  assert.equal((await app.inject({url:`/extension/searches/${view.searchId}`,headers:other})).statusCode,404);
  assert.equal((await app.inject({url:'/extension'+view.candidates[0]!.manifestUrl,headers:other})).statusCode,404);
  const replay=await app.inject({url:'/extension'+view.candidates[0]!.manifestUrl,headers});assert.equal(replay.statusCode,200,replay.body);assert.ok(replay.json().segments.length);
  const analysis=await app.inject({method:'POST',url:'/extension'+view.analysisEndpoint,headers,payload:{version:0}});assert.equal(analysis.statusCode,200,analysis.body);
  const recovery=await app.inject({method:'POST',url:`/extension/searches/${view.searchId}/recover`,headers,payload:{version:0,requestId:randomUUID()}});assert.equal(recovery.statusCode,200,recovery.body);assert.equal(recovery.json().rejectedIntervalCount,3);
  await links.unlink(a);assert.equal((await app.inject({url:`/extension/searches/${view.searchId}`,headers})).statusCode,403);assert.equal((await app.inject({url:'/extension/account',headers})).json().linked,false);
});
