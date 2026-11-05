import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import multipart from '@fastify/multipart';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TwelvelabsApiClient } from 'twelvelabs-js';
import { TwelveLabsProvider } from '../src/providers/twelvelabs.js';

test('real SDK serialization against mock HTTP provider: multipart transport, asset readiness, indexing and visual search', async () => {
  const api=Fastify(); await api.register(multipart);
  const bodies: Record<string,unknown>[]=[];
  const chunks: Buffer[]=[];
  let badSearch=false, failedPut=false;
  api.addContentTypeParser('application/octet-stream',{parseAs:'buffer'},(_req,body,done)=>done(null,body));
  api.addHook('onRequest',async req=>{ if (!req.url.startsWith('/part/')) assert.equal(req.headers['x-api-key'],'mock-secret'); });
  api.post('/v1.3/indexes',async req=>{ bodies.push(req.body as Record<string,unknown>); return {_id:'index'}; });
  api.get('/v1.3/indexes/index',async()=>({_id:'index',models:[{model_name:'marengo3.0',model_options:['visual','audio']}]}));
  api.post('/v1.3/assets/multipart-uploads',async req=>{ bodies.push(req.body as Record<string,unknown>); return {upload_id:'upload',asset_id:'asset',chunk_size:4,total_chunks:3,upload_headers:{'content-type':'application/octet-stream'}}; });
  api.post('/v1.3/assets/multipart-uploads/upload/presigned-urls',async req=>{
    const {start}=req.body as {start:number};
    return {upload_id:'upload',upload_urls:[{chunk_index:start,url:`${address}/part/${start}`}]};
  });
  api.put('/part/:part',async(req,reply)=>{
    if (!failedPut) { failedPut=true; return reply.code(503).send(); }
    assert.equal(req.headers['x-api-key'],undefined);
    chunks.push(req.body as Buffer); return reply.header('etag','"part-etag"').send();
  });
  api.post('/v1.3/assets/multipart-uploads/upload',async req=>{
    bodies.push(req.body as Record<string,unknown>); return {asset_id:'asset',processed_chunks:1};
  });
  api.get('/v1.3/assets/asset',async()=>({_id:'asset',status:'ready'}));
  api.post('/v1.3/indexes/index/indexed-assets',async req=>{bodies.push(req.body as Record<string,unknown>);return {_id:'indexed',asset_id:'asset'};});
  api.get('/v1.3/indexes/index/indexed-assets/indexed',async()=>({_id:'indexed',status:'ready'}));
  api.post('/v1.3/search',async req=>{
    const fields: Record<string,string>={};
    for await (const part of req.parts()) { assert.equal(part.type,'field'); if (part.type==='field') fields[part.fieldname]=String(part.value); }
    assert.equal(fields.search_options,'visual'); assert.equal(fields.group_by,'clip'); assert.equal(fields.filter,JSON.stringify({id:['indexed']}));
    assert.equal(fields.query_text,'Barcelona jersey');
    return {data:[{video_id:badSearch?'other-video':'indexed',start:10,end:20,rank:2},{video_id:'indexed',start:30,end:40,rank:1}]};
  });
  const address=await api.listen({host:'127.0.0.1',port:0});
  const folder=await mkdtemp(join(tmpdir(),'rewindr-adapter-'));
  try {
    const provider=new TwelveLabsProvider(new TwelvelabsApiClient({apiKey:'mock-secret',baseUrl:`${address}/v1.3`}));
    assert.equal(await provider.ensureIndex('recording'),'index');
    const file=join(folder,'source.mp4'); await writeFile(file,'abcdefghij');
    const session=await provider.createUpload(file,'source.mp4',10);
    const checkpoints: number[]=[];
    await provider.transfer(file,10,session,async n=>{checkpoints.push(n);});
    assert.deepEqual(checkpoints,[1,2,3]); assert.equal(Buffer.concat(chunks).toString(),'abcdefghij');
    assert.equal(await provider.assetStatus('asset'),'ready');
    assert.equal(await provider.indexAsset('index','asset'),'indexed');
    assert.equal(await provider.indexStatus('index','indexed'),'ready');
    const results=await provider.search('index','indexed','Barcelona jersey',3);
    assert.equal(results[0]!.rank,1); assert.equal(results[0]!.startSeconds,30);
    assert.deepEqual(bodies[0],{index_name:'rewindr-recording',models:[{model_name:'marengo3.0',model_options:['visual','audio']}]});
    assert.ok(bodies.some(b=>b.asset_id==='asset' && b.enable_video_stream===false));
    badSearch=true;
    await assert.rejects(provider.search('index','indexed','Barcelona jersey',3),/another source/);
    const reused=new TwelveLabsProvider(new TwelvelabsApiClient({apiKey:'mock-secret',baseUrl:`${address}/v1.3`}),'index');
    assert.equal(await reused.ensureIndex('recording'),'index');
  } finally { await api.close(); await rm(folder,{recursive:true,force:true}); }
});
