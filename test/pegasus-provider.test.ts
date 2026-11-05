import {test} from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import {TwelvelabsApiClient} from 'twelvelabs-js';
import {PegasusAnalysisProvider} from '../src/providers/pegasus.js';
test('installed SDK serializes clipped async Pegasus requests, structured output and idempotency headers',async()=>{
  const api=Fastify();let body:Record<string,unknown>|undefined;let header:unknown;
  const id='11111111-1111-4111-8111-111111111111';
  api.post('/analyze/tasks',async req=>{body=req.body as Record<string,unknown>;header=req.headers['idempotency-key'];return {task_id:'task-id',status:'queued'};});
  api.get('/analyze/tasks/task-id',async()=>({task_id:'task-id',status:'ready',created_at:new Date().toISOString(),result:{generation_id:'generation',finish_reason:'stop',usage:{input_tokens:10,output_tokens:10},
    data:JSON.stringify({verdict:'unclear',summary:'The pattern is unclear.',matchingDetails:['Sunglasses'],unclearDetails:['Floral pattern'],evidence:[{timestampSeconds:12,detail:'Sunglasses visible',modality:'visual'}]})}}));
  const address=await api.listen({host:'127.0.0.1',port:0});
  try{const provider=new PegasusAnalysisProvider(new TwelvelabsApiClient({apiKey:'test-key',baseUrl:address}));const part={id,indexedAssetId:'video',assetId:'asset',chunkStartSeconds:60,startSeconds:10,endSeconds:20};
    assert.equal(await provider.create(part,'Lady in floral dress'),'task-id');assert.equal(header,id);assert.equal(body!.model_name,'pegasus1.5');assert.deepEqual(body!.video,{type:'asset_id',asset_id:'asset'});
    assert.equal(body!.start_time,10);assert.equal(body!.end_time,20);assert.equal((body!.response_format as {type:string}).type,'json_schema');assert.match(String(body!.prompt),/Original user description/);
    const result=await provider.retrieve('task-id');assert.equal(result.assessment!.verdict,'unclear');assert.equal(result.assessment!.evidence[0]!.timestampSeconds,12);
  }finally{await api.close();}
});
