import { readFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';

const key=process.env.TWELVELABS_API_KEY?.trim();
if(!key)throw new Error('TWELVELABS_API_KEY is missing');
const root=resolve(process.env.DATA_DIR||'.local/rewindr');
const records=await Promise.all((await readdir(resolve(root,'records'))).filter(p=>p.endsWith('.json')).map(async p=>JSON.parse(await readFile(resolve(root,'records',p),'utf8'))));
const latest=records.filter(r=>r.indexId).sort((a,b)=>b.createdAt.localeCompare(a.createdAt))[0];
const requests=[{route:'/assets/multipart-uploads'},...(latest?[{route:`/indexes/${latest.indexId}`}]:[])];
if(process.argv.includes('--create-upload-session')&&latest)requests.push({route:'/assets/multipart-uploads',method:'POST',body:{filename:latest.filename,type:'video',total_size:latest.bytes}});
for(const {route,method='GET',body:requestBody} of requests){
  try{
    const response=await fetch(`https://api.twelvelabs.io/v1.3${route}`,{method,headers:{'x-api-key':key,...(requestBody?{'content-type':'application/json'}:{})},body:requestBody?JSON.stringify(requestBody):undefined,signal:AbortSignal.timeout(20000)});
    const body=await response.json();
    const redact=(value)=>typeof value==='string'?value.replaceAll(key,'[redacted]').replace(/https?:\/\/\S+/g,'[url]'):value;
    console.log(JSON.stringify({route,method,status:response.status,keys:Object.keys(body),code:redact(body.code),message:redact(body.message),error:typeof body.error==='string'?redact(body.error):undefined,detail:typeof body.detail==='string'?redact(body.detail):undefined,indexId:body._id,models:body.models,session:body.upload_id?{uploadId:body.upload_id,assetId:body.asset_id,chunkSize:body.chunk_size,totalChunks:body.total_chunks}:undefined,sessions:body.data?.map(s=>({uploadId:s.upload_id,status:s.status,totalSize:s.total_size,chunkSize:s.chunk_size,totalChunks:s.total_chunks,createdAt:s.created_at}))}));
  }catch(error){console.log(JSON.stringify({route,error:error.name,code:error.cause?.code}));process.exitCode=1;}
}
