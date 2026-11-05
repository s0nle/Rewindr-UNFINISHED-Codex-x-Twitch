import {randomUUID} from 'node:crypto';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {z} from 'zod';
import {broadcastSchema} from '../src/metadata.js';
import {broadcastEvalManifest,broadcastFingerprint,overlapCandidate,latencySummary} from '../src/broadcast-evaluation.js';
import type {SearchView} from '../src/search-service.js';
async function main(){
  const [mode,input,output]=process.argv.slice(2);if(!['init','run'].includes(mode||'')||!input)throw Error('Usage: npm run evaluate:broadcast -- init BROADCAST_UUID [output.json] OR run verified-manifest.json');
  const base=new URL(`http://127.0.0.1:${Number(process.env.PORT||3000)}`);
  async function request<T>(path:string,body?:unknown):Promise<T>{const response=await fetch(new URL(path,base),{method:body===undefined?'GET':'POST',headers:body===undefined?{}:{'content-type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(120000),redirect:'error'});if(!response.ok)throw Error(`Evaluation request returned ${response.status}. No accuracy claim was made.`);return await response.json() as T;}
  if(mode==='init'){
    z.uuid().parse(input);const source=broadcastSchema.parse(await request(`/broadcasts/${input}/status`));if(!source.chunks.length)throw Error('Wait until source chunks are indexed.');const path=resolve(output||'.local/broadcast-evaluation.json');await mkdir(resolve(path,'..'),{recursive:true});
    await writeFile(path,JSON.stringify({version:1,broadcastId:source.id,sourceFingerprint:broadcastFingerprint(source),verifiedBy:'',verifiedAt:null,queries:['Lady with black floral dress and black sunglasses walks by','Guy with a Polo Ralph Lauren hoodie asks for a photo','A song plays in the car','On the beach, a man in black sunglasses asks for a picture','A phone shows someone doing a backflip'].map((question,i)=>({id:`q${i+1}`,question,kind:'positive',manuallyVerified:false,modalities:['visual'],expectedRanges:[]}))},null,2),{flag:'wx'});console.log(`Saved unverified template: ${path}. Label source-relative expected intervals and modalities; fill the verifier/date before running.`);return;
  }
  const manifest=broadcastEvalManifest.parse(JSON.parse(await readFile(resolve(input),'utf8'))),source=broadcastSchema.parse(await request(`/broadcasts/${manifest.broadcastId}/status`));if(broadcastFingerprint(source)!==manifest.sourceFingerprint)throw Error('The indexed source changed. Re-label the current snapshot before evaluating.');
  const rows:unknown[]=[],firstLatency:number[]=[],recoveryLatency:number[]=[];let positives=0,overlap=0,recovered=0;
  for(const query of manifest.queries){
    const started=performance.now();const first=await request<SearchView>(`/broadcasts/${manifest.broadcastId}/searches`,{requestId:randomUUID(),question:query.question,modalities:query.modalities});const firstMs=performance.now()-started;firstLatency.push(firstMs);
    const overlapRank=overlapCandidate(query.expectedRanges,first.candidates);let next:SearchView|null=null,nextMs:number|null=null;
    if(query.kind==='positive'){positives++;if(overlapRank)overlap++;else if(first.canRecover){const tick=performance.now();next=await request(`/searches/${first.searchId}/recover`,{version:first.version,requestId:randomUUID()});nextMs=performance.now()-tick;recoveryLatency.push(nextMs);if(overlapCandidate(query.expectedRanges,next!.candidates))recovered++;}}
    rows.push({query,searchId:first.searchId,firstMs,temporalOverlapRank:overlapRank,first,recoveryMs:nextMs,recovery:next});console.log(`${query.id}: first ${firstMs.toFixed(0)} ms, temporal overlap rank ${overlapRank??'none'}. Human identity review remains required.`);
  }
  const folder=resolve('.local/evaluation-reports');await mkdir(folder,{recursive:true});const path=join(folder,new Date().toISOString().replaceAll(/[:.]/g,'-')+'.json');
  await writeFile(path,JSON.stringify({evidence:'live-provider-retrieval-unreviewed',sourceFingerprint:manifest.sourceFingerprint,manifest,summary:{positiveQueries:positives,temporalOverlapAt3:overlap,additionalOverlapAfterOneRecovery:recovered,searchLatency:latencySummary(firstLatency),recoveryLatency:latencySummary(recoveryLatency)},rows,humanReview:manifest.queries.map(q=>({id:q.id,inspectedTopThree:false,acceptedCandidate:null,notes:''})),qualityGate:'PENDING HUMAN REVIEW. Temporal overlap does not establish the right person/event; absent queries are not automatically classified.'},null,2),{flag:'wx'});
  console.log(`Saved ${path}. No calibrated confidence or retrieval accuracy pass is claimed.`);
}
main().catch(error=>{console.error(error instanceof z.ZodError?'Invalid/unverified evaluation manifest. Label it before running.':error instanceof Error?error.message:'Broadcast evaluation failed.');process.exitCode=1;});
