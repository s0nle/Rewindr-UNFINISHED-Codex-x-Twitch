import {createHash,randomUUID} from 'node:crypto';
import type {Broadcast} from './types.js';
import {mapChunkMoments,chunkEncounters} from './chunk-search.js';
import {type SearchHit,type Interval,type Selection,type AnalysisPart,type Assessment,ANALYSIS_REVISION} from './search-types.js';
const digest=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const hitKey=(hit:{videoId:string;startSeconds:number;endSeconds:number})=>`${hit.videoId}:${hit.startSeconds.toFixed(3)}:${hit.endSeconds.toFixed(3)}`;
export function rejectedInterval(start:number,end:number,rejected:Interval[]) {return rejected.some(r=>start<r.endSeconds+2&&end>r.startSeconds-2);}
export function mergePool(existing:SearchHit[],incoming:SearchHit[]) {
  const unique=new Map<string,SearchHit>();for(const hit of [...existing,...incoming]){const key=`${hit.query}:${hitKey(hit)}`;const prior=unique.get(key);if(!prior||hit.rank<prior.rank)unique.set(key,hit);}
  return [...unique.values()].sort((a,b)=>a.rank-b.rank).slice(0,400);
}
export function selectCandidates(source:Broadcast,pool:SearchHit[],rejected:Interval[]):Selection[] {
  const scores=new Map<string,number>();const unique=new Map<string,SearchHit>();
  mapChunkMoments(source,pool);
  for(const raw of pool){const chunk=source.chunks.find(c=>c.indexedAssetId===raw.videoId)!;const hit={...raw,endSeconds:Math.min(raw.endSeconds,chunk.endSeconds-chunk.startSeconds)};
    if(rejectedInterval(hit.startSeconds+chunk.startSeconds,hit.endSeconds+chunk.startSeconds,rejected))continue;
    const key=hitKey(hit);scores.set(key,(scores.get(key)??0)+1/(60+hit.rank));const prior=unique.get(key);if(!prior||hit.rank<prior.rank)unique.set(key,hit);}
  const mapped=mapChunkMoments(source,[...unique.values()]);
  return chunkEncounters(source,mapped).filter(group=>!rejectedInterval(group.matchStartSeconds,group.matchEndSeconds,rejected)).map(group=>{
    const keys=group.providerMatches.map(match=>hitKey({videoId:match.videoId,startSeconds:match.chunkStartSeconds,endSeconds:match.chunkEndSeconds}));
    const candidateId=digest({broadcast:source.id,index:source.indexId,offset:source.sourceOffsetSeconds,start:group.matchStartSeconds,end:group.matchEndSeconds,keys});
    return {candidateId,matchStartSeconds:group.matchStartSeconds,matchEndSeconds:group.matchEndSeconds,startSeconds:group.startSeconds,endSeconds:group.endSeconds,
      bestProviderRank:group.bestProviderRank,fusionScore:Math.max(...keys.map(key=>scores.get(key)??0)),providerMatches:group.providerMatches};
  }).sort((a,b)=>b.fusionScore-a.fusionScore||a.matchStartSeconds-b.matchStartSeconds).slice(0,3);
}
export function planAnalysis(source:Broadcast,selection:Selection):AnalysisPart[] {
  const start=Math.max(0,selection.matchStartSeconds-5),end=selection.matchEndSeconds+5;const parts:AnalysisPart[]=[];
  for(const chunk of source.chunks){if(chunk.endSeconds<=start||chunk.startSeconds>=end)continue;
    const duration=chunk.endSeconds-chunk.startSeconds;if(duration<4)throw Error('Source part is too short to assess.');
    let from=Math.max(0,start-chunk.startSeconds),to=Math.min(duration,end-chunk.startSeconds);
    if(to-from<4){from=Math.max(0,Math.min(from,duration-4));to=Math.min(duration,Math.max(to,from+4));}
    if(!chunk.assetId&&!chunk.storage)throw Error('Source media is unavailable for analysis.');
    parts.push({id:randomUUID(),indexedAssetId:chunk.indexedAssetId,assetId:chunk.assetId,storage:chunk.storage,storageKey:chunk.storageKey,chunkStartSeconds:chunk.startSeconds,startSeconds:from,endSeconds:to});
  }
  if(!parts.length||parts.length>3)throw Error('Encounter exceeds the bounded analysis scope.');return parts;
}
export function analysisCacheKey(source:Broadcast,question:string,selection:Selection,parts:AnalysisPart[],model:string) {
  return digest({broadcast:source.id,index:source.indexId,question,core:[selection.matchStartSeconds,selection.matchEndSeconds],model,revision:ANALYSIS_REVISION,
    parts:parts.map(({id:_id,...part})=>part)});
}
export function combineAssessments(parts:{plan:AnalysisPart;assessment:Assessment}[]):Assessment {
  if(parts.length===1){const {assessment,plan}=parts[0]!;return {...assessment,evidence:assessment.evidence.map(e=>({...e,timestampSeconds:e.timestampSeconds+plan.chunkStartSeconds}))};}
  const values=parts.map(p=>p.assessment.verdict);const same=values.every(v=>v===values[0]);
  const verdict=same?values[0]!:'unclear';const summary=same?(verdict==='match'?'The requested details are supported across the analyzed sections.':verdict==='no_match'?'The analyzed sections do not support the requested encounter.':'Some requested details remain unclear across the analyzed sections.'):'The analyzed sections give mixed evidence; review the matching and unclear details.';
  const distinct=(items:string[])=>[...new Set(items)].slice(0,4);
  return {verdict,summary,matchingDetails:distinct(parts.flatMap(p=>p.assessment.matchingDetails)),unclearDetails:distinct(parts.flatMap(p=>p.assessment.unclearDetails)),
    evidence:parts.flatMap(({plan,assessment})=>assessment.evidence.map(e=>({...e,timestampSeconds:e.timestampSeconds+plan.chunkStartSeconds}))).slice(0,4)};
}
