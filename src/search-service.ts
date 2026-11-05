import type {Broadcast,VideoSearchProvider} from './types.js';
import type {SearchRepository} from './search-store.js';
import {type SearchRow,type SearchState,sessionSettingsSchema,SEARCH_MAX_RECOVERIES} from './search-types.js';
import {queryVariants} from './query-variants.js';
import {mergePool,selectCandidates,planAnalysis,analysisCacheKey} from './search-ranking.js';
import {candidatePlayback} from './playback.js';
import {mapChunkMoments} from './chunk-search.js';
import {HttpError} from './http-errors.js';

export class SearchService {
  constructor(private repository:SearchRepository,private provider:VideoSearchProvider|undefined,private options:{storageEnabled:boolean;analysisConfigured:boolean;model:'pegasus1.5'|'pegasus1.6'}){}
  private async lookup(id:string){const search=await this.repository.get(id);if(!search)throw new HttpError(404,'Search session not found.');if(Date.parse(search.expires_at)<Date.now())throw new HttpError(410,'This search session expired. Start a new search.');return search;}
  private async retrieve(source:Broadcast,question:string,settings:SearchRow['settings']) {
    if(!this.provider?.searchMany)throw new HttpError(503,'Configure TwelveLabs before searching.');
    const {candidateLimit,...modalities}=settings;
    const hits=await this.provider.searchMany(source.indexId,source.chunks.map(c=>c.indexedAssetId),question,candidateLimit,modalities);
    mapChunkMoments(source,hits);return hits.map(hit=>({...hit,query:question}));
  }
  async start(source:Broadcast,id:string,question:string,input:SearchRow['settings']) {
    const settings=sessionSettingsSchema.parse(input);const existing=await this.repository.get(id);
    if(existing){if(existing.broadcast_id!==source.id||existing.question!==question||JSON.stringify(existing.settings)!==JSON.stringify(settings))throw new HttpError(409,'Search request identity changed. Start a new search.');return this.view(await this.lookup(id));}
    await this.repository.reserveInitial?.(id,source.id);
    let pool:SearchState['pool']=[];
    if(source.chunks.length){try{pool=mergePool([],await this.retrieve(source,question,settings));}catch(error){if(error instanceof HttpError)throw error;throw new HttpError(502,'TwelveLabs search failed or returned invalid source matches.');}}
    const state:SearchState={pool,current:source.chunks.length?selectCandidates(source,pool,[]):[],rejected:[],usedQueries:[question],round:0,warnings:[],stopReason:'none'};
    return this.view(await this.repository.create(id,source,question,settings,state));
  }
  async get(id:string){return this.view(await this.lookup(id));}
  async close(id:string){await this.lookup(id);await this.repository.close(id);return {closed:true};}
  async recover(id:string,version:number,requestId:string) {
    const previous=await this.lookup(id);if(!previous.snapshot.chunks.length)throw new HttpError(409,'Footage is still indexing. Start a new search when chunks are ready.');
    const reserved=await this.repository.reserve(id,version,requestId);if(!reserved.owned)return this.view(reserved.search);
    const search=reserved.search;
    try {
      const variants=queryVariants(search.question).filter(q=>!search.state.usedQueries.includes(q)).slice(0,2);
      const results=await Promise.allSettled(variants.map(query=>this.retrieve(search.snapshot,query,search.settings)));
      let pool=search.state.pool;const used=[...search.state.usedQueries];let succeeded=0;const warnings:string[]=[];
      results.forEach((result,i)=>{if(result.status==='fulfilled'){pool=mergePool(pool,result.value);used.push(variants[i]!);succeeded++;}else warnings.push('Some wording variations could not be searched.');});
      const current=selectCandidates(search.snapshot,pool,search.state.rejected);
      if(variants.length&&succeeded===0&&!current.length)throw new HttpError(502,'Recovery search is temporarily unavailable. Previous rejections were kept.');
      const round=search.state.round+1;const moreVariants=queryVariants(search.question).some(q=>!used.includes(q));
      const state:SearchState={pool,current,rejected:search.state.rejected,usedQueries:used,round,warnings:[...new Set(warnings)],
        stopReason:round>=SEARCH_MAX_RECOVERIES?'recovery_limit':!current.length&&!moreVariants?'pool_exhausted':'none'};
      return this.view(await this.repository.commit(id,requestId,state));
    }catch(error){await this.repository.fail(id,requestId).catch(()=>{});throw error;}
  }
  async requestAnalyses(id:string,version:number) {
    const search=await this.lookup(id);if(search.version!==version||search.phase!=='ready')throw new HttpError(409,'Displayed candidates changed. Refresh the search.');
    let limited=false;
    const outcomes=await Promise.allSettled(search.state.current.map(async candidate=>{
      let plan:ReturnType<typeof planAnalysis>=[];try{plan=planAnalysis(search.snapshot,candidate);}catch{/* Keep playback usable when evidence cannot be analyzed. */}
      const key=analysisCacheKey(search.snapshot,search.question,candidate,plan,this.options.model);
      try{return await this.repository.requestAnalysis(search,candidate.candidateId,key,plan,this.options.model,this.options.analysisConfigured&&plan.length>0);}
      catch(error){if(error instanceof HttpError&&error.statusCode===429){limited=true;return this.repository.requestAnalysis(search,candidate.candidateId,key,plan,this.options.model,false);}throw error;}
    }));
    const view=await this.view(search);if(limited)view.warnings.push('Daily analysis allowance reached. Replay remains available; retry match details after the UTC reset.');if(outcomes.some(outcome=>outcome.status==='rejected'))view.warnings.push('Some match details could not be queued. Playback and recovery remain available.');return view;
  }
  private async view(search:SearchRow) {
    let details:Awaited<ReturnType<SearchRepository['analyses']>>=[];let unavailable=false;
    try{details=await this.repository.analyses(search.id);}catch{unavailable=true;}
    const activeRecovery=search.phase==='recovering'&&Date.parse(search.pending_until??'')>Date.now();
    const candidates=search.state.current.map((candidate,index)=>{
      const analysis=details.find(d=>d.candidateId===candidate.candidateId)?.analysis;
      const sourceUrl=search.snapshot.sourceRecordingId?`/recordings/${search.snapshot.sourceRecordingId}/source`:null;
      return {candidateId:candidate.candidateId,candidateNumber:index+1,bestProviderRank:candidate.bestProviderRank,
        matchStartSeconds:candidate.matchStartSeconds,matchEndSeconds:candidate.matchEndSeconds,startSeconds:candidate.startSeconds,endSeconds:candidate.endSeconds,
        broadcastMatchStartSeconds:search.snapshot.sourceOffsetSeconds+candidate.matchStartSeconds,broadcastMatchEndSeconds:search.snapshot.sourceOffsetSeconds+candidate.matchEndSeconds,
        replayAvailable:Boolean(sourceUrl),sourceUrl,playbackUrl:sourceUrl?`${sourceUrl}#t=${candidate.startSeconds}`:null,
        ...candidatePlayback(search.snapshot,candidate,this.options.storageEnabled),
        analysis:{status:analysis?.status??(unavailable?'unavailable':'not_requested'),assessment:analysis?.status==='ready'?analysis.result:null,
          error:analysis?.error??(unavailable?'Match details are temporarily unavailable.':null)}};
    });
    return {searchId:search.id,broadcastId:search.broadcast_id,question:search.question,settings:search.settings,version:search.version,
      phase:activeRecovery?'recovering':'ready',candidates,recoveryCount:search.state.round,maxRecoveries:SEARCH_MAX_RECOVERIES,
      canRecover:!activeRecovery&&search.snapshot.chunks.length>0&&search.state.stopReason==='none'&&search.state.round<SEARCH_MAX_RECOVERIES,
      stopReason:search.state.stopReason,rejectedIntervalCount:search.state.rejected.length,queriesUsed:search.state.usedQueries,
      rewriter:'constrained_rules',scopeEndSeconds:Math.max(0,...search.snapshot.chunks.map(c=>c.endSeconds)),sourceOffsetSeconds:search.snapshot.sourceOffsetSeconds,
      pendingIngestion:!search.snapshot.chunks.length,expiresAt:search.expires_at,warnings:[...search.state.warnings],analysisEndpoint:`/searches/${search.id}/analyses`,
      uncertainty:'Candidates and AI observations need review. Relevance ranks and assessments are not calibrated confidence scores.'};
  }
}
export type SearchView=Awaited<ReturnType<SearchService['get']>>;
