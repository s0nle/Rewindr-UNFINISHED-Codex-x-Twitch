import {useEffect,useRef,useState} from 'react';
import {SegmentPlayer} from './SegmentPlayer';
import {retryLoop} from '../../src/request-retry';
import {ClipTools} from './ClipTools';
const prefix=import.meta.env.DEV?'/api':'';
const time=(n:number)=>[Math.floor(n/3600),Math.floor(n%3600/60),Math.floor(n%60)].map(v=>String(v).padStart(2,'0')).join(':');
type Assessment={verdict:'match'|'partial_match'|'no_match'|'unclear';summary:string;matchingDetails:string[];unclearDetails:string[];evidence:{timestampSeconds:number;detail:string;modality:string}[]};
type Candidate={candidateId:string;candidateNumber:number;broadcastMatchStartSeconds:number;broadcastMatchEndSeconds:number;startSeconds:number;endSeconds:number;
  replayAvailable:boolean;sourceUrl:string|null;manifestUrl?:string;analysis:{status:string;assessment:Assessment|null;error:string|null}};
type Search={searchId:string;broadcastId:string;question:string;version:number;phase:string;candidates:Candidate[];canRecover:boolean;recoveryCount:number;maxRecoveries:number;
  stopReason:string;queriesUsed:string[];warnings:string[];sourceOffsetSeconds:number;scopeEndSeconds:number;pendingIngestion:boolean};
type Broadcast={id:string;title:string;readyChunkCount:number;scopeEndSeconds:number;sourceOffsetSeconds:number};
class RequestError extends Error{constructor(message:string,readonly status:number){super(message);}}
async function localRequest<T>(path:string,body?:unknown):Promise<T>{const response=await fetch(prefix+path,body===undefined?{}:{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  const value=await response.json();if(!response.ok)throw new RequestError(value.error||'Request failed.',response.status);return value;}
function MatchDetails({candidate,offset}:{candidate:Candidate;offset:number}){
  const {analysis}=candidate,assessment=analysis.assessment;
  if(!assessment)return <div className="match-details" role="status"><strong>Match details</strong><p>{['queued','processing'].includes(analysis.status)?'Assessing the visible and audible details…':analysis.status==='not_requested'?'Match details are loading…':analysis.status==='needs_review'?'The assessment could not be confirmed. Review the video or try other candidates.':'Match details are unavailable. You can still review the video.'}</p></div>;
  const labels={match:'Details match',partial_match:'Some details match',no_match:'Details do not match',unclear:'Unclear'};
  return <div className="match-details"><div className="details-heading"><strong>Match details</strong><span className={`assessment-tag ${assessment.verdict}`}>{labels[assessment.verdict]}</span></div>
    <p>{assessment.summary}</p>{assessment.matchingDetails.length>0&&<div><h4>Seen or heard</h4><ul>{assessment.matchingDetails.map((detail,i)=><li key={i}>{detail}</li>)}</ul></div>}
    {assessment.unclearDetails.length>0&&<div><h4>Unclear or missing</h4><ul>{assessment.unclearDetails.map((detail,i)=><li key={i}>{detail}</li>)}</ul></div>}
    {assessment.evidence.length>0&&<details><summary>Evidence</summary><ul>{assessment.evidence.map((item,i)=><li key={i}><span className="evidence-time">{time(offset+item.timestampSeconds)}</span> {item.detail}</li>)}</ul></details>}
    <small>AI observations · verify in replay</small></div>;
}
export type SearchRequest=<T>(path:string,body?:unknown)=>Promise<T>;
export function BroadcastSearch({requestApi=localRequest,persist=true}:{requestApi?:SearchRequest;persist?:boolean}={}){
  const request=requestApi;
  const [broadcasts,setBroadcasts]=useState<Broadcast[]>([]),[selected,setSelected]=useState(''),[question,setQuestion]=useState(''),[modalities,setModalities]=useState(['visual']);
  const [search,setSearch]=useState<Search|null>(null),[busy,setBusy]=useState<'search'|'recover'|null>(null),[error,setError]=useState(''),[detailError,setDetailError]=useState('');
  const token=useRef(0),requestedDetails=useRef(new Set<string>()),recovery=useRef<{version:number;requestId:string}|null>(null);
  const [detailsAttempt,setDetailsAttempt]=useState(0);
  const [coverage,setCoverage]=useState<{readyChunkCount?:number;chunkCount?:number;scopeEndSeconds?:number;ingestion?:{latestCapturedSeconds:number;latestSearchableSeconds:number;gaps:{startSeconds:number;endSeconds:number}[];status:string}}|null>(null),[coverageError,setCoverageError]=useState('');
  const selectedBroadcast=broadcasts.find(b=>b.id===selected);
  useEffect(()=>{setCoverage(null);setCoverageError('');if(!selected)return;return retryLoop({request:()=>request<NonNullable<typeof coverage>>(`/broadcasts/${selected}/status`),received:value=>{setCoverage(value);setCoverageError('');},failed:e=>setCoverageError((e as Error).message),again:()=>true,intervalMs:10000});},[selected,request]);
  async function refreshBroadcasts(){const value=await request<{broadcasts:Broadcast[]}>('/broadcasts');setBroadcasts(value.broadcasts);setSelected(previous=>previous||value.broadcasts[0]?.id||'');}
  useEffect(()=>{let live=true;void refreshBroadcasts().catch(e=>{if(live)setError(e.message);});const saved=persist?localStorage.getItem('rewindr.search-session'):null;
    if(saved)void request<Search>(`/searches/${saved}`).then(value=>{if(live){setSearch(value);setSelected(value.broadcastId);setQuestion(value.question);}}).catch(()=>{if(persist)localStorage.removeItem('rewindr.search-session');});
    return()=>{live=false;token.current++;};},[]);
  const polling=Boolean(search&&(search.phase==='recovering'||search.candidates.some(c=>['queued','processing'].includes(c.analysis.status))));
  useEffect(()=>{if(!search||!polling||busy==='search')return;let cancelled=false;const id=search.searchId,currentToken=token.current;
    const stop=retryLoop({request:()=>request<Search>(`/searches/${id}`),initialMs:1000,
      received:value=>{if(!cancelled&&currentToken===token.current){setSearch(value);setDetailError('');}},failed:e=>{if(!cancelled&&currentToken===token.current)setDetailError((e as Error).message);},
      again:value=>!cancelled&&currentToken===token.current&&(value.phase==='recovering'||value.candidates.some(c=>['queued','processing'].includes(c.analysis.status)))});
    return()=>{cancelled=true;stop();};},[search?.searchId,search?.version,search?.phase,polling,busy]);
  useEffect(()=>{if(!search||search.phase!=='ready'||!search.candidates.length)return;const key=`${search.searchId}:${search.version}`;if(requestedDetails.current.has(key))return;requestedDetails.current.add(key);
    let cancelled=false,succeeded=false;const currentToken=token.current;
    const stop=retryLoop({maxAttempts:3,request:()=>{requestedDetails.current.add(key);return request<Search>(`/searches/${search.searchId}/analyses`,{version:search.version});},
      received:value=>{if(!cancelled&&currentToken===token.current){succeeded=true;setSearch(value);setDetailError('');}},
      failed:e=>{requestedDetails.current.delete(key);if(!cancelled&&currentToken===token.current)setDetailError((e as Error).message);}});
    return()=>{cancelled=true;stop();if(!succeeded)requestedDetails.current.delete(key);};},[search?.searchId,search?.version,search?.phase,detailsAttempt]);
  function retryDetails(){if(!search)return;requestedDetails.current.delete(`${search.searchId}:${search.version}`);setDetailError('');setDetailsAttempt(n=>n+1);}
  async function closeCurrent(){const id=search?.searchId;if(persist)localStorage.removeItem('rewindr.search-session');if(id)await request(`/searches/${id}/close`,{}).catch(()=>{});}
  async function find(event:React.FormEvent){event.preventDefault();if(!selected||!question.trim()||busy)return;const currentToken=++token.current;setBusy('search');setError('');setDetailError('');
    try{await closeCurrent();setSearch(null);const value=await request<Search>(`/broadcasts/${selected}/searches`,{requestId:crypto.randomUUID(),question:question.trim(),modalities,candidateLimit:30,operator:'or',transcriptionOptions:['lexical','semantic']});
      if(currentToken===token.current){setSearch(value);if(persist)localStorage.setItem('rewindr.search-session',value.searchId);recovery.current=null;}}
    catch(e){if(currentToken===token.current)setError((e as Error).message);}finally{if(currentToken===token.current)setBusy(null);}}
  async function recover(){if(!search||busy||!search.canRecover)return;const currentToken=++token.current;setBusy('recover');setError('');setDetailError('');
    const attempt=recovery.current?.version===search.version?recovery.current:{version:search.version,requestId:crypto.randomUUID()};recovery.current=attempt;
    try{const value=await request<Search>(`/searches/${search.searchId}/recover`,attempt);if(currentToken===token.current)setSearch(value);}
    catch(e){if(currentToken===token.current){setError((e as Error).message);const value=await request<Search>(`/searches/${search.searchId}`).catch(()=>null);if(value)setSearch(value);}}
    finally{if(currentToken===token.current)setBusy(null);}}
  function sourceChange(id:string){token.current++;void closeCurrent();setSearch(null);setSelected(id);setBusy(null);setError('');setDetailError('');}
  function toggle(value:string){setModalities(current=>current.includes(value)?current.length>1?current.filter(v=>v!==value):current:[...current,value]);}
  return <main className="broadcast-workspace"><div className="intro"><p className="eyebrow">YOUR STREAM. EVERY MOMENT.</p><h1>Remember a moment.<br/><span>Find it in your stream.</span></h1><p className="subtitle">Describe what happened. Review three candidates, then try other wording if the moment is still missing.</p></div>
    <section className="panel"><div className="section-heading"><span className="step">01</span><h2>Search your stream</h2></div>
      <div className="broadcast-picker"><label htmlFor="broadcast">Broadcast<select id="broadcast" value={selected} onChange={e=>sourceChange(e.target.value)} disabled={Boolean(busy)}><option value="" disabled>Choose a broadcast</option>{broadcasts.map(b=><option key={b.id} value={b.id}>{b.title}</option>)}</select></label>
        <button type="button" className="secondary" onClick={()=>void refreshBroadcasts().catch(e=>setError(e.message))}>Refresh streams</button></div>
      {!broadcasts.length&&<p className="search-hint">No indexed streams yet. Your stream appears here after its footage is prepared.</p>}
      {selectedBroadcast&&<p className="coverage">{coverage?.readyChunkCount??coverage?.chunkCount??selectedBroadcast.readyChunkCount} indexed sections · footage ending at {time(selectedBroadcast.sourceOffsetSeconds+(coverage?.scopeEndSeconds??coverage?.ingestion?.latestSearchableSeconds??selectedBroadcast.scopeEndSeconds))}</p>}
      {coverage?.ingestion&&<p className="search-hint" role="status">Latest captured: {time((selectedBroadcast?.sourceOffsetSeconds||0)+coverage.ingestion.latestCapturedSeconds)} · latest searchable: {time((selectedBroadcast?.sourceOffsetSeconds||0)+coverage.ingestion.latestSearchableSeconds)} · {coverage.ingestion.gaps.length} capture gap(s). New footage is included when you start a new search.</p>}{coverageError&&<p className="search-hint" role="status">Coverage update unavailable: {coverageError}</p>}
      <form onSubmit={find}><div className="query"><span aria-hidden="true">⌕</span><input aria-label="Describe the moment" value={question} maxLength={1000} onChange={e=>setQuestion(e.target.value)} placeholder="Lady with black floral dress and black sunglasses walks by"/>
        <button disabled={!selected||!question.trim()||Boolean(busy)}>{busy==='search'?'Searching…':'Find moment'}</button></div></form>
      <details className="source-options"><summary>Search sources</summary><div>{[['visual','Visual'],['transcription','Speech'],['audio','Audio / music']].map(([value,label])=><label key={value}><input type="checkbox" checked={modalities.includes(value)} onChange={()=>toggle(value)}/>{label}</label>)}</div></details>
      <p className="search-hint">Your first search uses your exact wording. Match details load separately so you can start reviewing immediately.</p></section>
    {error&&<p className="error global-error" role="alert">{error}</p>}
    {search&&<section className="results"><div className="section-heading"><h2>{search.candidates.length?`${search.candidates.length} candidate${search.candidates.length===1?'':'s'}`:search.phase==='recovering'?'Trying other wording…':'No candidates to review'}</h2><span className="muted">For “{search.question}”</span></div>
      {search.phase==='recovering'&&<p role="status" className="search-hint">Searching again. Rejected moments stay hidden in this search.</p>}
      {search.pendingIngestion&&<p className="empty">Your footage is still being indexed. Start a new search when more sections are ready.</p>}
      {!search.pendingIngestion&&search.phase!=='recovering'&&!search.candidates.length&&<p className="empty">No new candidates in this result pool. This does not establish that the event was absent.</p>}
      <div className="broadcast-candidates">{search.candidates.map(candidate=><article className="candidate" key={candidate.candidateId}><div className="candidate-title"><span className="rank">{candidate.candidateNumber}</span><h3>{time(candidate.broadcastMatchStartSeconds)} <span>–</span> {time(candidate.broadcastMatchEndSeconds)}</h3></div>
        <SegmentPlayer manifestUrl={candidate.manifestUrl} sourceUrl={candidate.sourceUrl} startSeconds={candidate.startSeconds} endSeconds={candidate.endSeconds} offset={search.sourceOffsetSeconds} requestApi={requestApi} tools={candidate.manifestUrl?position=><ClipTools request={request} broadcastId={search.broadcastId} start={candidate.startSeconds} end={candidate.endSeconds} offset={search.sourceOffsetSeconds} position={position}/>:undefined}/><MatchDetails candidate={candidate} offset={search.sourceOffsetSeconds}/></article>)}</div>
      {detailError&&<p className="search-hint" role="status">{detailError} Playback and recovery remain available.</p>}
      {(detailError||search.candidates.some(c=>['failed','unavailable'].includes(c.analysis.status)))&&<button type="button" className="secondary" onClick={retryDetails} disabled={search.phase!=='ready'||Boolean(busy)}>Retry match details</button>}
      {!search.pendingIngestion&&<div className="recovery-tools"><button className="recover-button" onClick={()=>void recover()} disabled={!search.canRecover||Boolean(busy)}>{busy==='recover'?'Trying other wording…':'Clip not found?'}</button><p>Hide these moments for this search and try wording variations of your original description.</p></div>}
      {search.stopReason==='recovery_limit'&&<p className="search-hint" role="status">This search has reached its recovery limit. Start a new search to try again.</p>}
      {search.stopReason==='pool_exhausted'&&<p className="search-hint" role="status">No further candidates remain in this retrieved pool.</p>}
      {search.warnings.map((warning,i)=><p key={i} className="search-hint" role="status">{warning}</p>)}
      {search.queriesUsed.length>1&&<details className="used-queries"><summary>Wording used for this search</summary><ul>{search.queriesUsed.map(query=><li key={query}>{query}</li>)}</ul></details>}
    </section>}
    <footer>Rewindr · Stream memory<span>Candidates and AI observations need your review.</span></footer></main>;
}
