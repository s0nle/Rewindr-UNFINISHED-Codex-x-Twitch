import { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';
import {BroadcastSearch} from './BroadcastSearch';
import {TwitchAccount} from './TwitchAccount';
import {MediaLibrary} from './MediaLibrary';

type Recording = { id:string; filename:string; phase:string; error?:string; media?:{durationSeconds:number}; uploadProgress?:{completedChunks:number;totalChunks:number} };
type Moment = {candidateNumber:number;bestProviderRank:number;startSeconds:number;endSeconds:number;matchStartSeconds:number;matchEndSeconds:number;sourceUrl:string;providerMatches:unknown[]};
const apiPrefix=import.meta.env.DEV?'/api':'';
const time=(seconds:number)=>`${Math.floor(seconds/60).toString().padStart(2,'0')}:${Math.floor(seconds%60).toString().padStart(2,'0')}`;
async function request<T>(path:string,options?:RequestInit):Promise<T>{
  const response=await fetch(`${apiPrefix}${path}`,options);
  const body=await response.json();
  if(!response.ok)throw new Error(body.error||`Request failed (${response.status})`);
  return body as T;
}
function Candidate({moment,recordingId,duration,onReference}:{moment:Moment;recordingId:string;duration:number;onReference:(file:File)=>void}){
  const [start,setStart]=useState(moment.startSeconds);
  const [end,setEnd]=useState(moment.endSeconds);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  const [download,setDownload]=useState('');
  const video=useRef<HTMLVideoElement>(null);
  const rangeValid=Number.isFinite(start)&&Number.isFinite(end)&&start>=0&&end>start&&end<=duration&&end-start<=300;
  function seek(seconds:number,play=false){if(!video.current)return;video.current.currentTime=Math.max(0,Math.min(seconds,duration));if(play)void video.current.play().catch(()=>{});}
  async function useFrame(){
    if(!video.current||video.current.readyState<2){setError('Play or seek the video to a clear frame first.');return;}
    try{
      const canvas=document.createElement('canvas');canvas.width=video.current.videoWidth;canvas.height=video.current.videoHeight;
      canvas.getContext('2d')!.drawImage(video.current,0,0);
      const blob=await new Promise<Blob>((resolve,reject)=>canvas.toBlob(value=>value?resolve(value):reject(new Error('Could not capture this frame.')),'image/jpeg',0.9));
      onReference(new File([blob],'reference-frame.jpg',{type:'image/jpeg'}));
    }catch(e){setError((e as Error).message);}
  }
  async function extract(){
    setBusy(true);setError('');setDownload('');
    try{
      const result=await request<{downloadUrl:string}>(`/recordings/${recordingId}/clips`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({startSeconds:start,endSeconds:end})});
      setDownload(result.downloadUrl);
    }catch(e){setError((e as Error).message);}finally{setBusy(false);}
  }
  return <article className="candidate">
    <div className="candidate-title"><span className="rank">{String(moment.candidateNumber).padStart(2,'0')}</span><h3>{time(start)} <span>→</span> {time(end)}</h3><span className="candidate-label">Best provider rank {moment.bestProviderRank}</span></div>
    <video ref={video} controls preload="metadata" src={`${apiPrefix}${moment.sourceUrl}`} onLoadedMetadata={()=>seek(start)}/>
    <div className="preview-tools"><button onClick={()=>seek(start,true)}>Play from clip start</button><button onClick={()=>seek(moment.matchStartSeconds,true)}>Jump to matched moment</button><button onClick={useFrame}>Search with current frame</button></div>
    <p className="playback-note">Matched {time(moment.matchStartSeconds)}–{time(moment.matchEndSeconds)} · {moment.providerMatches.length} search {moment.providerMatches.length===1?'segment':'segments'} · 30s of context added on each side. Playback continues past the clip end; the range below controls your download.</p>
    <div className="clip-tools"><label>Start · seconds<input type="number" min="0" max={duration} step="0.1" value={start} onChange={e=>{setStart(Number(e.target.value));setDownload('');}}/></label><label>End · seconds<input type="number" min="0" max={duration} step="0.1" value={end} onChange={e=>{setEnd(Number(e.target.value));setDownload('');}}/></label><button className="extract" onClick={extract} disabled={busy||!rangeValid}>{busy?'Extracting…':'Extract clip ↗'}</button>{download&&<a className="download" href={`${apiPrefix}${download}`} download>Download MP4 ↓</a>}</div>
    <div className="preview-tools"><button onClick={()=>{setStart(Math.max(0,start-15));setDownload('');}}>Add 15s before</button><button onClick={()=>{setEnd(Math.min(duration,end+15));setDownload('');}}>Add 15s after</button><button onClick={()=>{if(video.current){setStart(Math.floor(video.current.currentTime*10)/10);setDownload('');}}}>Set start here</button><button onClick={()=>{if(video.current){setEnd(Math.ceil(video.current.currentTime*10)/10);setDownload('');}}}>Set end here</button></div>
    {!rangeValid&&<p className="error playback-note">Select a start before the end, within this recording, and up to five minutes long.</p>}
    {error&&<p className="error" role="alert">{error}</p>}
  </article>;
}
function RecordingApp(){
  const [configured,setConfigured]=useState<boolean|null>(null);
  const [connectionError,setConnectionError]=useState(false);
  const [file,setFile]=useState<File|null>(null);
  const [authorized,setAuthorized]=useState(false);
  const [uploading,setUploading]=useState(false);
  const [progress,setProgress]=useState(0);
  const [recording,setRecording]=useState<Recording|null>(null);
  const [recordingId,setRecordingId]=useState(()=>localStorage.getItem('rewindr.recording')||'');
  const [question,setQuestion]=useState('');
  const [searching,setSearching]=useState(false);
  const [searched,setSearched]=useState(false);
  const [moments,setMoments]=useState<Moment[]>([]);
  const [visibleCount,setVisibleCount]=useState(6);
  const [reference,setReference]=useState<File|null>(null);
  const [imageOnly,setImageOnly]=useState(false);
  const [referencePreview,setReferencePreview]=useState('');
  const [error,setError]=useState('');
  const searchToken=useRef(0);
  useEffect(()=>{if(!reference){setReferencePreview('');return;}const url=URL.createObjectURL(reference);setReferencePreview(url);return()=>URL.revokeObjectURL(url);},[reference]);
  useEffect(()=>{
    let cancelled=false;let timer:ReturnType<typeof setTimeout>;
    async function refreshConfiguration(){
      try{
        const r=await request<{providerConfigured:boolean}>('/health',{signal:AbortSignal.timeout(5000)});
        if(!cancelled){setConfigured(r.providerConfigured);setConnectionError(false);}
      }catch{if(!cancelled){setConfigured(null);setConnectionError(true);}}
      finally{if(!cancelled)timer=setTimeout(refreshConfiguration,5000);}
    }
    void refreshConfiguration();
    return()=>{cancelled=true;clearTimeout(timer);};
  },[]);
  useEffect(()=>{
    if(!recordingId)return;
    let cancelled=false;let timer:ReturnType<typeof setTimeout>;
    async function poll(){
      try{
        const r=await request<Recording>(`/recordings/${recordingId}/status`);
        if(cancelled)return;setRecording(r);
        if(r.phase!=='ready'&&r.phase!=='failed')timer=setTimeout(poll,3000);
      }catch(e){if(!cancelled){setError((e as Error).message);timer=setTimeout(poll,5000);}}
    }
    void poll();return()=>{cancelled=true;clearTimeout(timer);};
  },[recordingId]);
  function upload(){
    if(!file||!authorized||configured!==true||uploading)return;
    if(file.size>4_000_000_000){setError('Choose an MP4 of up to 4 GB.');return;}
    setUploading(true);setProgress(0);setError('');setMoments([]);setSearched(false);setSearching(false);searchToken.current++;
    const xhr=new XMLHttpRequest();
    xhr.open('POST',`${apiPrefix}/recordings`);xhr.setRequestHeader('X-Creator-Authorized','true');
    xhr.upload.onprogress=e=>{if(e.lengthComputable)setProgress(Math.round(e.loaded/e.total*100));};
    xhr.onload=()=>{
      setUploading(false);
      try{const r=JSON.parse(xhr.responseText);if(xhr.status!==202)throw new Error(r.error||'Upload failed.');localStorage.setItem('rewindr.recording',r.recordingId);setRecording(null);setRecordingId(r.recordingId);}
      catch(e){setError((e as Error).message);}
    };
    xhr.onerror=()=>{setUploading(false);setError('Upload interrupted. Check the local server and try again.');};
    const body=new FormData();body.append('recording',file);xhr.send(body);
  }
  async function findMoments(referenceImage:File|null=reference){
    if(!recording||(!question.trim()&&!referenceImage))return;
    if(referenceImage&&referenceImage.size>5*1024*1024){setError('Choose a PNG or JPEG reference under 5 MB.');return;}
    const token=++searchToken.current;setSearching(true);setError('');setMoments([]);setSearched(false);setVisibleCount(6);
    try{
      let options:RequestInit;
      const query=referenceImage&&imageOnly?'':question.trim();
      if(referenceImage){const body=new FormData();body.append('question',query);body.append('reference',referenceImage);options={method:'POST',body};}
      else options={method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({question:query,limit:30})};
      const r=await request<{encounters:Moment[]}>(`/recordings/${recording.id}/search${referenceImage?'/reference':''}`,options);
      if(token===searchToken.current){setMoments(r.encounters);setSearched(true);}
    }
    catch(e){if(token===searchToken.current)setError((e as Error).message);}finally{if(token===searchToken.current)setSearching(false);}
  }
  function search(e:React.FormEvent){e.preventDefault();void findMoments();}
  function searchWithFrame(file:File){setReference(file);void findMoments(file);}
  const status=recording?.phase;
  const uploadBlocker=uploading?'':connectionError?'The local server is unavailable. Start it with npm.cmd start.':configured===null?'Checking the local server connection...':!configured?'Add TWELVELABS_API_KEY to .env, save it, and restart the server.':!file?'Choose your MP4 recording to enable upload.':!authorized?'Check the creator authorization box to enable upload.':'';
  const statusText:Record<string,string>={queued:'Waiting to process',probing:'Reading your video',creating_index:'Preparing visual search',creating_upload:'Preparing provider upload',uploading:'Sending video for visual indexing',asset_processing:'Processing your video',submitting_index:'Starting visual indexing',indexing:'Finding the details in your video',ready:'Ready to search',failed:'Processing failed'};
  return <><main>
    <div className="intro"><p className="eyebrow">YOUR STREAM. EVERY MOMENT.</p><h1>You remember the moment.<br/><span>We’ll find the timestamp.</span></h1><p className="subtitle">Upload a broadcast, describe what happened, and turn the moment into a clip.</p></div>
    {configured===false&&<div className="setup" role="status"><strong>Connect visual search to get started.</strong><p>Add your TwelveLabs API key to <code>.env</code> as <code>TWELVELABS_API_KEY</code>, then restart the server.</p></div>}
    <section className="panel"><div className="section-heading"><span className="step">01</span><h2>Add your broadcast</h2><span className="muted">20–30 minutes · MP4 · up to 4 GB</span></div>
      <label className={`dropzone ${file?'chosen':''}`} onDragOver={e=>e.preventDefault()} onDrop={e=>{e.preventDefault();if(!uploading)setFile(e.dataTransfer.files[0]||null);}}><input type="file" accept=".mp4,video/mp4" disabled={uploading} onChange={e=>setFile(e.target.files?.[0]||null)}/><span className="upload-icon">↑</span><strong>{file?file.name:'Drop a video here, or choose a file'}</strong><span>{file?`${(file.size/1024/1024).toFixed(1)} MB`:'Select an H.264 MP4 from your computer'}</span></label>
      <div className="upload-bottom"><label className="authorization"><input type="checkbox" checked={authorized} onChange={e=>setAuthorized(e.target.checked)}/> I’m authorized by the creator to upload and index this recording.</label><button onClick={upload} aria-describedby="upload-help" disabled={Boolean(uploadBlocker)||uploading}>{uploading?`Uploading ${progress}%`:'Upload & index →'}</button></div>
      <p id="upload-help" className="search-hint" role="status">{uploadBlocker||(!uploading?'Ready to upload and index your recording.':'')}</p>
      {uploading&&<progress max="100" value={progress} aria-label="Upload progress"/>}
      {recording&&<div className={`recording-status ${status}`}><span className={`status-dot ${status==='ready'?'done':''}`}/><div><strong>{statusText[status!]||status}</strong><p>{recording.filename}{recording.uploadProgress&&status==='uploading'?` · ${recording.uploadProgress.completedChunks}/${recording.uploadProgress.totalChunks} parts`:''}</p>{recording.error&&<p className="error">{recording.error}</p>}</div>{status==='ready'&&<span className="ready-tag">Indexed</span>}</div>}
    </section>
    <section className="search-section"><div className="section-heading"><span className="step">02</span><h2>What are you looking for?</h2></div><form onSubmit={search}><div className="query"><span>⌕</span><input aria-label="Describe a moment" placeholder="Find when the guy wearing a Barcelona jersey approached me…" maxLength={1000} value={question} onChange={e=>setQuestion(e.target.value)} disabled={uploading}/><button disabled={status!=='ready'||searching||uploading||(!question.trim()&&!reference)}>{searching?'Searching…':'Find moment →'}</button></div><div className="reference-tools"><label>Add a reference screenshot (optional)<input type="file" accept="image/png,image/jpeg" disabled={searching||uploading} onChange={e=>setReference(e.target.files?.[0]||null)}/></label>{reference&&<><label className="image-only"><input type="checkbox" checked={imageOnly} onChange={e=>setImageOnly(e.target.checked)}/> Match reference image only; ignore question text</label><button type="button" onClick={()=>setReference(null)}>Remove reference</button></>}{referencePreview&&<img src={referencePreview} alt="Reference image for this search"/>}</div></form><p className="search-hint">Describe visible clothing and actions. If text results look similar, add a screenshot cropped around the distinctive clothing, or use a frame from a result. No need to upload or index the video again.</p></section>
    {error&&<p className="error global-error" role="alert">{error}</p>}
    {moments.length>0&&<section className="results"><div className="section-heading"><h2>{moments.length} candidate encounters</h2><span className="muted">Adjacent matches combined</span></div><p className="search-hint">Results are candidates, not confirmed matches. Each includes context before and after the matched segments. Confirm the encounter, adjust the range, then extract an MP4.</p>{moments.slice(0,visibleCount).map(m=><Candidate key={`${recording!.id}-${searchToken.current}-${m.candidateNumber}`} moment={m} recordingId={recording!.id} duration={recording!.media!.durationSeconds} onReference={searchWithFrame}/>)}{visibleCount<moments.length&&<button className="show-more" onClick={()=>setVisibleCount(visibleCount+6)}>Show more candidates ({moments.length-visibleCount} remaining)</button>}</section>}
    {searched&&moments.length===0&&<div className="empty"><h3>No candidates returned</h3><p>Try a more concrete visual description. This doesn’t establish that the event was absent.</p></div>}
    <footer>Rewindr · Recorded-video prototype<span>Candidates need your review. Relevance rank is not a confidence score.</span></footer>
  </main></>;
}
function Workspace(){const[mode,setMode]=useState(new URLSearchParams(location.search).has('twitch')?'twitch':localStorage.getItem('rewindr.view')||'broadcasts');
  function select(value:string){setMode(value);localStorage.setItem('rewindr.view',value);}
  return <><header><a href="/" className="brand"><span className="brand-icon">↶</span> rewindr<span className="brand-dot">.</span></a>
    <nav className="workspace-nav" aria-label="Workspace"><button className={mode==='broadcasts'?'active':''} onClick={()=>select('broadcasts')}>Indexed streams</button><button className={mode==='media'?'active':''} onClick={()=>select('media')}>Clips & storage</button><button className={mode==='recording'?'active':''} onClick={()=>select('recording')}>Upload recording</button><button className={mode==='twitch'?'active':''} onClick={()=>select('twitch')}>Twitch</button></nav></header>
    {mode==='twitch'?<TwitchAccount/>:mode==='media'?<MediaLibrary/>:mode==='recording'?<RecordingApp/>:<BroadcastSearch/>}</>;}
createRoot(document.getElementById('root')!).render(<Workspace/>);
