import {useEffect,useRef,useState} from 'react';
const prefix=import.meta.env.DEV?'/api':'';
type Segment={url:string;startSeconds:number;endSeconds:number;recordingStartSeconds:number;recordingEndSeconds:number};
type Manifest={startSeconds:number;endSeconds:number;segments:Segment[]};
const format=(n:number)=>[Math.floor(n/3600),Math.floor(n%3600/60),Math.floor(n%60)].map(v=>String(v).padStart(2,'0')).join(':');
export function SegmentPlayer({manifestUrl,sourceUrl,startSeconds,endSeconds,offset,requestApi,tools}:{manifestUrl?:string;sourceUrl?:string|null;startSeconds:number;endSeconds:number;offset:number;requestApi?:<T>(path:string)=>Promise<T>;tools?:(position:number)=>React.ReactNode}) {
  const video=useRef<HTMLVideoElement>(null),manifest=useRef<Manifest|null>(null),part=useRef(0),switching=useRef(false),target=useRef(0),resume=useRef(false);
  const [range,setRange]=useState<[number,number]>([startSeconds,endSeconds]),[position,setPosition]=useState(startSeconds),[error,setError]=useState(''),[refresh,setRefresh]=useState(0);
  function load(index:number,seconds:number,play:boolean){const data=manifest.current;if(!data||!video.current)return;const segment=data.segments[index];
    part.current=index;target.current=seconds;resume.current=play;switching.current=true;video.current.src=segment.url;video.current.load();}
  useEffect(()=>{const controller=new AbortController();manifest.current=null;setError('');
    void(async()=>{try{
      let data:Manifest;
      if(manifestUrl){if(requestApi)data=await requestApi<Manifest>(manifestUrl);else{const response=await fetch(prefix+manifestUrl,{cache:'no-store',signal:controller.signal});if(!response.ok)throw Error('Preview is temporarily unavailable.');data=await response.json();}}
      else if(sourceUrl)data={startSeconds,endSeconds,segments:[{url:prefix+sourceUrl,startSeconds,endSeconds,recordingStartSeconds:startSeconds,recordingEndSeconds:endSeconds}]};
      else return;
      if(!data.segments.length)throw Error('No replay segments are available.');if(controller.signal.aborted)return;manifest.current=data;setRange([data.startSeconds,data.endSeconds]);setPosition(data.startSeconds);load(0,data.segments[0].startSeconds,false);
    }catch(e){if(!controller.signal.aborted)setError((e as Error).message);}})();
    return()=>{controller.abort();video.current?.pause();};
  },[manifestUrl,sourceUrl,startSeconds,endSeconds,refresh,requestApi]);
  function advance(){const data=manifest.current;if(!data||switching.current)return;if(part.current+1<data.segments.length){const next=part.current+1;load(next,data.segments[next].startSeconds,true);}else{video.current?.pause();setPosition(data.endSeconds);}}
  function update(){const data=manifest.current,media=video.current;if(!data||!media||switching.current)return;const segment=data.segments[part.current];
    setPosition(Math.min(data.endSeconds,segment.recordingStartSeconds+Math.max(0,media.currentTime-segment.startSeconds)));if(!media.paused&&media.currentTime>=segment.endSeconds)advance();}
  function seek(seconds:number){const data=manifest.current,media=video.current;if(!data||!media)return;const found=data.segments.findIndex(s=>seconds<s.recordingEndSeconds),index=found<0?data.segments.length-1:found,segment=data.segments[index];
    const local=segment.startSeconds+Math.max(0,seconds-segment.recordingStartSeconds);if(index===part.current&&media.readyState>=1){media.currentTime=local;setPosition(seconds);}else load(index,local,!media.paused);}
  if(!manifestUrl&&!sourceUrl)return <div className="no-preview">This candidate’s preview has not been stored yet.</div>;
  return <div className="segment-player"><video ref={video} controls playsInline preload="metadata" aria-label="Candidate replay"
    onLoadedMetadata={()=>{if(!video.current)return;video.current.currentTime=target.current;switching.current=false;if(resume.current)void video.current.play().catch(()=>setError('Press Play to continue.'));}}
    onTimeUpdate={update} onEnded={advance} onPlay={()=>{const data=manifest.current;if(data&&position>=data.endSeconds-0.1)load(0,data.segments[0].startSeconds,true);}}
    onError={()=>{if(manifest.current)setError('Playback link expired or media is unavailable. Refresh playback.');}}/>
    <div className="segment-controls"><input type="range" min={range[0]} max={range[1]} value={position} step="0.1" aria-label="Seek within candidate" onChange={e=>seek(Number(e.target.value))}/>
      <div><span>{format(offset+position)}</span><button type="button" onClick={()=>{const data=manifest.current;if(data)load(0,data.segments[0].startSeconds,true);}}>Replay</button><button type="button" onClick={()=>setRefresh(v=>v+1)}>Refresh playback</button></div>
      {error&&<p className="error" role="status">{error}</p>}</div>{tools?.(position)}</div>;
}
