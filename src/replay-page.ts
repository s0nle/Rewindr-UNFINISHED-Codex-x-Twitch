// A small private preview for backend testing, not the planned Twitch Extension UI.
export const replayPage = `<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Rewindr replay</title><style>
body{margin:0;background:#121319;color:#eee;font:16px system-ui;padding:24px}main{max-width:960px;margin:auto}
video{display:block;width:100%;background:#000;border-radius:12px}button{margin:12px 8px 12px 0;padding:10px 16px;cursor:pointer}
p{color:#bfc2d0}#error{color:#ffb5b5}input{width:100%}
</style><main><h1>Rewindr replay</h1><video controls playsinline></video>
<input id="seek" type="range" step="0.1" aria-label="Seek within replay">
<button id="play">Play replay</button><button id="refresh">Refresh playback links</button>
<p id="time">Loading private playback...</p><p id="error" role="alert"></p></main>
<script>
const video=document.querySelector('video'),seek=document.querySelector('#seek'),label=document.querySelector('#time'),error=document.querySelector('#error');
let manifest,index=0,switching=false;
const format=n=>[Math.floor(n/3600),Math.floor(n%3600/60),Math.floor(n%60)].map(v=>String(v).padStart(2,'0')).join(':');
function position(){const s=manifest?.segments[index];return s?s.recordingStartSeconds+Math.max(0,video.currentTime-s.startSeconds):0;}
function segmentAt(target){const found=manifest.segments.findIndex(s=>target<s.recordingEndSeconds);return found<0?manifest.segments.length-1:found;}
function loadSegment(next,seconds,play){
  switching=true;index=next;const s=manifest.segments[index];video.src=s.url;
  video.onloadedmetadata=()=>{video.currentTime=seconds;switching=false;if(play)video.play().catch(()=>{error.textContent='Press Play to continue.';});};
}
async function refresh(){
  const prior=manifest?position():undefined;const wasPlaying=!video.paused;
  error.textContent='';
  try{const url=location.pathname.replace(/\\/replay$/,'/playback')+location.search;
    const response=await fetch(url,{cache:'no-store'});const data=await response.json();if(!response.ok)throw Error(data.error||'Playback unavailable.');
    manifest=data;seek.min=data.startSeconds;seek.max=data.endSeconds;
    const target=Math.min(data.endSeconds,Math.max(data.startSeconds,prior??data.startSeconds));const next=segmentAt(target);
    const s=data.segments[next];loadSegment(next,s.startSeconds+Math.max(0,target-s.recordingStartSeconds),wasPlaying);seek.value=target;
  }catch(e){error.textContent=e.message;}
}
function advance(){if(switching||!manifest)return;if(index+1<manifest.segments.length)loadSegment(index+1,manifest.segments[index+1].startSeconds,true);else video.pause();}
video.addEventListener('timeupdate',()=>{if(!manifest||switching)return;const s=manifest.segments[index];
  seek.value=position();label.textContent='Recording '+format(position())+' · Original broadcast '+format(manifest.sourceOffsetSeconds+position());
  if(!video.paused&&video.currentTime>=s.endSeconds)advance();});
video.addEventListener('ended',advance);video.addEventListener('error',()=>{error.textContent='Could not load media. Refresh playback links and try again.';});
seek.addEventListener('change',()=>{if(!manifest)return;const target=Number(seek.value),play=!video.paused;
  const next=segmentAt(target);const s=manifest.segments[next];loadSegment(next,s.startSeconds+Math.max(0,target-s.recordingStartSeconds),play);});
document.querySelector('#play').onclick=()=>{if(!manifest)return;if(position()>=manifest.endSeconds-0.1){loadSegment(0,manifest.segments[0].startSeconds,true);return;}video.play().catch(()=>{error.textContent='Refresh playback links and try again.';});};
document.querySelector('#refresh').onclick=refresh;refresh();
</script></html>`;
