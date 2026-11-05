// Capture-process uptime is separate from the footage's wall-clock timeline.
export function activeCaptureClock(state:{activeSeconds?:number},now=()=>performance.now()){
  state.activeSeconds??=0;let since:number|undefined;
  const used=()=>state.activeSeconds!+(since===undefined?0:Math.max(0,now()-since)/1000);
  const checkpoint=()=>{state.activeSeconds=used();if(since!==undefined)since=now();};
  return {used,checkpoint,start:()=>{if(since===undefined)since=now();},pause:()=>{checkpoint();since=undefined;}};
}
