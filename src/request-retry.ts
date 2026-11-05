export function retryableRequest(error:unknown){const status=(error as {status?:number})?.status;return status===undefined||status===408||status===429||status>=500;}
export function retryLoop<T>(options:{request:()=>Promise<T>;received:(value:T)=>void;failed:(error:unknown)=>void;again?:(value:T)=>boolean;
  maxAttempts?:number;initialMs?:number;intervalMs?:number;retryMs?:number}){
  let stopped=false,attempts=0,failures=0,timer:ReturnType<typeof setTimeout>;
  const schedule=(ms:number)=>{timer=setTimeout(()=>void run(),ms);};
  async function run(){attempts++;try{const value=await options.request();if(stopped)return;failures=0;options.received(value);if(options.again?.(value))schedule(options.intervalMs??1500);}
    catch(error){if(stopped)return;options.failed(error);failures++;if(retryableRequest(error)&&attempts<(options.maxAttempts??Infinity))schedule(Math.min(30000,(options.retryMs??5000)*2**Math.min(failures-1,4)));}}
  schedule(options.initialMs??0);return()=>{stopped=true;clearTimeout(timer);};
}
