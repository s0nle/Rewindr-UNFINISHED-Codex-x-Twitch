import {runLiveCapture} from '../src/live-capture.js';
import {IngestionInputError} from '../src/ingestion-client.js';
const controller=new AbortController();process.once('SIGINT',()=>controller.abort());process.once('SIGTERM',()=>controller.abort());
async function main(){const args=process.argv.slice(2),values=new Map<string,string>(),flags=new Set<string>();
  for(let i=0;i<args.length;i++){const key=args[i]!;if(['--creator-authorized','--finish','--keep-local'].includes(key)){if(flags.has(key))throw new IngestionInputError('Duplicate flag.');flags.add(key);continue;}
    if(!['--resume','--workspace','--title','--index-id','--input-env','--rtmp-port','--max-seconds','--source-offset-seconds','--max-spool-mb'].includes(key)||!args[i+1]||values.has(key))throw new IngestionInputError('Invalid arguments. See docs/live-capture.md.');values.set(key,args[++i]!);}
  const number=(key:string)=>values.has(key)?Number(values.get(key)):undefined;
  await runLiveCapture({creatorAuthorized:flags.has('--creator-authorized'),finish:flags.has('--finish'),keepLocal:flags.has('--keep-local'),signal:controller.signal,
    resume:values.get('--resume'),workspace:values.get('--workspace'),title:values.get('--title'),indexId:values.get('--index-id')||(values.has('--resume')?undefined:process.env.TWELVELABS_INDEX_ID)||undefined,
    inputEnv:values.get('--input-env'),rtmpPort:number('--rtmp-port'),maxSeconds:number('--max-seconds'),offset:number('--source-offset-seconds'),
    maxSpoolBytes:values.has('--max-spool-mb')?Number(values.get('--max-spool-mb'))*1_000_000:undefined,dataDir:process.env.DATA_DIR,port:Number(process.env.PORT||3000)});
}
main().catch(error=>{console.error(error instanceof IngestionInputError?error.message:'Live capture stopped. Check FFmpeg, input configuration, API/worker and the saved workspace. URLs and credentials are not printed.');process.exitCode=1;});
