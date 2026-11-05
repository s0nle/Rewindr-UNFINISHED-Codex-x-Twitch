import {runRecordedIngestion} from '../src/recorded-ingestion.js';
import {IngestionInputError} from '../src/ingestion-client.js';
const controller=new AbortController();
process.once('SIGINT',()=>controller.abort());process.once('SIGTERM',()=>controller.abort());
async function main(){
  const args=process.argv.slice(2);const values=new Map<string,string>();const flags=new Set<string>();
  for(let i=0;i<args.length;i++) {
    const key=args[i]!;
    if(['--prepare-only','--creator-authorized'].includes(key)){if(flags.has(key))throw new IngestionInputError('Duplicate flag.');flags.add(key);continue;}
    if(!['--file','--resume','--workspace','--title','--index-id','--source-offset-seconds'].includes(key)||!args[i+1]||values.has(key))throw new IngestionInputError('Invalid arguments. See docs/ingestion.md.');
    values.set(key,args[++i]!);
  }
  const offset=values.has('--source-offset-seconds')?Number(values.get('--source-offset-seconds')):undefined;
  if(offset!==undefined&&(!Number.isFinite(offset)||offset<0))throw new IngestionInputError('Source offset must be nonnegative seconds.');
  await runRecordedIngestion({source:values.get('--file'),resume:values.get('--resume'),workspace:values.get('--workspace'),title:values.get('--title'),
    indexId:values.get('--index-id')||(values.has('--resume')?undefined:process.env.TWELVELABS_INDEX_ID)||undefined,offset,dataDir:process.env.DATA_DIR,port:Number(process.env.PORT||3000),
    prepareOnly:flags.has('--prepare-only'),creatorAuthorized:flags.has('--creator-authorized'),signal:controller.signal});
}
main().catch(error=>{console.error(controller.signal.aborted?'Ingestion stopped; accepted jobs can continue. Resume the printed workspace.':error instanceof IngestionInputError?error.message:'Ingestion stopped. Check the source file, FFmpeg, saved workspace and backend configuration. Source footage was retained.');process.exitCode=1;});
