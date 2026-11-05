import { resolve } from 'node:path';
import { TwelvelabsApiClient } from 'twelvelabs-js';
import { TwelveLabsProvider } from './providers/twelvelabs.js';
import { configuredSupabaseClient,createSupabasePersistence,LOCAL_OWNER_ID } from './supabase-persistence.js';
import { configuredChunkStorage } from './chunk-storage.js';
import { SupabaseJobQueue } from './jobs.js';
import { JobWorker } from './job-worker.js';
import {SupabaseIngestions} from './ingestions.js';
import {SupabaseSearches} from './search-store.js';
import {PegasusAnalysisProvider} from './providers/pegasus.js';
import {analysisConfig} from './analysis-config.js';
import {SupabaseMedia} from './media-store.js';
import {SupabaseExports} from './export-storage.js';
import {MediaWorker} from './media-worker.js';

async function main() {
  if(process.env.PERSISTENCE_MODE==='local')throw Error('The durable worker requires Supabase mode.');
  const args=process.argv.slice(2);let once=false;let queueName='rewindr_jobs';
  for(let i=0;i<args.length;i++) {
    if(args[i]==='--once')once=true;
    else if(args[i]==='--verification-queue'&&/^rewindr_verify_[a-f0-9]{32}$/.test(args[i+1]??''))queueName=args[++i]!;
    else throw Error('Invalid worker arguments.');
  }
  const root=resolve(process.env.DATA_DIR||'.local/rewindr');
  const owner=process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID;
  const queue=new SupabaseJobQueue(configuredSupabaseClient(),owner,queueName);
  await queue.status();await configuredChunkStorage().init();
  const key=process.env.TWELVELABS_API_KEY?.trim();
  const analysis=analysisConfig();
  const worker=new JobWorker({queue,dataDir:root,
    persistence:id=>createSupabasePersistence(root,{...process.env,REWINDR_OWNER_ID:id}),
    storage:id=>configuredChunkStorage({...process.env,REWINDR_OWNER_ID:id}),
    ingestions:id=>new SupabaseIngestions(configuredSupabaseClient(),id,queueName),
    searches:id=>new SupabaseSearches(configuredSupabaseClient(),id,queueName),
    analysisProvider:key&&analysis.enabled?new PegasusAnalysisProvider(new TwelvelabsApiClient({apiKey:key}),analysis.model):undefined,
    analysisStorage:id=>configuredChunkStorage({...process.env,REWINDR_OWNER_ID:id,SIGNED_URL_TTL_SECONDS:'3600'}),
    provider:key?new TwelveLabsProvider(new TwelvelabsApiClient({apiKey:key}),process.env.TWELVELABS_INDEX_ID||undefined):undefined});
  const media=queueName==='rewindr_jobs'?new MediaWorker({root,queue:new SupabaseMedia(configuredSupabaseClient(),owner),repository:id=>new SupabaseMedia(configuredSupabaseClient(),id),chunks:id=>configuredChunkStorage({...process.env,REWINDR_OWNER_ID:id}),exports:id=>new SupabaseExports(configuredSupabaseClient(),id,process.env.SUPABASE_EXPORT_BUCKET||'rewindr-exports')}):undefined;
  const stop=()=>{worker.stop();media?.stop();};process.once('SIGINT',stop);process.once('SIGTERM',stop);
  console.log('Rewindr durable worker started. Keep this process running alongside the API with the same DATA_DIR.');
  if(!key)console.warn('TwelveLabs is not configured; recording jobs cannot finish until it is configured.');
  if(once){await worker.runOnce();await media?.runOnce();}else await Promise.all([worker.run(),media?.run()]);
}
main().catch(()=>{console.error('Worker startup failed. Check Supabase settings, bucket, job migration and shared DATA_DIR. Credentials are not printed.');process.exitCode=1;});
