import {resolve} from 'node:path';
import {TwelvelabsApiClient} from 'twelvelabs-js';
import {TwelveLabsProvider} from './providers/twelvelabs.js';
import {configuredSupabaseClient,createSupabasePersistence} from './supabase-persistence.js';
import {configuredChunkStorage} from './chunk-storage.js';
import {SupabaseSearches} from './search-store.js';
import {SupabaseTwitch} from './twitch-store.js';
import {TwitchApi} from './twitch-api.js';
import {oauthConfig,extensionConfig} from './twitch-config.js';
import {analysisConfig} from './analysis-config.js';
import {buildTwitchEbs} from './twitch-ebs.js';
import {SupabaseMedia} from './media-store.js';
import {SupabaseExports} from './export-storage.js';
import {HostedChannels} from './hosted.js';
import {SupabaseIngestions} from './ingestions.js';
import {acquireIngestionLock} from './ingestion-lock.js';
import {startStagingSweeper} from './staging-sweeper.js';
async function main(){
  const config=extensionConfig(),oauth=oauthConfig();if(!oauth)throw Error('Twitch application settings are missing.');
  const client=configuredSupabaseClient(),root=resolve(process.env.DATA_DIR||'.local/rewindr'),api=new TwitchApi(oauth),stopValidation=api.startValidation(),analysis=analysisConfig(),key=process.env.TWELVELABS_API_KEY?.trim();
  if(process.env.HOSTED_ONBOARDING_ENABLED&&!['true','false'].includes(process.env.HOSTED_ONBOARDING_ENABLED))throw Error('Invalid hosted mode.');
  const hostedEnabled=process.env.HOSTED_ONBOARDING_ENABLED==='true',maxChannels=Number(process.env.HOSTED_MAX_CHANNELS||1);if(!Number.isInteger(maxChannels)||maxChannels<1||maxChannels>100)throw Error('Invalid hosted capacity.');
  const indexId=process.env.TWELVELABS_INDEX_ID?.trim();if(hostedEnabled&&!indexId)throw Error('Hosted capture requires an assigned TwelveLabs index.');
  const allowlist=(process.env.TWITCH_CHANNEL_ALLOWLIST||'').split(',').map(s=>s.trim()).filter(Boolean);if(allowlist.some(id=>!/^\d{1,30}$/.test(id)))throw Error('Invalid onboarding allowlist.');
  const hosted=hostedEnabled?new HostedChannels({client,api,dataDir:root,indexId:indexId!,maxChannels,allowlist}):undefined;
  const releaseIngestion=hostedEnabled?await acquireIngestionLock(root):undefined;let stopSweep:(()=>Promise<void>)|undefined;
  try{
  const app=await buildTwitchEbs({config,links:new SupabaseTwitch(client),api,hosted,logger:true,analysisConfigured:Boolean(key&&analysis.enabled),analysisModel:analysis.model,
    ready:async()=>{const result=await client.from('rewindr_exports').select('id').limit(1);if(result.error)throw Error();await configuredChunkStorage().init();await new SupabaseExports(client,process.env.REWINDR_OWNER_ID||'00000000-0000-4000-8000-000000000001',process.env.SUPABASE_EXPORT_BUCKET||'rewindr-exports').init();},
    provider:key?new TwelveLabsProvider(new TwelvelabsApiClient({apiKey:key}),process.env.TWELVELABS_INDEX_ID||undefined):undefined,
    scope:owner=>{const env={...process.env,REWINDR_OWNER_ID:owner};return {persistence:createSupabasePersistence(root,env),storage:configuredChunkStorage(env),searches:new SupabaseSearches(client,owner),media:new SupabaseMedia(client,owner),ingestions:new SupabaseIngestions(client,owner),exports:new SupabaseExports(client,owner,process.env.SUPABASE_EXPORT_BUCKET||'rewindr-exports')};}});
  const bind=process.env.EBS_BIND_ADDRESS||'127.0.0.1';if(!['127.0.0.1','0.0.0.0'].includes(bind))throw Error('Invalid EBS bind address.');
  app.addHook('onClose',async()=>{stopValidation();await stopSweep?.();await releaseIngestion?.();});await app.listen({host:bind,port:config.port});
  if(hostedEnabled)stopSweep=startStagingSweeper(root,client);
  let closing=false;const stop=()=>{if(closing)return;closing=true;void app.close();};process.once('SIGINT',stop);process.once('SIGTERM',stop);
  console.log('Rewindr Extension backend started. Expose only its configured port through HTTPS; keep the local administration API private.');
  }catch(error){stopValidation();await stopSweep?.();await releaseIngestion?.();throw error;}
}
main().catch(()=>{console.error('Extension backend startup failed. Check docs/twitch.md, Twitch settings and Supabase migrations. Credentials are not printed.');process.exitCode=1;});
