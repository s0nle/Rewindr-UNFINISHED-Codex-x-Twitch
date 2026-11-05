import { resolve } from 'node:path';
import { mkdir, open, readFile, rm } from 'node:fs/promises';
import { TwelvelabsApiClient } from 'twelvelabs-js';
import { buildApp } from './app.js';
import { TwelveLabsProvider } from './providers/twelvelabs.js';
import { createLocalPersistence } from './local-persistence.js';
import { configuredSupabaseClient, createSupabasePersistence, LOCAL_OWNER_ID } from './supabase-persistence.js';
import { configuredChunkStorage } from './chunk-storage.js';
import { SupabaseJobQueue } from './jobs.js';
import {SupabaseIngestions} from './ingestions.js';
import {SupabaseSearches} from './search-store.js';
import {analysisConfig} from './analysis-config.js';
import {startStagingSweeper} from './staging-sweeper.js';
import {oauthConfig} from './twitch-config.js';
import {TwitchApi} from './twitch-api.js';
import {SupabaseTwitch} from './twitch-store.js';
import {SupabaseMedia} from './media-store.js';
import {SupabaseExports} from './export-storage.js';

const dataDir = resolve(process.env.DATA_DIR || '.local/rewindr');
await mkdir(dataDir, { recursive: true });
const lockPath = resolve(dataDir, 'api.lock');
// One local API owns the upload directory. Separate durable workers use DB leases;
// they do not take this API PID lock.
try {
  const oldPid = Number(await readFile(lockPath, 'utf8'));
  if (!Number.isSafeInteger(oldPid) || oldPid <= 0) throw new Error('Invalid api.lock; inspect it before removal.');
  let live = true;
  try { process.kill(oldPid, 0); } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ESRCH') live = false; else throw error; }
  if (live) throw new Error(`Another API process (${oldPid}) holds this data directory.`);
  await rm(lockPath);
} catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
const lock = await open(lockPath, 'wx');
await lock.writeFile(String(process.pid));
await lock.close();
const apiKey = process.env.TWELVELABS_API_KEY?.trim();
let app: Awaited<ReturnType<typeof buildApp>> | undefined;
let stopSweep:(()=>Promise<void>)|undefined;
let stopTwitchValidation:(()=>void)|undefined;
try {
  const mode = process.env.PERSISTENCE_MODE;
  if (mode && mode !== 'local' && mode !== 'supabase') throw new Error('PERSISTENCE_MODE must be local or supabase.');
  const useSupabase = mode === 'supabase' || (mode !== 'local' && Boolean(process.env.SUPABASE_URL || process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_DB_URL));
  const persistence = useSupabase ? createSupabasePersistence(dataDir) : createLocalPersistence(dataDir);
  const jobs=useSupabase?new SupabaseJobQueue(configuredSupabaseClient(),process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID):undefined;
  const ingestions=useSupabase?new SupabaseIngestions(configuredSupabaseClient(),process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID):undefined;
  const searches=useSupabase?new SupabaseSearches(configuredSupabaseClient(),process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID):undefined;const analysis=analysisConfig();
  const oauth=oauthConfig(),twitchApi=oauth?new TwitchApi(oauth):undefined;if(twitchApi)stopTwitchValidation=twitchApi.startValidation();
  app = await buildApp({ dataDir, persistence, jobs,ingestions,searches,analysisConfigured:Boolean(apiKey&&analysis.enabled),analysisModel:analysis.model,
    media:useSupabase?{media:new SupabaseMedia(configuredSupabaseClient(),process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID),exports:new SupabaseExports(configuredSupabaseClient(),process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID,process.env.SUPABASE_EXPORT_BUCKET||'rewindr-exports')}:undefined,
    twitch:{ownerId:process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID,repository:useSupabase?new SupabaseTwitch(configuredSupabaseClient()):undefined,api:twitchApi},
    storage: useSupabase ? configuredChunkStorage() : undefined, logger: true, provider: apiKey ? new TwelveLabsProvider(new TwelvelabsApiClient({ apiKey }), process.env.TWELVELABS_INDEX_ID || undefined) : undefined });
  const port = Number(process.env.PORT || 3000);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be between 1 and 65535');
  await app.listen({ host: '127.0.0.1', port });
  if(useSupabase)stopSweep=startStagingSweeper(dataDir,configuredSupabaseClient());
} catch (error) {stopTwitchValidation?.();await stopSweep?.();await app?.close(); await rm(lockPath, { force: true }); throw error; }
let closing = false;
async function shutdown() {
  if (closing) return;
  closing = true;
  stopTwitchValidation?.();
  await stopSweep?.();
  await app?.close();
  await rm(lockPath, { force: true });
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
