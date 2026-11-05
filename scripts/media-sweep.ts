import {resolve} from 'node:path';
import {configuredSupabaseClient} from '../src/supabase-persistence.js';
import {SupabaseMedia} from '../src/media-store.js';
import {sweepMediaWork} from '../src/media-sweeper.js';
const args=process.argv.slice(2);if(args.some(a=>a!=='--apply')){console.error('Usage: npm run media:sweep -- [--apply]');process.exitCode=1;}else{
  const apply=args.includes('--apply');sweepMediaWork(resolve(process.env.DATA_DIR||'.local/rewindr'),owner=>new SupabaseMedia(configuredSupabaseClient(),owner),apply).then(result=>console.log(JSON.stringify({mode:apply?'apply':'dry-run',...result}))).catch(()=>{console.error('Media sweep could not verify workspace/database state. Files were preserved when checks failed.');process.exitCode=1;});
}
