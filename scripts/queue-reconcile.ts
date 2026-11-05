import { resolve } from 'node:path';
import { configuredSupabaseClient,createSupabasePersistence,LOCAL_OWNER_ID } from '../src/supabase-persistence.js';
import { SupabaseJobQueue } from '../src/jobs.js';
async function run() {
  const persistence=createSupabasePersistence(resolve(process.env.DATA_DIR||'.local/rewindr'));await persistence.init();
  const queue=new SupabaseJobQueue(configuredSupabaseClient(),process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID);
  let count=0;
  for(const record of await persistence.recordings.all()) {
    if(record.phase==='ready'||record.phase==='failed')continue;
    await queue.enqueue('recording',record.id);count++;
  }
  console.log(`Reconciled ${count} unfinished recording(s). Existing jobs were reused. No provider operation was requested.`);
}
run().catch(()=>{console.error('Queue reconciliation failed. Check Supabase settings and the job migration.');process.exitCode=1;});
