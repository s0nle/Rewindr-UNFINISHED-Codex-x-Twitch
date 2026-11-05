import {randomUUID} from 'node:crypto';
import {configuredSupabaseClient,LOCAL_OWNER_ID} from '../src/supabase-persistence.js';
import {SupabaseMedia} from '../src/media-store.js';
import {HttpError} from '../src/http-errors.js';
import {setTimeout as wait} from 'node:timers/promises';
const controller=new AbortController();process.once('SIGINT',()=>controller.abort());process.once('SIGTERM',()=>controller.abort());
async function run(owner:string,days:number,apply:boolean){
  const client=configuredSupabaseClient(),media=new SupabaseMedia(client,owner),before=new Date(Date.now()-days*86400000).toISOString();
  const archives=await client.from('rewindr_broadcasts').select('id,title').eq('owner_id',owner).is('deleting_at',null).lt('created_at',before).order('created_at').limit(100);
  const exports=await client.from('rewindr_exports').select('id').eq('owner_id',owner).lt('expires_at',new Date().toISOString()).order('expires_at').limit(100);if(archives.error||exports.error)throw Error('Retention inventory unavailable.');
  console.log(`${apply?'APPLY':'DRY RUN'}: ${archives.data.length} archive(s) older than ${days} days; ${exports.data.length} expired export(s), capped at 100 each per run.`);
  console.log('Only this configured owner is processed. TwelveLabs assets, original local footage and shared provider indexes are retained.');
  if(!apply)return;
  let queued=0,blocked=0;
  for(const item of archives.data)try{await media.purge(randomUUID(),item.id,item.title);queued++;}catch(e){blocked++;console.error('Archive not queued.',{id:item.id,error:e instanceof HttpError?e.message:'Storage state could not be confirmed.'});}
  for(const item of exports.data)try{await media.rpc('rewindr_begin_export_expiry',{p_owner_id:owner,p_id:randomUUID(),p_export_id:item.id});queued++;}catch(e){blocked++;console.error('Expired export not queued.',{id:item.id,error:e instanceof HttpError?e.message:'Storage state could not be confirmed.'});}
  await media.rpc('rewindr_prune_operational',{p_owner_id:owner});
  console.log(`Queued ${queued} durable cleanup task(s); ${blocked} blocked/conflicting item(s). Inspect Clips & storage; keep the worker running. Repeat the dry run after completion.`);
}
async function main(){
  const args=process.argv.slice(2),apply=args.includes('--apply'),watch=args.includes('--watch'),all=args.includes('--all-owners'),daysAt=args.indexOf('--days'),days=daysAt<0?14:Number(args[daysAt+1]);
  if(!Number.isInteger(days)||days<1||days>365||args.some((v,i)=>!['--apply','--watch','--all-owners','--days'].includes(v)&&!(daysAt>=0&&i===daysAt+1)))throw Error('Usage: npm run retention -- [--days 14] [--apply] [--watch] [--all-owners]');
  do{
    const owner=process.env.REWINDR_OWNER_ID||LOCAL_OWNER_ID;let owners=[owner];
    if(all){let cursor:string|undefined;for(;;){let query=configuredSupabaseClient().from('rewindr_broadcasters').select('id').order('id').limit(100);if(cursor)query=query.gt('id',cursor);const {data,error}=await query;if(error)throw Error();for(const row of data){if(controller.signal.aborted)return;await run(row.id,days,apply);}if(data.length<100)break;cursor=data.at(-1)!.id;}}
    else for(const id of owners){if(controller.signal.aborted)return;await run(id,days,apply);}
    if(watch)await wait(86400000,undefined,{signal:controller.signal}).catch(()=>{});
  }while(watch&&!controller.signal.aborted);
}
main().catch(error=>{console.error(error instanceof Error&&error.message.startsWith('Usage:')?error.message:'Retention failed. Check migrations, owner settings and active work. Credentials are not printed.');process.exitCode=1;});
