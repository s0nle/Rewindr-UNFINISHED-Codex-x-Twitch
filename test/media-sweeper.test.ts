import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,utimes,stat,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {sweepMediaWork} from '../src/media-sweeper.js';
import type {MediaRepository,MediaTask} from '../src/media-store.js';
test('media workspace cleanup is dry-run by default and preserves active jobs, recent files and unverifiable state',async()=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-media-sweep-')),owner=randomUUID(),now=Date.now(),rows=new Map<string,Partial<MediaTask>>();
  try{
    async function workspace(state:string,old:boolean){const id=randomUUID(),dir=join(root,'media-work',id+'-ABC123');await mkdir(dir,{recursive:true});await writeFile(join(dir,'task.json'),JSON.stringify({taskId:id,owner,leaseToken:randomUUID()}));if(old)await utimes(dir,new Date(now-2*86400000),new Date(now-2*86400000));rows.set(id,{owner_id:owner,state:state as MediaTask['state'],lease_until:new Date(now+300000).toISOString()});return dir;}
    const eligible=await workspace('completed',true),active=await workspace('running',true),recent=await workspace('failed',false),repo=()=>({task:async(id:string)=>rows.get(id)} as MediaRepository);
    assert.equal((await sweepMediaWork(root,repo,false,now)).eligible,1);assert.ok(await stat(eligible));
    const applied=await sweepMediaWork(root,repo,true,now);assert.equal(applied.removed,1);await assert.rejects(stat(eligible));assert.ok(await stat(active));assert.ok(await stat(recent));
    const blocked=await workspace('failed',true);const unavailable=()=>({task:async()=>{throw Error('Database unavailable');}} as unknown as MediaRepository);assert.equal((await sweepMediaWork(root,unavailable,true,now)).removed,0);assert.ok(await stat(blocked));
  }finally{assert.ok(root.startsWith(join(tmpdir(),'rewindr-media-sweep-')));await rm(root,{recursive:true,force:true});}
});
