import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,utimes,rm,symlink,stat} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {sweepStagedFiles,type StagedFileStatus} from '../src/staging-sweeper.js';
import {chunkJobFile,lockStagedFile,StagedFileBusyError} from '../src/staged-media.js';
async function fixture(t:{after:(fn:()=>Promise<void>)=>void}){
  const root=await mkdtemp(join(tmpdir(),'rewindr-sweep-'));await mkdir(join(root,'chunk-jobs'));t.after(async()=>{assert.ok(root.startsWith(join(tmpdir(),'rewindr-sweep-')));await rm(root,{recursive:true,force:true});});
  const rows=new Map<string,StagedFileStatus>(),now=Date.now();
  const file=async(state:StagedFileStatus['state'],days=8,replicas:StagedFileStatus['replicas']=[])=>{const id=randomUUID(),path=chunkJobFile(root,id);await writeFile(path,'synthetic staged bytes');await utimes(path,new Date(now-days*86400000),new Date(now-days*86400000));rows.set(id,{fileId:id,state,updatedAt:new Date(now-days*86400000).toISOString(),replicas});return {id,path};};
  return {root,now,rows,file,status:async(ids:string[])=>ids.flatMap(id=>rows.has(id)?[rows.get(id)!]:[]),replicaExists:async()=>true};
}
test('sweep removes old orphans/completed/recoverable copies while retaining active, recent and unrecoverable files',async t=>{
  const f=await fixture(t),replica=[{bucket:'test',path:'private-replica'}];const orphan=await f.file('orphan'),done=await f.file('completed',8,replica),recoverable=await f.file('recoverable',8,replica),active=await f.file('keep'),recent=await f.file('orphan',0),youngFailed=await f.file('recoverable',2,replica),unbacked=await f.file('completed');
  await writeFile(join(f.root,'chunk-jobs','keep-unrecognized.mp4'),'original');const result=await sweepStagedFiles(f);assert.equal(result.removed,3);
  for(const file of [orphan,done,recoverable])await assert.rejects(()=>stat(file.path),{code:'ENOENT'});for(const file of [active,recent,youngFailed,unbacked])assert.ok((await stat(file.path)).isFile());
  assert.equal(await readFile(join(f.root,'chunk-jobs','keep-unrecognized.mp4'),'utf8'),'original');
});
test('an active file lock or changed job state prevents cleanup; a missing replica preserves the local copy',async t=>{
  const f=await fixture(t),locked=await f.file('orphan'),release=await lockStagedFile(f.root,locked.id);await assert.rejects(()=>lockStagedFile(f.root,locked.id),StagedFileBusyError);
  assert.equal((await sweepStagedFiles(f)).removed,0);await release();
  let calls=0;assert.equal((await sweepStagedFiles({...f,status:async(ids)=>{calls++;return ids.map(fileId=>({...f.rows.get(fileId)!,state:calls===1?'orphan':'keep'}));}})).removed,0);
  f.rows.get(locked.id)!.state='recoverable';f.rows.get(locked.id)!.replicas=[{bucket:'test',path:'missing'}];assert.equal((await sweepStagedFiles({...f,replicaExists:async()=>false})).removed,0);assert.ok(await stat(locked.path));
});
test('database/replica outages preserve sources and dry-run performs no media deletion',async t=>{
  const f=await fixture(t),file=await f.file('orphan');await assert.rejects(()=>sweepStagedFiles({...f,status:async()=>{throw Error('offline');}}));assert.ok(await stat(file.path));
  const dry=await sweepStagedFiles({...f,apply:false});assert.equal(dry.eligible,1);assert.equal(dry.removed,0);assert.ok(await stat(file.path));
});
test('single-API sweep reclaims a dead process lock and never reclaims a live process lock',async t=>{
  const f=await fixture(t),file=await f.file('orphan'),child=spawn(process.execPath,['-e',''],{windowsHide:true});const pid=child.pid!;await new Promise<void>((resolve,reject)=>{child.once('error',reject);child.once('close',()=>resolve());});
  const lock=file.path+'.use.lock';await writeFile(lock,JSON.stringify({pid,token:'dead'}));await utimes(lock,new Date(f.now-600000),new Date(f.now-600000));assert.equal((await sweepStagedFiles(f)).staleLocks,1);
  const live=await f.file('orphan'),release=await lockStagedFile(f.root,live.id);try{await utimes(live.path+'.use.lock',new Date(f.now-600000),new Date(f.now-600000));assert.equal((await sweepStagedFiles(f)).removed,0);assert.ok(await stat(live.path));}finally{await release();}
});
test('sweep rejects a redirected staging directory',async t=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-sweep-link-')),external=await mkdtemp(join(tmpdir(),'rewindr-sweep-target-'));
  t.after(async()=>{assert.ok(root.startsWith(join(tmpdir(),'rewindr-sweep-link-'))&&external.startsWith(join(tmpdir(),'rewindr-sweep-target-')));await rm(root,{recursive:true,force:true});await rm(external,{recursive:true,force:true});});
  await symlink(external,join(root,'chunk-jobs'),'junction');await assert.rejects(()=>sweepStagedFiles({root,status:async()=>[],replicaExists:async()=>true}),/Invalid staging directory/);
});
