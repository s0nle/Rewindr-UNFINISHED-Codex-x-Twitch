import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,rmdir,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createSessionWriter} from '../src/session-writer.js';
import {atomicJsonOrText} from '../src/segmenter.js';

test('a real atomic-write failure is visible, and later serialized saves still persist the latest job mapping',async t=>{
  const root=await mkdtemp(join(tmpdir(),'rewindr-session-save-')),path=join(root,'live-session.json');
  t.after(async()=>{assert.ok(root.startsWith(join(tmpdir(),'rewindr-session-save-')));await rm(root,{recursive:true,force:true});});
  let state={phase:'capturing',jobId:''},active=0,maxActive=0,calls=0;
  const writer=createSessionWriter(async()=>{calls++;active++;maxActive=Math.max(maxActive,active);try{await atomicJsonOrText(path,JSON.stringify(state));}finally{active--;}});
  await mkdir(path); // An actual rename error, rather than an artificially rejected queue.
  await assert.rejects(writer.save());await rmdir(path);
  state={phase:'interrupted',jobId:'retained-provider-job'};
  await Promise.all([writer.save(),writer.save()]);await writer.flush();
  assert.equal(calls,3);assert.equal(maxActive,1);assert.deepEqual(JSON.parse(await readFile(path,'utf8')),state);
});
