import {test} from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as wait} from 'node:timers/promises';
import {retryLoop} from '../src/request-retry.js';
async function until(check:()=>boolean){for(let i=0;i<100;i++){if(check())return;await wait(2);}throw Error('Retry did not finish');}
test('search polling survives a transient error and stops after a terminal view',async()=>{
  let calls=0,received=0,errors=0;const stop=retryLoop({retryMs:1,intervalMs:1,request:async()=>{calls++;if(calls===1)throw Object.assign(Error('blip'),{status:503});return {done:calls===3};},received:()=>received++,failed:()=>errors++,again:value=>!value.done});
  try{await until(()=>received===2);await wait(10);assert.equal(calls,3);assert.equal(errors,1);}finally{stop();}
});
test('assessment request retries are bounded and can be started again by an explicit retry',async()=>{
  let calls=0,errors=0,success=0;const request=async()=>{calls++;if(calls<=3)throw TypeError('network');return true;};
  const stop=retryLoop({request,maxAttempts:3,retryMs:1,received:()=>success++,failed:()=>errors++});await until(()=>errors===3);await wait(10);assert.equal(calls,3);stop();
  const retry=retryLoop({request,maxAttempts:3,retryMs:1,received:()=>success++,failed:()=>errors++});try{await until(()=>success===1);assert.equal(calls,4);}finally{retry();}
});
test('unmounted/stale requests cannot update the UI and expired searches do not keep polling',async()=>{
  let received=0,errors=0,resolve!:(value:boolean)=>void;const stop=retryLoop({request:()=>new Promise<boolean>(r=>{resolve=r;}),received:()=>received++,failed:()=>errors++});await until(()=>Boolean(resolve));stop();resolve(true);await wait(5);assert.equal(received,0);
  let calls=0;const expired=retryLoop({request:async()=>{calls++;throw Object.assign(Error('expired'),{status:410});},received:()=>received++,failed:()=>errors++,retryMs:1});try{await until(()=>errors===1);await wait(10);assert.equal(calls,1);}finally{expired();}
});
