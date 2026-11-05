import {useEffect,useState} from 'react';
import {retryLoop} from '../../src/request-retry';
const prefix=import.meta.env.DEV?'/api':'';
type Status={configured:boolean;linked:boolean;channel:{id:string;login:string;displayName:string;profileImageUrl:string;verifiedAt:string}|null;live:{online:boolean;stream:{title:string;game:string;startedAt:string}|null}|null;error:string|null;connectUrl:string|null};
export function TwitchAccount(){
  const [status,setStatus]=useState<Status|null>(null),[error,setError]=useState(''),[busy,setBusy]=useState(false),[revision,setRevision]=useState(0);
  useEffect(()=>retryLoop({request:async()=>{const response=await fetch(prefix+'/twitch/status');const value=await response.json();if(!response.ok)throw Object.assign(Error(value.error||'Connection status unavailable.'),{status:response.status});return value as Status;},received:value=>{setStatus(value);setError('');},failed:e=>setError((e as Error).message),again:()=>true,intervalMs:60000}),[revision]);
  async function disconnect(){setBusy(true);try{const response=await fetch(prefix+'/twitch/disconnect',{method:'POST'});const value=await response.json();if(!response.ok)throw Error(value.error||'Disconnect failed.');setRevision(n=>n+1);}catch(e){setError((e as Error).message);}finally{setBusy(false);}}
  return <main><div className="intro"><p className="eyebrow">YOUR CHANNEL. YOUR MEMORY.</p><h1>Connect Rewindr<br/><span>to Twitch.</span></h1><p className="subtitle">Link the channel that can access your private stream history inside Twitch.</p></div>
    <section className="panel"><h2>Twitch connection</h2>{error&&<p role="alert" className="error">{error}</p>}
      {!status&&<p role="status">Checking your connection…</p>}
      {status&&!status.configured&&<p>Finish the Twitch Developer application settings in .env, apply migrations, and restart Rewindr. Setup steps are in docs/twitch.md.</p>}
      {status?.channel&&<div className="twitch-account"><img src={status.channel.profileImageUrl} alt=""/><div><h3>{status.channel.displayName}</h3><p>@{status.channel.login} · channel {status.channel.id}</p><span className="ready-tag">Verified channel link</span></div></div>}
      {status?.live&&<p>{status.live.online?'Live now':'Currently offline'}{status.live.stream?` · ${status.live.stream.title}`:''}</p>}{status?.error&&<p role="status">{status.error}</p>}
      <div className="twitch-actions">{status?.configured&&!status.linked&&<a className="connect-twitch" href={status.connectUrl||prefix+'/twitch/connect'}>Connect with Twitch</a>}
        {status?.linked&&<button type="button" className="secondary" onClick={()=>void disconnect()} disabled={busy}>Disconnect channel</button>}
        <button type="button" className="secondary" onClick={()=>setRevision(n=>n+1)}>Refresh connection</button></div>
      <p className="search-hint">The connection verifies your channel. Search remains private to the broadcaster; your normal Twitch broadcast continues independently.</p></section>
    <section className="panel"><h2>Next: enable the Extension</h2><p>After linking, install your Rewindr Extension in Twitch Local Test or Hosted Test. Open its configuration page to verify the same channel, then use Live Config for private search and replay.</p></section></main>;
}
