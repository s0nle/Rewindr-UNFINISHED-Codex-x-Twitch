import {useEffect,useMemo,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import {BroadcastSearch,type SearchRequest} from '../src/BroadcastSearch';
import {retryLoop} from '../../src/request-retry';
import {ViewerPanel} from './ViewerPanel';
import {MediaLibrary} from '../src/MediaLibrary';
import '../src/style.css';
import './style.css';
declare const __REWINDR_EBS_URL__:string;
declare const __REWINDR_EXTENSION_CLIENT_ID__:string;
declare const __REWINDR_DASHBOARD_URL__:string;
type Auth={channelId:string;clientId:string;token:string};
declare global {interface Window {Twitch?:{ext:{onAuthorized:(callback:(auth:Auth)=>void)=>void;onContext:(callback:(context:{theme?:string})=>void)=>void}}}}
type Account={linked:boolean;channelId:string;channel:{id:string;displayName:string;login:string;profileImageUrl:string}|null;live:{online:boolean;stream:{title:string}|null}|null;statusError:string|null;hostedEnabled?:boolean};
function Extension(){
  const mode=document.body.dataset.rewindrView||'panel',auth=useRef<Auth|null>(null),[version,setVersion]=useState(0),[role,setRole]=useState(''),[account,setAccount]=useState<Account|null>(null),[error,setError]=useState('');
  const backend=__REWINDR_EBS_URL__;
  const ngrokBackend=backend?['.ngrok-free.dev','.ngrok-free.app','.ngrok.app'].some(suffix=>new URL(backend).hostname.endsWith(suffix)):false;
  const [workspace,setWorkspace]=useState('search'),[credential,setCredential]=useState<{token:string;indexId:string}|null>(null),[onboarding,setOnboarding]=useState(false);
  const request=useMemo<SearchRequest>(()=>async<T,>(path:string,body?:unknown):Promise<T>=>{
    const current=auth.current;if(!current)throw Object.assign(Error('Waiting for Twitch authorization.'),{status:401});
    if(!path.startsWith('/')||path.startsWith('//'))throw Error('Invalid request path.');
    const response=await fetch(backend+(path==='/viewer/selection'?'':'/extension')+path,{method:body===undefined?'GET':'POST',headers:{Authorization:'Bearer '+current.token,...(ngrokBackend?{'ngrok-skip-browser-warning':'1'}:{}),...(body===undefined?{}:{'content-type':'application/json'})},...(body===undefined?{}:{body:JSON.stringify(body)}),credentials:'omit',signal:AbortSignal.timeout(20000)});
    const value=await response.json();if(!response.ok)throw Object.assign(Error(value.error||'Rewindr is temporarily unavailable.'),{status:response.status});return value;
  },[backend,ngrokBackend]);
  useEffect(()=>{
    let mounted=true;const helper=window.Twitch?.ext;if(!helper){setError('Open these controls inside your Twitch Extension. The Twitch helper is unavailable.');return;}
    helper.onAuthorized(value=>{if(!mounted)return;
      try{if(value.clientId!==__REWINDR_EXTENSION_CLIENT_ID__)throw Error('Extension client ID does not match this build.');const claims=JSON.parse(atob(value.token.split('.')[1]!.replace(/-/g,'+').replace(/_/g,'/')));
        if(auth.current?.channelId!==value.channelId){setAccount(null);setCredential(null);}auth.current=value;setRole(claims.role);setError('');setVersion(v=>v+1);
      }catch(e){auth.current=null;setRole('');setAccount(null);setCredential(null);setError((e as Error).message);}
    });
    helper.onContext(context=>{if(mounted&&context.theme)document.documentElement.dataset.twitchTheme=context.theme;});return()=>{mounted=false;auth.current=null;};
  },[mode]);
  useEffect(()=>{if(!version||role!=='broadcaster'||!backend||mode==='panel')return;return retryLoop({request:()=>request<Account>('/account'),received:value=>{setAccount(value);setError('');},failed:e=>setError((e as Error).message)});},[version,role,backend,mode,request]);
  const header=<header className="extension-header"><span className="brand">↶ rewindr<span className="brand-dot">.</span></span><span>{mode==='config'?'Channel setup':mode==='live'?'Private broadcaster controls':'Stream memory'}</span></header>;
  if(mode==='panel')return <div className="extension-shell">{header}<ViewerPanel request={request} authorized={Boolean(version&&backend)} authVersion={version} error={error}/></div>;
  if(!backend||!__REWINDR_EXTENSION_CLIENT_ID__)return <div className="extension-shell">{header}<main className="panel"><h2>Finish Extension setup</h2><p>Set the backend HTTPS URL and Extension client ID, then rebuild the Extension files.</p></main></div>;
  if(error&&!account)return <div className="extension-shell">{header}<main className="panel"><p role="alert" className="error">{error}</p><button onClick={()=>setVersion(v=>v+1)}>Retry connection</button></main></div>;
  if(role&&role!=='broadcaster')return <div className="extension-shell">{header}<main className="panel"><h2>Broadcaster access only</h2><p>Private questions and stream history are available to the channel owner.</p></main></div>;
  if(!account)return <div className="extension-shell">{header}<main className="panel" role="status">Waiting for Twitch to authorize your broadcaster session…</main></div>;
  if(!account.linked||mode==='config'||credential)return <div className="extension-shell">{header}<main><section className="panel"><h2>{account.linked?'Channel connected':'Link your Twitch channel'}</h2>
    {account.channel&&<div className="twitch-account"><img src={account.channel.profileImageUrl} alt=""/><div><h3>{account.channel.displayName}</h3><p>@{account.channel.login}</p><span className="ready-tag">Verified channel link</span></div></div>}
    {!account.linked&&<p>{account.hostedEnabled?'Generate a hosted capture credential below, or connect through your local Rewindr dashboard.':'Open your local Rewindr dashboard, choose Twitch, and connect this channel:'} {account.channelId}.</p>}
    {account.hostedEnabled&&<section className="hosted-setup"><h3>Connect a capture computer</h3><p>Generate a 30-day credential for the OBS capture client. Generating a new one revokes the previous credential.</p><button disabled={onboarding} onClick={()=>{setOnboarding(true);void request<{token:string;indexId:string}>('/onboard',{}).then(value=>{setCredential(value);setVersion(v=>v+1);}).catch(e=>setError(e.message)).finally(()=>setOnboarding(false));}}>Generate capture credential</button><button className="secondary" disabled={onboarding||!account.linked} onClick={()=>{setOnboarding(true);void request('/agent/revoke',{}).then(()=>setCredential(null)).catch(e=>setError(e.message)).finally(()=>setOnboarding(false));}}>Revoke capture credential</button>
      {credential&&<><p>Save this credential in the capture computer's .env. It is shown only in this session.</p><label>REWINDR_AGENT_TOKEN<input type="password" readOnly value={credential.token} onFocus={e=>e.target.select()}/></label><p>REWINDR_API_URL: {backend}<br/>TWELVELABS_INDEX_ID: {credential.indexId}</p>{mode==='live'&&<button className="secondary" onClick={()=>setCredential(null)}>I've saved it — open search</button>}</>}
    </section>}
    <a className="connect-twitch" href={__REWINDR_DASHBOARD_URL__} target="_blank" rel="noopener noreferrer">Open Rewindr dashboard</a>
    {account.live&&<p>{account.live.online?'Live now':'Currently offline'}{account.live.stream?` · ${account.live.stream.title}`:''}</p>}{account.statusError&&<p role="status">{account.statusError}</p>}
    <button className="secondary" onClick={()=>setVersion(v=>v+1)}>Refresh connection</button></section>
    <section className="panel"><h2>Search during your stream</h2><ol><li>Start the local API, worker and live capture.</li><li>Send a separate OBS output to Rewindr.</li><li>Open this Extension’s Live Config in Twitch to search indexed footage.</li></ol><p>Your first question stays unchanged. Match details and “Clip not found?” work privately in Live Config.</p></section></main></div>;
  return <div className="extension-shell">{header}{error&&<p className="error" role="status">{error} <button className="secondary" onClick={()=>setVersion(v=>v+1)}>Retry connection</button></p>}<nav className="extension-nav"><button className={workspace==='search'?'active':'secondary'} onClick={()=>setWorkspace('search')}>Search stream</button><button className={workspace==='media'?'active':'secondary'} onClick={()=>setWorkspace('media')}>Clips & storage</button></nav>{workspace==='media'?<MediaLibrary request={request}/>:<BroadcastSearch requestApi={request} persist={false}/>}</div>;
}
createRoot(document.getElementById('root')!).render(<Extension/>);
