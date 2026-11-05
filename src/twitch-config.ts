export interface TwitchOAuthConfig {clientId:string;clientSecret:string;redirectUri:string}
export interface TwitchExtensionConfig {clientId:string;secrets:Buffer[];publicUrl:string;origins:string[];port:number}
export function oauthConfig(env:NodeJS.ProcessEnv=process.env):TwitchOAuthConfig|undefined{
  if(!env.TWITCH_CLIENT_ID?.trim()&&!env.TWITCH_CLIENT_SECRET?.trim())return;
  const clientId=env.TWITCH_CLIENT_ID?.trim(),clientSecret=env.TWITCH_CLIENT_SECRET?.trim(),redirectUri=env.TWITCH_REDIRECT_URI?.trim();
  if(!clientId||!clientSecret||!redirectUri||!(/^[a-zA-Z0-9]+$/).test(clientId))throw Error('Set TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET and TWITCH_REDIRECT_URI.');
  const url=new URL(redirectUri);if(!['127.0.0.1','localhost'].includes(url.hostname)||url.protocol!=='http:'||url.pathname!=='/twitch/callback'||url.search||url.hash||url.username||url.password)throw Error('Use the local dashboard callback http://127.0.0.1:3000/twitch/callback (or its configured local port).');
  return {clientId,clientSecret,redirectUri};
}
export function extensionConfig(env:NodeJS.ProcessEnv=process.env):TwitchExtensionConfig{
  const clientId=env.TWITCH_EXTENSION_CLIENT_ID?.trim(),secret=env.TWITCH_EXTENSION_SECRET?.trim(),publicUrl=env.TWITCH_EBS_PUBLIC_URL?.trim();
  if(!clientId||!secret||!publicUrl||!(/^[a-zA-Z0-9]+$/).test(clientId))throw Error('Set TWITCH_EXTENSION_CLIENT_ID, TWITCH_EXTENSION_SECRET and TWITCH_EBS_PUBLIC_URL.');
  const url=new URL(publicUrl);if(url.protocol!=='https:'||url.username||url.password||url.pathname!=='/'||url.search||url.hash)throw Error('TWITCH_EBS_PUBLIC_URL must be an HTTPS origin.');
  const decode=(value:string)=>{if(!/^[A-Za-z0-9+/]+={0,2}$/.test(value))throw Error('Extension secrets must be base64.');const bytes=Buffer.from(value,'base64');if(bytes.length<32||bytes.toString('base64').replace(/=+$/,'')!==value.replace(/=+$/,''))throw Error('Invalid Extension secret.');return bytes;};
  const origins=[`https://${clientId}.ext-twitch.tv`,url.origin,...(env.TWITCH_EXTENSION_ORIGINS||'').split(',').map(s=>s.trim()).filter(Boolean)];
  for(const origin of origins){const u=new URL(origin);if(u.protocol!=='https:'||u.origin!==origin||u.username||u.password)throw Error('Extension origins must be exact HTTPS origins.');}
  const port=Number(env.TWITCH_EBS_PORT||3001);if(!Number.isInteger(port)||port<1024||port>65535)throw Error('Invalid TWITCH_EBS_PORT.');
  return {clientId,secrets:[decode(secret),...(env.TWITCH_EXTENSION_PREVIOUS_SECRET?[decode(env.TWITCH_EXTENSION_PREVIOUS_SECRET.trim())]:[])],publicUrl:url.origin,origins:[...new Set(origins)],port};
}
