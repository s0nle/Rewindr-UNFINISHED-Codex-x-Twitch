import {defineConfig,loadEnv} from 'vite';
import react from '@vitejs/plugin-react';
import {resolve} from 'node:path';
export default defineConfig(({mode})=>{
  const env={...loadEnv(mode,process.cwd(),''),...process.env};let url=env.TWITCH_EBS_PUBLIC_URL?.trim()||'';
  if(url){const parsed=new URL(url);if(parsed.protocol!=='https:'||parsed.pathname!=='/'||parsed.search||parsed.hash||parsed.username||parsed.password)throw Error('TWITCH_EBS_PUBLIC_URL must be an HTTPS origin.');url=parsed.origin;}
  const dashboard=env.TWITCH_REDIRECT_URI?new URL('/',env.TWITCH_REDIRECT_URI).href:'http://localhost:3000/';
  return {root:'web/extension',base:'./',plugins:[react()],define:{__REWINDR_EBS_URL__:JSON.stringify(url),__REWINDR_EXTENSION_CLIENT_ID__:JSON.stringify(env.TWITCH_EXTENSION_CLIENT_ID||''),__REWINDR_DASHBOARD_URL__:JSON.stringify(dashboard)},
    build:{outDir:'../../dist/extension',emptyOutDir:true,rollupOptions:{input:{config:resolve('web/extension/config.html'),live:resolve('web/extension/live.html'),panel:resolve('web/extension/panel.html')}}}};
});
