import {readFile,readdir} from 'node:fs/promises';
import {resolve,join} from 'node:path';
async function check(){
  const root=resolve('dist/extension');
  for(const name of ['config','live','panel']){
    const html=await readFile(join(root,name+'.html'),'utf8');
    if(!html.includes('https://extension-files.twitch.tv/helper/v1/twitch-ext.min.js')||!html.includes(`data-rewindr-view="${name}"`))throw Error('Missing Extension helper or view entry point.');
    const scripts=[...html.matchAll(/(?:src|href)="([^"]+)"/g)].map(match=>match[1]!);
    if(!scripts.some(path=>path.startsWith('./assets/'))||scripts.some(path=>path.startsWith('/')))throw Error('Extension assets must use relative paths.');
  }
  const contents=(await Promise.all((await readdir(join(root,'assets'))).filter(name=>/\.(js|css)$/.test(name)).map(name=>readFile(join(root,'assets',name),'utf8')))).join('\n');
  for(const name of ['SUPABASE_SECRET_KEY','SUPABASE_DB_URL','TWELVELABS_API_KEY','TWITCH_CLIENT_SECRET','TWITCH_EXTENSION_SECRET','TWITCH_EXTENSION_PREVIOUS_SECRET','REWINDR_AGENT_TOKEN']){
    const value=process.env[name]?.trim();if(value&&value.length>=8&&contents.includes(value))throw Error('A backend credential appeared in the Extension build. Do not upload it.');
  }
  console.log('Extension build checked: three helper-enabled pages, relative assets, and no configured backend credential values in the bundle.');
}
check().catch(()=>{console.error('Extension build verification failed. Rebuild and inspect the entry points/assets before uploading. Credential values are not printed.');process.exitCode=1;});
