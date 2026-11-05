import {oauthConfig,extensionConfig} from '../src/twitch-config.js';
let valid=true;
try{if(!oauthConfig()){valid=false;console.log('OAuth setup missing: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, TWITCH_REDIRECT_URI.');}else console.log('Local Twitch account-link settings are valid.');}catch{valid=false;console.log('OAuth settings are incomplete or invalid. See docs/twitch.md.');}
try{extensionConfig();console.log('Extension backend settings are valid. Rebuild frontend assets after changing public URLs/IDs.');}catch{valid=false;console.log('Extension setup missing or invalid: TWITCH_EXTENSION_CLIENT_ID, TWITCH_EXTENSION_SECRET, TWITCH_EBS_PUBLIC_URL; optional TWITCH_EXTENSION_ORIGINS and TWITCH_EBS_PORT.');}
if(!valid)process.exitCode=1;
