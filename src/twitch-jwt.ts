import {createHmac,timingSafeEqual} from 'node:crypto';
import {z} from 'zod';
import {HttpError} from './http-errors.js';
const claimsSchema=z.object({exp:z.number().int().positive(),channel_id:z.string().regex(/^\d{1,30}$/),role:z.enum(['broadcaster','viewer','moderator','external']),opaque_user_id:z.string().min(1).max(200),
  user_id:z.string().optional(),nbf:z.number().optional(),iat:z.number().optional(),client_id:z.string().optional()});
export function verifyExtensionToken(token:string,secrets:Buffer[],clientId:string,now=Date.now()){
  try{
    if(token.length>8192)throw Error();const parts=token.split('.');if(parts.length!==3||parts.some(p=>!/^[A-Za-z0-9_-]+$/.test(p)))throw Error();
    const header=JSON.parse(Buffer.from(parts[0]!,'base64url').toString());if(header.alg!=='HS256'||header.crit!==undefined||header.b64!==undefined||(header.typ!==undefined&&header.typ!=='JWT'))throw Error();
    const signature=Buffer.from(parts[2]!,'base64url');if(signature.length!==32||!secrets.some(secret=>timingSafeEqual(signature,createHmac('sha256',secret).update(parts[0]+'.'+parts[1]).digest())))throw Error();
    const value=claimsSchema.parse(JSON.parse(Buffer.from(parts[1]!,'base64url').toString()));
    if(value.exp<=now/1000||(value.nbf!==undefined&&value.nbf>now/1000+5)||(value.iat!==undefined&&value.iat>now/1000+5)||(value.client_id&&value.client_id!==clientId))throw Error();
    if(value.role==='external')throw new HttpError(403,'A Twitch browser session is required.');
    if(value.role==='broadcaster'&&(value.opaque_user_id!=='U'+value.channel_id||(value.user_id&&value.user_id!==value.channel_id)))throw Error();
    return {channelId:value.channel_id,expiresAt:value.exp,role:value.role,opaqueId:value.opaque_user_id};
  }catch(e){if(e instanceof HttpError)throw e;throw new HttpError(401,'A valid Twitch broadcaster Extension token is required.');}
}
export function verifyBroadcasterToken(token:string,secrets:Buffer[],clientId:string,now=Date.now()){
  const value=verifyExtensionToken(token,secrets,clientId,now);if(value.role!=='broadcaster')throw new HttpError(403,'Only the broadcaster can use private Rewindr controls.');return value;
}
