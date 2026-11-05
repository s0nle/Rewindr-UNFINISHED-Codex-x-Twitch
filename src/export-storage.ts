import {openAsBlob} from 'node:fs';
import {z} from 'zod';
import type {SupabaseClient} from '@supabase/supabase-js';
import {HttpError} from './http-errors.js';
import {exportStorageSchema,type ExportStorage} from './media-store.js';
export interface ExportStore{
  bucket:string;init():Promise<void>;exists(ref:ExportStorage):Promise<boolean>;upload(ref:ExportStorage,file:string):Promise<void>;sign(ref:ExportStorage,download?:boolean,ttl?:number):Promise<{url:string;expiresAt:string}>;remove(bucket:string,path:string):Promise<void>;
}
export class SupabaseExports implements ExportStore{
  constructor(private client:SupabaseClient,readonly owner:string,readonly bucket='rewindr-exports'){z.uuid().parse(owner);if(!/^[a-z0-9][a-z0-9_-]{1,62}$/.test(bucket)||bucket===(process.env.SUPABASE_STORAGE_BUCKET||'rewindr-chunks'))throw Error('Use a separate private export bucket.');}
  private validate(input:ExportStorage){const ref=exportStorageSchema.parse(input);if(ref.bucket!==this.bucket||!new RegExp('^'+this.owner+'/exports/[a-f0-9-]{36}\\.mp4$').test(ref.path))throw new HttpError(409,'Export does not belong to this owner.');return ref;}
  async init(){const {data,error}=await this.client.storage.getBucket(this.bucket);if(error||!data||data.public)throw new HttpError(503,'Private export bucket unavailable. Run media:init.');}
  async exists(input:ExportStorage){const ref=this.validate(input);await this.init();const {data,error}=await this.client.storage.from(ref.bucket).info(ref.path);if(!error&&data){if(data.size!==ref.bytes)throw new HttpError(409,'Existing export has unexpected bytes.');return true;}if([400,404].includes(Number((error as {statusCode?:unknown;status?:unknown})?.statusCode??(error as {status?:unknown})?.status)))return false;throw new HttpError(503,'Could not inspect export storage.');}
  async upload(input:ExportStorage,file:string){const ref=this.validate(input);await this.init();if(await this.exists(ref))return;const {error}=await this.client.storage.from(ref.bucket).upload(ref.path,await openAsBlob(file,{type:'video/mp4'}),{upsert:false,contentType:'video/mp4',cacheControl:'0',metadata:{sha256:ref.sha256}});if(error&&!await this.exists(ref))throw new HttpError(503,'Export upload failed; retry retained task.');}
  async sign(input:ExportStorage,download=false,ttl=300){const ref=this.validate(input);await this.init();const {data,error}=await this.client.storage.from(ref.bucket).createSignedUrl(ref.path,ttl,download?{download:'rewindr-clip.mp4'}:undefined);if(error||!data?.signedUrl)throw new HttpError(503,'Could not sign export replay.');return {url:data.signedUrl,expiresAt:new Date(Date.now()+ttl*1000).toISOString()};}
  async remove(bucket:string,path:string){if(!path.startsWith(this.owner+'/')||!['rewindr-exports',this.bucket,process.env.SUPABASE_STORAGE_BUCKET||'rewindr-chunks'].includes(bucket))throw new HttpError(409,'Deletion path is outside this owner.');const {error}=await this.client.storage.from(bucket).remove([path]);if(error)throw new HttpError(503,'Archive media deletion will retry.');}
}
