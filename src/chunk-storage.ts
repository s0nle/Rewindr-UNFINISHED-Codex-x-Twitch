import { createHash,randomUUID } from 'node:crypto';
import { openAsBlob } from 'node:fs';
import {createWriteStream} from 'node:fs';
import {rm,rename} from 'node:fs/promises';
import {Readable,Transform} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import { setTimeout as wait } from 'node:timers/promises';
import { z } from 'zod';
import type { SupabaseClient } from '@supabase/supabase-js';
import { configuredSupabaseClient, LOCAL_OWNER_ID } from './supabase-persistence.js';
import { storageRefSchema } from './metadata.js';
import { HttpError } from './http-errors.js';
import type { ChunkStorageRef } from './types.js';

export const MAX_CHUNK_BYTES = 50_000_000;
export const DEFAULT_CHUNK_BUCKET = 'rewindr-chunks';
export interface ChunkStorage {
  readonly bucket: string;
  init(): Promise<void>;
  upload(broadcastId: string, indexedAssetId: string, file: string, sha256: string, bytes: number): Promise<{ reference: ChunkStorageRef; reused: boolean }>;
  sign(broadcastId: string, indexedAssetId: string, reference: ChunkStorageRef,storageKey?:string): Promise<{ url: string; expiresAt: string }>;
  download?(broadcastId:string,key:string,reference:ChunkStorageRef,destination:string):Promise<void>;
}
export function chunkObjectPath(ownerId: string, broadcastId: string, indexedAssetId: string, sha256: string) {
  const owner = z.uuid().parse(ownerId).toLowerCase(); const broadcast = z.uuid().parse(broadcastId).toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(sha256)) throw new HttpError(400, 'Invalid chunk hash.');
  return `${owner}/${broadcast}/${createHash('sha256').update(indexedAssetId).digest('hex')}/${sha256}.mp4`;
}
function statusOf(error: unknown) {
  const value = error as { statusCode?: unknown; status?: unknown };
  return Number(value?.statusCode ?? value?.status ?? 0);
}
export class SupabaseChunkStorage implements ChunkStorage {
  constructor(private client: SupabaseClient, readonly ownerId: string, readonly bucket = DEFAULT_CHUNK_BUCKET,
    readonly ttlSeconds = 300, private retryDelayMs = 1000) {
    z.uuid().parse(ownerId);
    if (!/^[a-z0-9][a-z0-9_-]{1,62}$/.test(bucket) || !Number.isInteger(ttlSeconds) || ttlSeconds < 60 || ttlSeconds > 3600) throw new Error('Invalid storage bucket or signed URL lifetime.');
  }
  async init() {
    const { data, error } = await this.client.storage.getBucket(this.bucket);
    if (error || !data) throw new HttpError(503, 'Private chunk storage is unavailable. Run storage:init and check Supabase settings.');
    if (data.public) throw new HttpError(503, 'The Rewindr chunk bucket must be private.');
  }
  async upload(broadcastId: string, indexedAssetId: string, file: string, sha256: string, bytes: number) {
    await this.init();
    if (!Number.isSafeInteger(bytes) || bytes < 1 || bytes > MAX_CHUNK_BYTES) throw new HttpError(413, 'Chunk must contain at most 50 MB.');
    const path = chunkObjectPath(this.ownerId, broadcastId, indexedAssetId, sha256);
    let reused = false;
    const prior = await this.client.storage.from(this.bucket).info(path);
    if (!prior.error && prior.data) {
      if (prior.data.size !== bytes) throw new HttpError(409, 'Stored object size does not match its content hash.');
      return { reference: storageRefSchema.parse({ bucket: this.bucket, path, sha256, bytes, uploadedAt: new Date().toISOString() }), reused: true };
    }
    if (statusOf(prior.error) !== 400 && statusOf(prior.error) !== 404) throw new HttpError(503, 'Could not check existing chunk media. Retry later.');
    for (let attempt = 0; attempt < 3; attempt++) {
      const body = await openAsBlob(file, { type: 'video/mp4' });
      const { error } = await this.client.storage.from(this.bucket).upload(path, body,
        { contentType: 'video/mp4', upsert: false, cacheControl: '3600', metadata: { sha256 } });
      if (!error) break;
      const status = statusOf(error);
      if (status === 400 || status === 409) {
        // A previous upload may have committed before its response/DB link failed.
        const existing = await this.client.storage.from(this.bucket).info(path);
        if (!existing.error && existing.data && existing.data.size === bytes) { reused = true; break; }
      }
      if ((status >= 400 && status < 500 && status !== 408 && status !== 429) || attempt === 2) {
        throw new HttpError(503, 'Chunk upload failed. Local source files are retained; retry the same chunk.');
      }
      await wait(this.retryDelayMs * 2 ** attempt);
    }
    const reference = storageRefSchema.parse({ bucket: this.bucket, path, sha256, bytes, uploadedAt: new Date().toISOString() });
    return { reference, reused };
  }
  async sign(broadcastId: string, indexedAssetId: string, input: ChunkStorageRef,storageKey?:string) {
    await this.init();
    const reference = storageRefSchema.parse(input);
    if(storageKey&&!/^ingest:[a-f0-9-]{36}$/.test(storageKey))throw new HttpError(409,'Invalid ingestion storage identity.');
    if (reference.bucket !== this.bucket || reference.path !== chunkObjectPath(this.ownerId, broadcastId, storageKey||indexedAssetId, reference.sha256)) {
      throw new HttpError(409, 'Stored media does not belong to this broadcast and owner.');
    }
    const { data, error } = await this.client.storage.from(this.bucket).createSignedUrl(reference.path, this.ttlSeconds);
    if (error || !data?.signedUrl) throw new HttpError(503, 'Playback link could not be created. Check stored media and retry.');
    return { url: data.signedUrl, expiresAt: new Date(Date.now() + this.ttlSeconds * 1000).toISOString() };
  }
  async download(broadcastId:string,key:string,reference:ChunkStorageRef,destination:string) {
    const signed=await this.sign(broadcastId,key,reference);const partial=`${destination}.${randomUUID()}.download`;
    try {
      const response=await fetch(signed.url,{signal:AbortSignal.timeout(120000),redirect:'error'});
      if(!response.ok||!response.body)throw new HttpError(503,'Stored chunk download failed.');
      let bytes=0;const hash=createHash('sha256');
      await pipeline(Readable.fromWeb(response.body as import('node:stream/web').ReadableStream),new Transform({transform(buffer:Buffer,_encoding,done){
        bytes+=buffer.length;if(bytes>reference.bytes){done(new Error('Stored media size mismatch.'));return;}hash.update(buffer);done(null,buffer);
      }}),createWriteStream(partial,{flags:'wx',mode:0o600}));
      if(bytes!==reference.bytes||hash.digest('hex')!==reference.sha256)throw new HttpError(409,'Stored media failed its integrity check.');
      await rename(partial,destination);
    } finally {await rm(partial,{force:true});}
  }
}
export function configuredChunkStorage(env: NodeJS.ProcessEnv = process.env) {
  return new SupabaseChunkStorage(configuredSupabaseClient(env, 120000), env.REWINDR_OWNER_ID || LOCAL_OWNER_ID,
    env.SUPABASE_STORAGE_BUCKET || DEFAULT_CHUNK_BUCKET, Number(env.SIGNED_URL_TTL_SECONDS || 300));
}
