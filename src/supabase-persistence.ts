import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { z } from 'zod';
import { recordingSchema, broadcastSchema, storageRefSchema } from './metadata.js';
import { HttpError } from './http-errors.js';
import type { Broadcast, Recording } from './types.js';
import type { Persistence, RecordingRepository, BroadcastRepository } from './persistence.js';

export const LOCAL_OWNER_ID = '00000000-0000-4000-8000-000000000001';
function databaseFailure(): never { throw new HttpError(503, 'Supabase metadata is unavailable. Check connection settings and apply the database migrations.'); }
function decodeRecording(row: Record<string, unknown>): Recording {
  try {
    return recordingSchema.parse({ id: row.id, filename: row.filename, bytes: Number(row.bytes), sha256: row.sha256,
      creatorAuthorized: row.creator_authorized, authorizedAt: row.authorized_at, createdAt: row.created_at,
      updatedAt: row.updated_at, phase: row.phase, media: row.media ?? undefined,
      indexId: row.index_id ?? undefined, assetId: row.asset_id ?? undefined, indexedAssetId: row.indexed_asset_id ?? undefined,
      upload: row.upload ?? undefined, providerStatus: row.provider_status ?? undefined, error: row.error ?? undefined,
      pollErrors: row.poll_errors ?? undefined, providerError: row.provider_error ?? undefined });
  } catch { return databaseFailure(); }
}
function encodeRecording(input: Recording, ownerId: string) {
  const r = recordingSchema.parse(input);
  return { id: r.id, owner_id: ownerId, filename: r.filename, bytes: r.bytes, sha256: r.sha256,
    creator_authorized: r.creatorAuthorized, authorized_at: r.authorizedAt, created_at: r.createdAt,
    updated_at: new Date().toISOString(), phase: r.phase, media: r.media ?? null,
    index_id: r.indexId ?? null, asset_id: r.assetId ?? null, indexed_asset_id: r.indexedAssetId ?? null,
    upload: r.upload ?? null, provider_status: r.providerStatus ?? null, error: r.error ?? null,
    poll_errors: r.pollErrors ?? null, provider_error: r.providerError ?? null };
}

export class SupabasePersistence implements Persistence {
  readonly kind = 'supabase' as const;
  readonly recordings: RecordingRepository;
  readonly broadcasts: BroadcastRepository;
  constructor(readonly root: string, private client: SupabaseClient, readonly ownerId: string) {
    z.uuid().parse(ownerId);
    this.recordings = {
      root, mediaPath: id => join(root, 'media', id + '.mp4'),
      init: async () => { await mkdir(join(root, 'media'), { recursive: true }); },
      get: async id => {
        if (!z.uuid().safeParse(id).success) return undefined;
        const { data, error } = await client.from('rewindr_recordings').select('*').eq('owner_id', ownerId).eq('id', id).maybeSingle();
        if (error) return databaseFailure();
        return data ? decodeRecording(data) : undefined;
      },
      all: async () => (await this.list('rewindr_recordings', '*')).map(decodeRecording),
      put: async record => {
        // The composite conflict target cannot overwrite an ID belonging to another owner.
        const { error } = await client.from('rewindr_recordings').upsert(encodeRecording(record, ownerId), { onConflict: 'owner_id,id' });
        if (error) databaseFailure();
      }
    };
    this.broadcasts = {
      init: async () => {},
      get: async id => {
        if (!z.uuid().safeParse(id).success) return undefined;
        const { data, error } = await client.rpc('rewindr_get_broadcast', { p_owner_id: ownerId, p_id: id });
        if (error) return databaseFailure();
        if (data === null) return undefined;
        try { return broadcastSchema.parse(data); } catch { return databaseFailure(); }
      },
      all: async () => {
        const records: Broadcast[] = [];
        for (const row of await this.list('rewindr_broadcasts', 'id')) {
          const record = await this.broadcasts.get(String(row.id));
          if (record) records.push(record);
        }
        return records;
      },
      put: async record => {
        const value = broadcastSchema.parse(record);
        const { error } = await client.rpc('rewindr_save_broadcast', { p_owner_id: ownerId, p_broadcast: value });
        if (error) databaseFailure();
      },
      attachStorage: async (broadcastId, indexedAssetId, reference) => {
        const storage = storageRefSchema.parse(reference);
        const { error } = await client.rpc('rewindr_attach_chunk_storage', { p_owner_id: ownerId, p_broadcast_id: broadcastId,
          p_indexed_asset_id: indexedAssetId, p_storage: storage });
        if (error?.code === '23514') throw new HttpError(409, 'Chunk media is already attached with different content or an invalid storage mapping.');
        if (error) databaseFailure();
      }
    };
  }
  private async list(table: string, select: string): Promise<Record<string, unknown>[]> {
    const rows: Record<string, unknown>[] = [];
    for (let offset = 0; ; offset += 500) {
      const { data, error } = await this.client.from(table).select(select).eq('owner_id', this.ownerId).order('id').range(offset, offset + 499);
      if (error || !Array.isArray(data)) return databaseFailure();
      rows.push(...data as unknown as Record<string, unknown>[]);
      if (data.length < 500) return rows;
    }
  }
  async init() {
    const { error } = await this.client.from('rewindr_broadcasters').upsert({ id: this.ownerId, label: 'Local development owner; Twitch identity not verified' }, { onConflict: 'id', ignoreDuplicates: true });
    if (error) databaseFailure();
    await this.recordings.init();
    await this.health();
  }
  async health() {
    for (const table of ['rewindr_recordings', 'rewindr_broadcasts', 'rewindr_chunks']) {
      const { error } = await this.client.from(table).select('id').eq('owner_id', this.ownerId).limit(1);
      if (error) databaseFailure();
    }
  }
}

export function configuredSupabaseClient(env: NodeJS.ProcessEnv = process.env, timeoutMs = 15000) {
  const url = env.SUPABASE_URL?.trim();
  const secret = env.SUPABASE_SECRET_KEY?.trim();
  if (!url || !secret) throw new Error('Supabase mode requires SUPABASE_URL and SUPABASE_SECRET_KEY.');
  let parsed: URL;
  try { parsed = new URL(url); } catch { throw new Error('SUPABASE_URL must be a valid HTTPS project URL.'); }
  if (parsed.protocol !== 'https:') throw new Error('SUPABASE_URL must use HTTPS.');
  if (parsed.pathname !== '/' || parsed.search || parsed.hash || parsed.username || parsed.password) throw new Error('SUPABASE_URL must be the project root URL, without /rest/v1 or credentials.');
  let legacyServiceKey = false;
  try { legacyServiceKey = JSON.parse(Buffer.from(secret.split('.')[1] ?? '', 'base64url').toString()).role === 'service_role'; } catch {}
  if (!secret.startsWith('sb_secret_') && !legacyServiceKey) throw new Error('Use a backend secret key, not a publishable key, for Supabase persistence.');
  const client = createClient(url, secret, { auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
    global: { fetch: (input, init) => fetch(input, { ...init, signal: AbortSignal.timeout(timeoutMs) }) } });
  return client;
}
export function createSupabasePersistence(root: string, env: NodeJS.ProcessEnv = process.env) {
  return new SupabasePersistence(root, configuredSupabaseClient(env), env.REWINDR_OWNER_ID || LOCAL_OWNER_ID);
}
