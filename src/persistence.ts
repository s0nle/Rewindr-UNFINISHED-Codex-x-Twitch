import type { Broadcast, Recording, ChunkStorageRef } from './types.js';
export interface RecordingRepository {
  readonly root: string;
  mediaPath(id: string): string;
  init(): Promise<void>;
  get(id: string): Promise<Recording | undefined>;
  all(): Promise<Recording[]>;
  put(record: Recording): Promise<void>;
}
export interface BroadcastRepository {
  init(): Promise<void>;
  get(id: string): Promise<Broadcast | undefined>;
  all(): Promise<Broadcast[]>;
  put(broadcast: Broadcast): Promise<void>;
  attachStorage?(broadcastId: string, indexedAssetId: string, storage: ChunkStorageRef): Promise<void>;
}
export interface Persistence {
  readonly kind: 'local' | 'supabase';
  readonly recordings: RecordingRepository;
  readonly broadcasts: BroadcastRepository;
  init(): Promise<void>;
  health(): Promise<void>;
}
