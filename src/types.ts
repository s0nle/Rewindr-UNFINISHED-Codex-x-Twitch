export type Phase = 'queued' | 'probing' | 'creating_index' | 'creating_upload' | 'uploading' | 'asset_processing' | 'submitting_index' | 'indexing' | 'ready' | 'failed';
export interface MediaInfo { durationSeconds: number; width: number; height: number; videoCodec: string }
export interface UploadSession {
  uploadId: string; assetId: string; chunkSize: number; totalChunks: number;
  completedChunks: number; uploadHeaders: Record<string, string>;
}
export interface Recording {
  id: string; filename: string; bytes: number; sha256: string;
  creatorAuthorized: true; authorizedAt: string; createdAt: string; updatedAt: string;
  phase: Phase; media?: MediaInfo; indexId?: string; assetId?: string;
  indexedAssetId?: string; upload?: UploadSession; providerStatus?: string;
  error?: string; pollErrors?: number;
  providerError?: { operation: string; httpStatus?: number; code?: string };
}
export interface ProviderMoment { videoId: string; startSeconds: number; endSeconds: number; rank: number }
export type SearchModality = 'visual' | 'audio' | 'transcription';
export interface SearchSettings {
  modalities: SearchModality[];
  operator: 'or' | 'and';
  transcriptionOptions: ('lexical' | 'semantic')[];
}
export interface IndexedChunk {
  indexedAssetId: string;
  assetId?: string;
  filename: string;
  durationSeconds: number;
  status: string;
}
export interface BroadcastChunk {
  filename: string;
  indexedAssetId: string;
  assetId?: string;
  startSeconds: number;
  endSeconds: number;
  storage?: ChunkStorageRef;
  storageKey?: string;
}
export interface ChunkStorageRef { bucket: string; path: string; sha256: string; bytes: number; uploadedAt: string }
export interface Broadcast {
  id: string;
  title: string;
  indexId: string;
  createdAt: string;
  sourceOffsetSeconds: number;
  sourceRecordingId?: string;
  ingestionId?: string;
  chunks: BroadcastChunk[];
}
export interface VideoSearchProvider {
  readonly name: 'twelvelabs';
  ensureIndex(recordingId: string): Promise<string>;
  createUpload(path: string, filename: string, bytes: number): Promise<UploadSession>;
  transfer(path: string, bytes: number, session: UploadSession, checkpoint: (completed: number) => Promise<void>): Promise<void>;
  assetStatus(assetId: string): Promise<'processing' | 'ready' | 'failed'>;
  indexAsset(indexId: string, assetId: string): Promise<string>;
  indexStatus(indexId: string, indexedAssetId: string): Promise<'pending' | 'queued' | 'indexing' | 'ready' | 'failed'>;
  search(indexId: string, indexedAssetId: string, question: string, limit: number, referenceImagePath?: string, settings?: SearchSettings): Promise<ProviderMoment[]>;
  searchMany?(indexId: string, indexedAssetIds: string[], question: string, limit: number, settings: SearchSettings): Promise<ProviderMoment[]>;
  listIndexedChunks?(indexId: string): Promise<IndexedChunk[]>;
  validateIndex?(indexId: string): Promise<void>;
}
