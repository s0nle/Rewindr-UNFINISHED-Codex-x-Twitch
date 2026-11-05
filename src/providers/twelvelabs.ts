import { createReadStream } from 'node:fs';
import { Readable } from 'node:stream';
import { setTimeout as delay } from 'node:timers/promises';
import { TwelvelabsApiClient } from 'twelvelabs-js';
import { z } from 'zod';
import type { IndexedChunk, SearchSettings, UploadSession, VideoSearchProvider } from '../types.js';
import {isTransientProviderError} from './errors.js';

const id = z.string().min(1);
const request = { timeoutInSeconds: 60, maxRetries: 0 };
const pollRequest = { timeoutInSeconds: 30, maxRetries: 2 };
const sessionSchema = z.object({
  uploadId: id, assetId: id, chunkSize: z.number().int().positive(), totalChunks: z.number().int().positive(),
  uploadHeaders: z.record(z.string(), z.string()).optional().default({})
});
const momentSchema = z.object({ videoId: id, start: z.number().finite().nonnegative(), end: z.number().finite().positive(), rank: z.number().int().positive() }).refine(r => r.end > r.start);

// All provider-specific SDK calls and response validation stay here.
export class TwelveLabsProvider implements VideoSearchProvider {
  readonly name = 'twelvelabs' as const;
  constructor(private client: TwelvelabsApiClient, private existingIndexId?: string) {}
  async validateIndex(indexId:string) {
    const index=await this.client.indexes.retrieve(indexId,pollRequest);
    if(!index.models?.some(m=>m.modelName==='marengo3.0'&&m.modelOptions?.includes('visual')&&m.modelOptions.includes('audio')))throw Error('Index must enable marengo3.0 with visual and audio.');
  }
  async ensureIndex(recordingId: string) {
    if (this.existingIndexId) {
      const index = await this.client.indexes.retrieve(this.existingIndexId, pollRequest);
      if (!index.models?.some(m => m.modelName === 'marengo3.0' && m.modelOptions?.includes('visual') && m.modelOptions.includes('audio'))) throw new Error('Index must enable marengo3.0 with visual and audio.');
      return this.existingIndexId;
    }
    const index = await this.client.indexes.create({ indexName: `rewindr-${recordingId}`, models: [{ modelName: 'marengo3.0', modelOptions: ['visual', 'audio'] }] }, request);
    return id.parse(index.id);
  }
  async createUpload(_path: string, filename: string, bytes: number): Promise<UploadSession> {
    const response = await this.client.multipartUpload.create({ filename, type: 'video', totalSize: bytes }, request);
    const session = sessionSchema.parse(response);
    if (session.totalChunks !== Math.ceil(bytes / session.chunkSize)) throw new Error('Invalid provider upload geometry');
    return { ...session, completedChunks: 0 };
  }
  async transfer(path: string, bytes: number, session: UploadSession, checkpoint: (completed: number) => Promise<void>) {
    // These are transport parts of ONE asset, not time-based video chunks. Stream
    // each byte range directly from disk; memory does not grow with recording size.
    for (let part = session.completedChunks + 1; part <= session.totalChunks; part++) {
      const start = (part - 1) * session.chunkSize;
      const end = Math.min(bytes, start + session.chunkSize) - 1;
      let etag: string | null = null;
      for (let attempt = 0; attempt < 3; attempt++) {
        const urls = await this.client.multipartUpload.getAdditionalPresignedUrls(session.uploadId, { start: part, count: 1 }, pollRequest);
        const url = z.string().url().parse(urls.uploadUrls?.find(u => u.chunkIndex === part)?.url);
        const stream = createReadStream(path, { start, end });
        try {
          const response = await fetch(url, {
            method: 'PUT', headers: { ...session.uploadHeaders, 'content-length': String(end - start + 1) },
            body: Readable.toWeb(stream) as ReadableStream<Uint8Array>, duplex: 'half', redirect: 'error', signal: AbortSignal.timeout(120_000)
          } as RequestInit & { duplex: 'half' });
          etag = response.headers.get('etag');
          await response.body?.cancel();
          if (!response.ok)throw Object.assign(new Error('Provider part upload failed'),{statusCode:response.status});
          if(!etag)throw new Error('Provider part upload proof was missing');
          break;
        } catch(error) {
          if (attempt === 2) throw Object.assign(new Error('Provider part upload failed after three attempts; the upload session was retained.'),{retryable:isTransientProviderError(error),cause:error});
          await delay(500 * 2 ** attempt);
        } finally { stream.destroy(); }
      }
      await this.client.multipartUpload.reportChunkBatch(session.uploadId, { completedChunks: [{ chunkIndex: part, proof: id.parse(etag).replaceAll('"', ''), proofType: 'etag', chunkSize: end - start + 1 }] }, pollRequest);
      await checkpoint(part);
    }
    // Completion of the transfer is distinct from asynchronous asset validation.
    // assets.retrieve below is the readiness authority; no blocking SDK wait loop.
  }
  async assetStatus(assetId: string) {
    const asset = await this.client.assets.retrieve(assetId, pollRequest);
    return z.enum(['processing', 'ready', 'failed']).parse(asset.status);
  }
  async indexAsset(indexId: string, assetId: string) {
    const indexed = await this.client.indexes.indexedAssets.create(indexId, { assetId, enableVideoStream: false }, request);
    return id.parse(indexed.id);
  }
  async indexStatus(indexId: string, indexedAssetId: string) {
    const indexed = await this.client.indexes.indexedAssets.retrieve(indexId, indexedAssetId, {}, pollRequest);
    return z.enum(['pending', 'queued', 'indexing', 'ready', 'failed']).parse(indexed.status);
  }
  async listIndexedChunks(indexId: string): Promise<IndexedChunk[]> {
    const results: IndexedChunk[] = [];
    const page = await this.client.indexes.indexedAssets.list(indexId, { pageLimit: 50 }, pollRequest);
    for await (let item of page) {
      if (results.length >= 2000) throw new Error('Index exceeds the supported 2000-asset import limit.');
      if (!item.systemMetadata?.filename || item.systemMetadata.duration === undefined) {
        item = await this.client.indexes.indexedAssets.retrieve(indexId, id.parse(item.id), {}, pollRequest);
      }
      results.push({
        indexedAssetId: id.parse(item.id), assetId: item.assetId,
        filename: id.parse(item.systemMetadata?.filename),
        durationSeconds: z.number().finite().positive().parse(item.systemMetadata?.duration),
        status: id.parse(item.status)
      });
    }
    return results;
  }
  async search(indexId: string, indexedAssetId: string, question: string, limit: number, referenceImagePath?: string, settings?: SearchSettings) {
    return this.searchScoped(indexId, [indexedAssetId], question, limit, settings, referenceImagePath);
  }
  async searchMany(indexId: string, indexedAssetIds: string[], question: string, limit: number, settings: SearchSettings) {
    return this.searchScoped(indexId, indexedAssetIds, question, limit, settings);
  }
  private async searchScoped(indexId: string, indexedAssetIds: string[], question: string, limit: number, settings?: SearchSettings, referenceImagePath?: string) {
    const ids = [...new Set(indexedAssetIds)];
    if (!ids.length || !Number.isInteger(limit) || limit < 1 || limit > 50) throw new Error('Invalid scoped search.');
    const modalities = settings?.modalities ?? ['visual'];
    const response = await this.client.search.create({ indexId, ...(question.trim()?{queryText:question}:{}),
      searchOptions: modalities, operator: settings?.operator ?? 'or',
      ...(modalities.includes('transcription') ? { transcriptionOptions: settings?.transcriptionOptions ?? ['lexical', 'semantic'] } : {}),
      groupBy: 'clip', pageLimit: limit, filter: JSON.stringify({ id: ids }),
      ...(referenceImagePath ? { queryMediaType: 'image' as const, queryMediaFile: createReadStream(referenceImagePath) } : {}) }, pollRequest);
    const rows = z.array(momentSchema).parse(response.data);
    if (rows.some(r => !ids.includes(r.videoId))) throw new Error('Provider returned a result from another source');
    return rows.sort((a, b) => a.rank - b.rank).slice(0, limit).map(r => ({ videoId: r.videoId, startSeconds: r.start, endSeconds: r.end, rank: r.rank }));
  }
}
