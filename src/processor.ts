import { probeMedia } from './media.js';
import type { RecordingRepository } from './persistence.js';
import { HttpError } from './http-errors.js';
import type { Recording, VideoSearchProvider } from './types.js';
import { describeProviderFailure,isTransientProviderError } from './providers/errors.js';
import { setTimeout as wait } from 'node:timers/promises';

const ambiguous = new Set(['creating_index', 'creating_upload', 'submitting_index']);
export interface MetadataRetryOptions { maxFailures?: number; initialDelayMs?: number; maxDelayMs?: number }
class ProcessorStopped extends Error {}
class MetadataOperationError extends Error { constructor() { super('Metadata storage operation failed.'); } }
export class Processor {
  private timer?: ReturnType<typeof setTimeout>;
  private running?: Promise<void>;
  private stopped = true;
  private retryAbort = new AbortController();
  private metadataFailures = 0;
  private retryDelayMs?: number;
  private haltReason?: 'metadata_unavailable' | 'storage_or_processing_error';
  private readonly retry: Required<MetadataRetryOptions>;
  constructor(private store: RecordingRepository, private provider: VideoSearchProvider, private intervalMs = 5000, retry: MetadataRetryOptions = {}) {
    this.retry = { maxFailures: retry.maxFailures ?? 5, initialDelayMs: retry.initialDelayMs ?? 1000, maxDelayMs: retry.maxDelayMs ?? 30000 };
    if (!Number.isSafeInteger(this.retry.maxFailures) || this.retry.maxFailures < 1
      || !Number.isSafeInteger(this.retry.initialDelayMs) || this.retry.initialDelayMs < 1
      || !Number.isSafeInteger(this.retry.maxDelayMs) || this.retry.maxDelayMs < this.retry.initialDelayMs) throw new Error('Invalid metadata retry policy.');
  }
  status() {
    return { state: this.haltReason ? 'halted' : this.stopped ? 'stopped' : this.retryDelayMs !== undefined ? 'retrying' : this.running ? 'running' : 'idle',
      consecutiveMetadataFailures: this.metadataFailures, maxMetadataFailures: this.retry.maxFailures,
      retryDelayMs: this.retryDelayMs, haltReason: this.haltReason };
  }
  private async metadata<T>(operation: () => Promise<T>): Promise<T> {
    for (;;) {
      if (this.stopped) throw new ProcessorStopped();
      try {
        const value = await operation();
        if (this.stopped) throw new ProcessorStopped();
        this.metadataFailures = 0;
        this.retryDelayMs = undefined;
        return value;
      } catch (error) {
        if (error instanceof ProcessorStopped) throw error;
        if (!(error instanceof HttpError) || error.statusCode !== 503) throw new MetadataOperationError();
        this.metadataFailures++;
        if (this.metadataFailures >= this.retry.maxFailures) throw error;
        this.retryDelayMs = Math.min(this.retry.maxDelayMs, this.retry.initialDelayMs * 2 ** (this.metadataFailures - 1));
        console.warn('Metadata unavailable; background processing will retry.', { consecutiveFailures: this.metadataFailures, nextRetryMs: this.retryDelayMs });
        try { await wait(this.retryDelayMs, undefined, { signal: this.retryAbort.signal }); }
        catch { if (this.stopped) throw new ProcessorStopped(); throw new Error('Metadata retry wait interrupted.'); }
      }
    }
  }
  async start() {
    this.stopped = false;
    this.retryAbort = new AbortController();
    this.metadataFailures = 0; this.retryDelayMs = undefined; this.haltReason = undefined;
    // Never blindly repeat a billable, non-idempotent submission after a crash.
    try {
      for (const r of await this.metadata(() => this.store.all())) {
        if (ambiguous.has(r.phase)) await this.metadata(() => this.store.put({ ...r, phase: 'failed', error: 'Process interrupted during provider submission. Check TwelveLabs for orphan resources before uploading again.' }));
        else if (r.phase === 'probing') await this.metadata(() => this.store.put({ ...r, phase: 'queued' }));
      }
    } catch (error) {
      if (error instanceof ProcessorStopped) return;
      this.haltReason = error instanceof HttpError && error.statusCode === 503 ? 'metadata_unavailable' : 'storage_or_processing_error';
      this.stopped = true; this.retryDelayMs = undefined; throw error;
    }
    this.kick();
  }
  kick() {
    if (this.stopped || this.running) return;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.running = this.tick().catch(error => {
        if (this.stopped) return;
        this.haltReason = error instanceof HttpError && error.statusCode === 503 ? 'metadata_unavailable' : 'storage_or_processing_error';
        this.stopped = true; this.retryDelayMs = undefined;
        console.error('Metadata processor halted:', this.haltReason);
      }).finally(() => { this.running = undefined; this.kick(); });
    }, this.intervalMs);
    this.timer.unref();
  }
  async stop() { this.stopped = true; clearTimeout(this.timer); this.retryAbort.abort(); await this.running; this.retryDelayMs = undefined; }
  // A leased external worker advances one recording without starting the API timer
  // or recovering somebody else's records. Writes use its fenced repository.
  async processOne(id: string):Promise<{record:Recording;needsReview:boolean;retryAfterMs?:number}> {
    this.stopped = false;
    this.retryAbort = new AbortController();
    try {
      const record = await this.metadata(() => this.store.get(id));
      if (!record) throw new Error('Queued recording is missing.');
      if (ambiguous.has(record.phase)) {
        record.phase = 'failed'; record.error = 'Process interrupted during provider submission. Check TwelveLabs for orphan resources before uploading again.';
        await this.metadata(() => this.store.put(record));
        return { record, needsReview: true };
      }
      if (record.phase === 'probing') record.phase = 'queued';
      if (record.phase !== 'ready' && record.phase !== 'failed') {
        if (Date.now() - Date.parse(record.createdAt) > 4 * 3600_000) {
          record.phase = 'failed'; record.error = 'Processing deadline exceeded. Provider resources may still exist; inspect TwelveLabs.';
          await this.metadata(() => this.store.put(record));
        } else await this.advance(record);
      }
      return { record, needsReview: record.phase === 'failed' && ambiguous.has(record.providerError?.operation ?? ''),retryAfterMs:this.providerRetryRemaining(record) };
    } finally { this.stopped = true; this.retryDelayMs = undefined; }
  }
  private async tick() {
    // One background operation at a time bounds local I/O and upload memory.
    for (const record of await this.metadata(() => this.store.all())) {
      if (this.stopped) return;
      if (record.phase === 'ready' || record.phase === 'failed') continue;
      // A four-hour deadline prevents infinite processing of a stuck provider job.
      if (Date.now() - Date.parse(record.createdAt) > 4 * 3600_000) {
        await this.metadata(() => this.store.put({ ...record, phase: 'failed', error: 'Processing deadline exceeded. Provider resources may still exist; inspect TwelveLabs.' }));
        continue;
      }
      await this.advance(record);
    }
  }
  private async advance(r: Recording) {
    if(this.providerRetryRemaining(r)>0)return;
    // Retry only the metadata operation, retaining this in-flight record and any
    // returned provider IDs. Re-running the whole advance could submit paid work twice.
    const persist = () => this.metadata(() => this.store.put(r));
    try {
      if (r.phase === 'queued') {
        r.phase = 'probing'; await persist();
        try { r.media = await probeMedia(this.store.mediaPath(r.id)); }
        catch (error) {
          r.phase = 'failed';
          // ffprobe errors include paths; only our validation messages are public.
          r.error = error instanceof Error && !('code' in error) && !error.message.includes('JSON') ? error.message : 'Cannot probe recording. Verify the MP4 and FFPROBE_PATH.';
          await persist(); return;
        }
        r.phase = 'creating_index'; await persist();
        r.indexId = await this.provider.ensureIndex(r.id);
        r.phase = 'creating_upload'; await persist();
        r.upload = await this.provider.createUpload(this.store.mediaPath(r.id), r.filename, r.bytes);
        r.assetId = r.upload.assetId;
        r.phase = 'uploading'; await persist();
      }
      if (r.phase === 'uploading') {
        if (!r.upload) throw new Error('Missing upload session');
        await this.provider.transfer(this.store.mediaPath(r.id), r.bytes, r.upload, async completed => {
          r.upload!.completedChunks = completed;r.pollErrors=0;delete r.providerError;delete r.error;await persist();
        });
        r.phase = 'asset_processing';r.pollErrors=0;delete r.providerError;delete r.error;await persist();return;
      }
      if (r.phase === 'asset_processing') {
        r.providerStatus = await this.provider.assetStatus(r.assetId!);
        r.pollErrors = 0;
        if (r.providerStatus === 'failed') { r.phase = 'failed'; r.error = 'TwelveLabs asset processing failed. Inspect the asset in TwelveLabs.'; }
        else if (r.providerStatus === 'ready') {
          r.phase = 'submitting_index'; await persist();
          r.indexedAssetId = await this.provider.indexAsset(r.indexId!, r.assetId!);
          r.phase = 'indexing';
        }
        await persist(); return;
      }
      if (r.phase === 'indexing') {
        r.providerStatus = await this.provider.indexStatus(r.indexId!, r.indexedAssetId!);
        r.pollErrors = 0;
        if (r.providerStatus === 'ready') r.phase = 'ready';
        else if (r.providerStatus === 'failed') { r.phase = 'failed'; r.error = 'TwelveLabs indexing failed. Inspect the indexed asset in TwelveLabs.'; }
        await persist();
      }
    } catch (error) {
      if (error instanceof ProcessorStopped || error instanceof MetadataOperationError || this.stopped) throw error;
      if (error instanceof HttpError && error.statusCode === 503) throw error;
      const failure = describeProviderFailure(error, r.phase);
      r.providerError = failure.details;
      console.error('Recording provider operation failed', { recordingId: r.id, ...failure.details });
      if(r.phase==='uploading'&&isTransientProviderError(error)&&(r.pollErrors??0)<this.retry.maxFailures-1){
        r.pollErrors=(r.pollErrors??0)+1;r.updatedAt=new Date().toISOString();r.error='Temporary provider upload failure; retrying the existing upload session.';
      }else if ((r.phase === 'asset_processing' || r.phase === 'indexing') && (r.pollErrors ?? 0) < 4) {
        r.pollErrors = (r.pollErrors ?? 0) + 1;
      } else {
        r.phase = 'failed';
        r.error = failure.message;
      }
      await persist();
    }
  }
  private providerRetryRemaining(r:Recording){return r.phase==='uploading'&&r.providerError?.operation==='uploading'&&(r.pollErrors??0)>0?
    Math.max(0,Date.parse(r.updatedAt)+Math.min(this.retry.maxDelayMs,this.retry.initialDelayMs*2**((r.pollErrors??1)-1))-Date.now()):0;}
}
