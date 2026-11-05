import type { FastifyInstance } from 'fastify';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { broadcastSearchSchema, chunkEncounters, mapChunkMoments } from './chunk-search.js';
import type { Broadcast, IndexedChunk, Recording, VideoSearchProvider } from './types.js';
import { HttpError as ApiError } from './http-errors.js';
import { chunkFilenameSchema as filename } from './filenames.js';
import type { BroadcastRepository } from './persistence.js';
import { candidatePlayback } from './playback.js';
import type {IngestionRepository} from './ingestions.js';

const identifier = z.string().trim().min(1).max(200).regex(/^[a-zA-Z0-9_-]+$/);
const intervalSchema = z.object({
  filename, startSeconds: z.number().finite().nonnegative(), endSeconds: z.number().finite().positive(),
  indexedAssetId: identifier.optional()
}).strict().refine(chunk => chunk.endSeconds > chunk.startSeconds && chunk.endSeconds - chunk.startSeconds <= 65,
  'Chunks must be positive intervals of at most 65 seconds.');
export const importBroadcastSchema = z.object({
  title: z.string().trim().min(1).max(200), indexId: identifier,
  sourceOffsetSeconds: z.number().finite().nonnegative().default(0),
  sourceRecordingId: z.uuid().optional(), chunks: z.array(intervalSchema).min(1).max(2000)
}).strict().superRefine((value, context) => {
  const sorted = [...value.chunks].sort((a, b) => a.startSeconds - b.startSeconds);
  if (new Set(sorted.map(chunk => chunk.filename)).size !== sorted.length) {
    context.addIssue({ code: 'custom', message: 'Chunk filenames must be unique.' });
  }
  for (let i = 1; i < sorted.length; i++) {
    // Small muxing differences are tolerated; deliberate overlap is not the default workflow.
    if (sorted[i]!.startSeconds < sorted[i - 1]!.endSeconds - 0.1) {
      context.addIssue({ code: 'custom', message: 'Chunk intervals must not overlap.' });
    }
  }
});

export async function registerBroadcastRoutes(app: FastifyInstance, options: {
  store: BroadcastRepository; persistence: 'local' | 'supabase';ingestions?:IngestionRepository; storageEnabled?: boolean; provider?: VideoSearchProvider; recording: (id: string) => Promise<Recording | undefined>;
}) {
  const lookup = async (id: string) => {
    const broadcast = await options.store.get(id);
    if (!broadcast) throw new ApiError(404, 'Broadcast not found.');
    return broadcast;
  };
  const validateSource = async (broadcast: { sourceRecordingId?: string; chunks: { endSeconds: number }[] }) => {
    if (!broadcast.sourceRecordingId) return;
    const source = await options.recording(broadcast.sourceRecordingId);
    if (!source?.media) throw new ApiError(400, 'The source recording must exist locally and have validated media.');
    if (Math.max(...broadcast.chunks.map(chunk => chunk.endSeconds)) > source.media.durationSeconds + 0.1) {
      throw new ApiError(400, 'Chunk timeline extends beyond the attached source recording.');
    }
  };
  app.post('/broadcasts/import', { bodyLimit: 1024 * 1024 }, async (req, reply) => {
    if (req.headers['x-creator-authorized'] !== 'true') throw new ApiError(400, 'Send X-Creator-Authorized: true for authorized footage.');
    const parsed = importBroadcastSchema.safeParse(req.body);
    if (!parsed.success) throw new ApiError(400, 'Provide a title, indexId, and unique non-overlapping chunk intervals of at most 65 seconds.');
    if (!options.provider?.listIndexedChunks) throw new ApiError(503, 'Configure TwelveLabs before importing indexed chunks.');
    const input = parsed.data;
    await validateSource(input);
    let catalog: IndexedChunk[];
    try { catalog = await options.provider.listIndexedChunks(input.indexId); }
    catch { throw new ApiError(502, 'Could not read the TwelveLabs index. Check credentials, index ID, and provider status.'); }
    const chunks = input.chunks.map(chunk => {
      const matching = catalog.filter(asset => asset.filename === chunk.filename
        && (!chunk.indexedAssetId || asset.indexedAssetId === chunk.indexedAssetId));
      if (matching.length !== 1) throw new ApiError(409, `Expected one indexed asset for ${chunk.filename}; supply indexedAssetId if filenames are duplicated in the index.`);
      const asset = matching[0]!;
      if (asset.status !== 'ready') throw new ApiError(409, `${chunk.filename} has not finished indexing.`);
      if (Math.abs(asset.durationSeconds - (chunk.endSeconds - chunk.startSeconds)) > 1) {
        throw new ApiError(409, `Duration mismatch for ${chunk.filename}. Use the manifest from these exact straight-cut files.`);
      }
      return { ...chunk, indexedAssetId: asset.indexedAssetId, assetId: asset.assetId };
    }).sort((a, b) => a.startSeconds - b.startSeconds);
    if (new Set(chunks.map(chunk => chunk.indexedAssetId)).size !== chunks.length) throw new ApiError(409, 'An indexed asset cannot represent two different chunks.');
    const broadcast: Broadcast = { id: randomUUID(), title: input.title, indexId: input.indexId,
      sourceOffsetSeconds: input.sourceOffsetSeconds, sourceRecordingId: input.sourceRecordingId,
      chunks, createdAt: new Date().toISOString() };
    await options.store.put(broadcast);
    return reply.code(201).send({ broadcastId: broadcast.id, chunkCount: chunks.length,
      statusUrl: `/broadcasts/${broadcast.id}/status`, searchUrl: `/broadcasts/${broadcast.id}/search`,
      replayAvailable: Boolean(broadcast.sourceRecordingId) });
  });
  app.get<{ Params: { id: string } }>('/broadcasts/:id/status', async req => {
    const broadcast = await lookup(req.params.id);
    let contiguousEnd = 0;
    for (const chunk of broadcast.chunks) {
      if (chunk.startSeconds > contiguousEnd + 0.1) break;
      contiguousEnd = Math.max(contiguousEnd, chunk.endSeconds);
    }
    return { ...broadcast, chunkCount: broadcast.chunks.length,
      searchableThroughSeconds: contiguousEnd, readyVerifiedAt: broadcast.createdAt,
      replayAvailable: Boolean(broadcast.sourceRecordingId) || Boolean(options.storageEnabled && broadcast.chunks.some(chunk => chunk.storage)),
      storedChunkCount: broadcast.chunks.filter(chunk => chunk.storage).length,
      ...(broadcast.ingestionId&&options.ingestions?{ingestion:await options.ingestions.status(broadcast.ingestionId)}:{}),
      persistence: options.persistence === 'supabase' ? `Supabase PostgreSQL metadata; ${options.storageEnabled ? 'private Storage chunks and local source media' : 'local source media'}` : 'local JSON; not Supabase or a live ingestion worker' };
  });
  app.post<{ Params: { id: string } }>('/broadcasts/:id/search', async req => {
    const broadcast = await lookup(req.params.id);
    const parsed = broadcastSearchSchema.safeParse(req.body);
    if (!parsed.success) throw new ApiError(400, 'Provide question, optional candidateLimit (3-50), modalities, operator, and transcriptionOptions.');
    if (!options.provider?.searchMany) throw new ApiError(503, 'Configure TwelveLabs before searching.');
    await validateSource(broadcast);
    const { question, candidateLimit, ...settings } = parsed.data;
    if(!broadcast.chunks.length)return {broadcastId:broadcast.id,question,provider:'twelvelabs',model:'marengo3.0',...settings,candidateLimit,rawMatchCount:0,moments:[],encounters:[],moreCandidatesAvailable:false,
      pendingIngestion:true,uncertainty:'No chunks are indexed and ready yet. This does not establish that the event is absent.'};
    try {
      const raw = await options.provider.searchMany(broadcast.indexId,
        broadcast.chunks.map(chunk => chunk.indexedAssetId), question, candidateLimit, settings);
      const moments = mapChunkMoments(broadcast, raw);
      const encounters = chunkEncounters(broadcast, moments);
      return { broadcastId: broadcast.id, question, provider: 'twelvelabs', model: 'marengo3.0',
        ...settings, candidateLimit, rawMatchCount: moments.length, moments,
        encounters: encounters.slice(0, 3).map(candidate => ({ ...candidate, ...candidatePlayback(broadcast, candidate, Boolean(options.storageEnabled)) })), moreCandidatesAvailable: encounters.length > 3,
        uncertainty: 'Ranked matches need review. Context and grouping do not verify an encounter. No query rewrite or Pegasus analysis has run.' };
    } catch { throw new ApiError(502, 'TwelveLabs search failed or returned invalid chunk data. Check the index, selected modalities, and provider status.'); }
  });
}
