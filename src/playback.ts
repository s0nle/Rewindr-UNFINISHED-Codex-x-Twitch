import type { Broadcast, BroadcastChunk } from './types.js';
import type { ChunkStorage } from './chunk-storage.js';
import { HttpError } from './http-errors.js';

export function playbackPlan(broadcast: Broadcast, startSeconds: number, endSeconds: number) {
  if (!Number.isFinite(startSeconds) || !Number.isFinite(endSeconds) || startSeconds < 0 || endSeconds <= startSeconds || endSeconds - startSeconds > 300) return undefined;
  const segments: { chunk: BroadcastChunk; startSeconds: number; endSeconds: number; recordingStartSeconds: number; recordingEndSeconds: number }[] = [];
  let cursor = startSeconds;
  for (const chunk of [...broadcast.chunks].sort((a, b) => a.startSeconds - b.startSeconds)) {
    if (chunk.endSeconds <= cursor || chunk.startSeconds >= endSeconds) continue;
    if (chunk.startSeconds > cursor + 0.1 || !chunk.storage) return undefined;
    const start = Math.max(cursor, chunk.startSeconds); const end = Math.min(endSeconds, chunk.endSeconds);
    segments.push({ chunk, startSeconds: start - chunk.startSeconds, endSeconds: end - chunk.startSeconds,
      recordingStartSeconds: start, recordingEndSeconds: end });
    cursor = end;
    if (cursor >= endSeconds - 0.001) break;
  }
  return segments.length && cursor >= endSeconds - 0.1 ? { startSeconds, endSeconds, segments } : undefined;
}

export function candidatePlayback(broadcast: Broadcast, candidate: { startSeconds: number; endSeconds: number; matchStartSeconds: number; matchEndSeconds: number }, storageEnabled: boolean) {
  const core = storageEnabled && playbackPlan(broadcast, candidate.matchStartSeconds, candidate.matchEndSeconds);
  if (!core) return {}; // preserve the existing local-source fallback, if present
  const chunks = [...broadcast.chunks].sort((a, b) => a.startSeconds - b.startSeconds);
  let first = chunks.findIndex(chunk => chunk.indexedAssetId === core.segments[0]!.chunk.indexedAssetId);
  let last = chunks.findIndex(chunk => chunk.indexedAssetId === core.segments.at(-1)!.chunk.indexedAssetId);
  while (first > 0 && chunks[first - 1]!.storage && chunks[first]!.startSeconds - chunks[first - 1]!.endSeconds <= 0.1) first--;
  while (last < chunks.length - 1 && chunks[last + 1]!.storage && chunks[last + 1]!.startSeconds - chunks[last]!.endSeconds <= 0.1) last++;
  const start = Math.max(candidate.startSeconds, chunks[first]!.startSeconds);
  const end = Math.min(candidate.endSeconds, chunks[last]!.endSeconds);
  const query = new URLSearchParams({ startSeconds: String(start), endSeconds: String(end) }).toString();
  const pageUrl = `/broadcasts/${broadcast.id}/replay?${query}`;
  return { replayAvailable: true, replayMode: 'stored-chunks', playbackFormat: 'html', playbackUrl: pageUrl,
    manifestUrl: `/broadcasts/${broadcast.id}/playback?${query}`, playbackStartSeconds: start, playbackEndSeconds: end,
    contextTrimmed: start !== candidate.startSeconds || end !== candidate.endSeconds };
}

export async function signedPlayback(broadcast: Broadcast, start: number, end: number, storage: ChunkStorage) {
  const plan = playbackPlan(broadcast, start, end);
  if (!plan) throw new HttpError(409, 'Some requested footage is not stored or contains a timeline gap. Upload its chunks before replaying.');
  const segments = [];
  for (const segment of plan.segments) {
    const signed = await storage.sign(broadcast.id, segment.chunk.indexedAssetId, segment.chunk.storage!,segment.chunk.storageKey);
    segments.push({ filename: segment.chunk.filename, indexedAssetId: segment.chunk.indexedAssetId,
      startSeconds: segment.startSeconds, endSeconds: segment.endSeconds,
      recordingStartSeconds: segment.recordingStartSeconds, recordingEndSeconds: segment.recordingEndSeconds,
      broadcastStartSeconds: broadcast.sourceOffsetSeconds + segment.recordingStartSeconds, ...signed });
  }
  return { broadcastId: broadcast.id, startSeconds: start, endSeconds: end, sourceOffsetSeconds: broadcast.sourceOffsetSeconds,
    segments, expiresAt: segments.reduce((earliest, segment) => segment.expiresAt < earliest ? segment.expiresAt : earliest, segments[0]!.expiresAt) };
}
