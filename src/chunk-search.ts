import { z } from 'zod';
import type { Broadcast, ProviderMoment } from './types.js';

export const searchSettingsSchema = z.object({
  modalities: z.array(z.enum(['visual', 'audio', 'transcription'])).min(1).max(3)
    .refine(values => new Set(values).size === values.length, 'Duplicate search modalities.').default(['visual']),
  operator: z.enum(['or', 'and']).default('or'),
  transcriptionOptions: z.array(z.enum(['lexical', 'semantic'])).min(1).max(2)
    .refine(values => new Set(values).size === values.length, 'Duplicate transcription options.').default(['lexical', 'semantic'])
});

export const broadcastSearchSchema = searchSettingsSchema.extend({
  question: z.string().trim().min(1).max(1000),
  candidateLimit: z.number().int().min(3).max(50).default(30)
}).strict();

// IDs are mapped explicitly. Neither upload order nor a filename's number is a timestamp.
export function mapChunkMoments(broadcast: Broadcast, moments: ProviderMoment[]) {
  const chunks = new Map(broadcast.chunks.map(chunk => [chunk.indexedAssetId, chunk]));
  const sourceUrl = broadcast.sourceRecordingId ? `/recordings/${broadcast.sourceRecordingId}/source` : null;
  return moments.map(moment => {
    const chunk = chunks.get(moment.videoId);
    if (!chunk || !Number.isFinite(moment.startSeconds) || !Number.isFinite(moment.endSeconds)
      || !Number.isSafeInteger(moment.rank) || moment.rank < 1 || moment.startSeconds < 0
      || moment.endSeconds <= moment.startSeconds || moment.startSeconds >= chunk.endSeconds - chunk.startSeconds
      || moment.endSeconds > chunk.endSeconds - chunk.startSeconds + 1) {
      throw new Error('Provider returned an unmapped source or invalid chunk timestamps.');
    }
    // Encoded audio/video duration can differ slightly from the source manifest.
    const localEnd = Math.min(moment.endSeconds, chunk.endSeconds - chunk.startSeconds);
    const start = chunk.startSeconds + moment.startSeconds;
    const end = chunk.startSeconds + localEnd;
    return {
      videoId: moment.videoId, filename: chunk.filename, rank: moment.rank,
      chunkStartSeconds: moment.startSeconds, chunkEndSeconds: localEnd,
      recordingStartSeconds: start, recordingEndSeconds: end,
      broadcastStartSeconds: broadcast.sourceOffsetSeconds + start,
      broadcastEndSeconds: broadcast.sourceOffsetSeconds + end,
      sourceUrl, playbackUrl: sourceUrl ? `${sourceUrl}#t=${start}` : null
    };
  }).sort((a, b) => a.rank - b.rank);
}

export function chunkEncounters(broadcast: Broadcast, moments: ReturnType<typeof mapChunkMoments>) {
  const groups: { start: number; end: number; matches: typeof moments }[] = [];
  for (const moment of [...moments].sort((a, b) => a.recordingStartSeconds - b.recordingStartSeconds)) {
    const previous = groups.at(-1);
    // Merge boundary hits only where the manifest actually has continuous footage.
    const continuous = previous && !broadcast.chunks.some((chunk, index) => {
      const next = broadcast.chunks[index + 1];
      return next && next.startSeconds - chunk.endSeconds > 0.1
        && chunk.endSeconds >= previous.end - 0.1 && next.startSeconds <= moment.recordingStartSeconds + 0.1;
    });
    if (previous && continuous && moment.recordingStartSeconds <= previous.end + 3
      && Math.max(previous.end, moment.recordingEndSeconds) - previous.start <= 90) {
      previous.end = Math.max(previous.end, moment.recordingEndSeconds);
      previous.matches.push(moment);
    } else groups.push({ start: moment.recordingStartSeconds, end: moment.recordingEndSeconds, matches: [moment] });
  }
  const duration = Math.max(...broadcast.chunks.map(chunk => chunk.endSeconds));
  return groups.sort((a, b) => Math.min(...a.matches.map(m => m.rank)) - Math.min(...b.matches.map(m => m.rank)))
    .map((group, index) => {
      const start = Math.max(0, group.start - 30);
      const end = Math.min(duration, group.end + 30);
      const sourceUrl = broadcast.sourceRecordingId ? `/recordings/${broadcast.sourceRecordingId}/source` : null;
      return {
        candidateNumber: index + 1, bestProviderRank: Math.min(...group.matches.map(m => m.rank)),
        matchStartSeconds: group.start, matchEndSeconds: group.end,
        broadcastMatchStartSeconds: broadcast.sourceOffsetSeconds + group.start,
        broadcastMatchEndSeconds: broadcast.sourceOffsetSeconds + group.end,
        startSeconds: start, endSeconds: end, providerMatches: group.matches.sort((a, b) => a.rank - b.rank),
        sourceUrl, playbackUrl: sourceUrl ? `${sourceUrl}#t=${start}` : null,
        replayAvailable: Boolean(sourceUrl), analysisStatus: 'not_implemented' as const
      };
    });
}
