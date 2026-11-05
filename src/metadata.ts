import { z } from 'zod';
import { chunkFilenameSchema } from './filenames.js';
const identifier = z.string().min(1).max(200);
export const storageRefSchema = z.object({ bucket: z.string().regex(/^[a-z0-9][a-z0-9_-]{1,62}$/),
  path: z.string().regex(/^[a-f0-9-]{36}\/[a-f0-9-]{36}\/[a-f0-9]{64}\/[a-f0-9]{64}\.mp4$/),
  sha256: z.string().regex(/^[a-f0-9]{64}$/), bytes: z.number().int().positive().max(50_000_000), uploadedAt: z.iso.datetime({ offset: true }) }).strict();
export const recordingSchema = z.object({
  id: z.uuid(), filename: z.string().min(1).max(200), bytes: z.number().int().nonnegative().max(4_000_000_000),
  sha256: z.string().regex(/^[a-f0-9]{64}$/), creatorAuthorized: z.literal(true),
  authorizedAt: z.iso.datetime({ offset: true }), createdAt: z.iso.datetime({ offset: true }), updatedAt: z.iso.datetime({ offset: true }),
  phase: z.enum(['queued', 'probing', 'creating_index', 'creating_upload', 'uploading', 'asset_processing', 'submitting_index', 'indexing', 'ready', 'failed']),
  media: z.object({ durationSeconds: z.number().finite().positive(), width: z.number().int().positive(), height: z.number().int().positive(), videoCodec: z.string().min(1) }).optional(),
  indexId: identifier.optional(), assetId: identifier.optional(), indexedAssetId: identifier.optional(),
  upload: z.object({ uploadId: identifier, assetId: identifier, chunkSize: z.number().int().positive(), totalChunks: z.number().int().positive(), completedChunks: z.number().int().nonnegative(), uploadHeaders: z.record(z.string(), z.string()) }).optional(),
  providerStatus: z.string().optional(), error: z.string().optional(), pollErrors: z.number().int().nonnegative().optional(),
  providerError: z.object({ operation: z.string(), httpStatus: z.number().int().optional(), code: z.string().optional() }).optional()
});
export const broadcastSchema = z.object({
  id: z.uuid(), title: z.string().min(1).max(200), indexId: identifier, createdAt: z.iso.datetime({ offset: true }),
  sourceOffsetSeconds: z.number().finite().nonnegative(), sourceRecordingId: z.uuid().optional(),ingestionId:z.uuid().optional(),
  chunks: z.array(z.object({ filename: chunkFilenameSchema, indexedAssetId: identifier, assetId: identifier.optional(), startSeconds: z.number().finite().nonnegative(), endSeconds: z.number().finite().positive(), storage: storageRefSchema.optional(),
    storageKey:z.string().regex(/^ingest:[a-f0-9-]{36}$/).optional() })).max(2000)
}).superRefine((broadcast, context) => {
  const sorted = [...broadcast.chunks].sort((a, b) => a.startSeconds - b.startSeconds);
  if (new Set(sorted.map(chunk => chunk.filename)).size !== sorted.length || new Set(sorted.map(chunk => chunk.indexedAssetId)).size !== sorted.length) context.addIssue({ code: 'custom', message: 'Duplicate chunk mapping.' });
  sorted.forEach((chunk, index) => {
    if (chunk.endSeconds <= chunk.startSeconds || chunk.endSeconds - chunk.startSeconds > 65 || (index > 0 && chunk.startSeconds < sorted[index - 1]!.endSeconds - 0.1)) context.addIssue({ code: 'custom', message: 'Invalid or overlapping chunk interval.' });
  });
});
