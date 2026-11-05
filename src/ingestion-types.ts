import { z } from 'zod';
import { chunkFilenameSchema } from './filenames.js';
import { storageRefSchema, recordingSchema } from './metadata.js';

export const MAX_SOURCE_SECONDS = 12 * 3600;
export const createIngestionSchema = z.object({id:z.uuid(),title:z.string().trim().min(1).max(200),indexId:z.string().regex(/^[a-zA-Z0-9_-]{1,200}$/),
  sourceDurationSeconds:z.number().finite().min(4).max(MAX_SOURCE_SECONDS),sourceOffsetSeconds:z.number().finite().nonnegative().default(0),
  sourceFingerprint:z.string().regex(/^[a-f0-9]{64}$/)}).strict();
export type CreateIngestion = z.infer<typeof createIngestionSchema>;
export const createLiveIngestionSchema=createIngestionSchema.omit({sourceDurationSeconds:true}).extend({mode:z.literal('live')}).strict();
export type CreateLiveIngestion=z.infer<typeof createLiveIngestionSchema>;
export const ingestionCreationSchema=z.union([createLiveIngestionSchema,createIngestionSchema]);
export const ingestCheckpointSchema = z.object({phase:z.enum(['queued','stored','creating_upload','uploading','asset_processing','submitting_index','indexing','ready','failed']),
  storage:storageRefSchema.optional(),upload:recordingSchema.shape.upload,assetId:z.string().min(1).optional(),indexedAssetId:z.string().min(1).optional(),
  providerStatus:z.string().optional(),error:z.string().max(500).optional()}).strict();
export type IngestCheckpoint = z.infer<typeof ingestCheckpointSchema>;
export const ingestChunkSchema = z.object({id:z.uuid(),owner_id:z.uuid(),ingestion_id:z.uuid(),ordinal:z.number().int().nonnegative(),filename:chunkFilenameSchema,
  start_seconds:z.coerce.number().finite().nonnegative(),end_seconds:z.coerce.number().finite().positive(),file_id:z.uuid(),sha256:z.string().regex(/^[a-f0-9]{64}$/),
  bytes:z.coerce.number().int().positive().max(50_000_000),job_id:z.uuid(),checkpoint:ingestCheckpointSchema,created_at:z.string()});
export type IngestChunk = z.infer<typeof ingestChunkSchema>;
export const ingestionSchema = z.object({id:z.uuid(),owner_id:z.uuid(),title:z.string(),index_id:z.string(),mode:z.enum(['recorded','live']).default('recorded'),source_duration_seconds:z.coerce.number().finite(),
  source_offset_seconds:z.coerce.number().finite(),source_fingerprint:z.string(),sealed_at:z.string().nullable(),created_at:z.string()});
export type Ingestion = z.infer<typeof ingestionSchema>;

// Hold a short tail with the preceding minute. Every indexed file is >=4 sec,
// all source footage is retained, and adjacent source intervals do not overlap.
export function planChunkIntervals(duration:number,segmentSeconds=60) {
  if(!Number.isFinite(duration)||duration<4||duration>MAX_SOURCE_SECONDS||!Number.isInteger(segmentSeconds)||segmentSeconds<5||segmentSeconds>60)throw Error('Invalid source duration or chunk size.');
  // Match PostgreSQL's integer-microsecond arithmetic, including exact 4.1s tails.
  // The valid range always serializes without exponent notation. Quantize that
  // canonical JSON decimal so binary multiplication cannot move a half-us tie.
  const [seconds,fraction='']=String(duration).split('.');
  const durationUs=Number(seconds)*1_000_000+Number(fraction.padEnd(6,'0').slice(0,6))+(Number(fraction[6]??'0')>=5?1:0);
  const segmentUs=segmentSeconds*1_000_000,boundaries=[0];
  for(let next=segmentUs;next<=durationUs-4_100_000;next+=segmentUs)boundaries.push(next);
  boundaries.push(durationUs);
  return boundaries.slice(0,-1).map((start,ordinal)=>({ordinal,filename:`chunk_${String(ordinal).padStart(4,'0')}.mp4`,startSeconds:start/1_000_000,endSeconds:boundaries[ordinal+1]!/1_000_000}));
}
export const ingestStorageKey=(chunkId:string)=>`ingest:${z.uuid().parse(chunkId).toLowerCase()}`;
