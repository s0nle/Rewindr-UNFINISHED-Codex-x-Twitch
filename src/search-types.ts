import {z} from 'zod';
import {broadcastSchema} from './metadata.js';
import {searchSettingsSchema} from './chunk-search.js';
export const SEARCH_MAX_RECOVERIES=3;
export const ANALYSIS_REVISION='evidence-v1';
export const intervalSchema=z.object({startSeconds:z.number().finite().nonnegative(),endSeconds:z.number().finite().positive()}).strict().refine(v=>v.endSeconds>v.startSeconds);
export type Interval=z.infer<typeof intervalSchema>;
export const hitSchema=z.object({videoId:z.string().min(1).max(200),startSeconds:z.number().finite().nonnegative(),endSeconds:z.number().finite().positive(),rank:z.number().int().positive(),query:z.string().min(1).max(1200)}).strict();
export type SearchHit=z.infer<typeof hitSchema>;
export const mappedSchema=z.object({videoId:z.string(),filename:z.string(),rank:z.number().int().positive(),chunkStartSeconds:z.number(),chunkEndSeconds:z.number(),
  recordingStartSeconds:z.number(),recordingEndSeconds:z.number(),broadcastStartSeconds:z.number(),broadcastEndSeconds:z.number()});
export const selectionSchema=z.object({candidateId:z.string().regex(/^[a-f0-9]{64}$/),matchStartSeconds:z.number().finite().nonnegative(),matchEndSeconds:z.number().finite().positive(),
  startSeconds:z.number().finite().nonnegative(),endSeconds:z.number().finite().positive(),bestProviderRank:z.number().int().positive(),fusionScore:z.number().finite().nonnegative(),providerMatches:z.array(mappedSchema).min(1).max(400)}).strict();
export type Selection=z.infer<typeof selectionSchema>;
export const searchStateSchema=z.object({pool:z.array(hitSchema).max(400),current:z.array(selectionSchema).max(3),rejected:z.array(intervalSchema).max(12),
  usedQueries:z.array(z.string().max(1200)).min(1).max(7),round:z.number().int().min(0).max(SEARCH_MAX_RECOVERIES),warnings:z.array(z.string().max(300)).max(5),
  stopReason:z.enum(['none','pool_exhausted','recovery_limit']).default('none')}).strict();
export type SearchState=z.infer<typeof searchStateSchema>;
export const sessionSettingsSchema=searchSettingsSchema.extend({candidateLimit:z.number().int().min(3).max(50).default(30)});
export const searchRowSchema=z.object({id:z.uuid(),owner_id:z.uuid(),broadcast_id:z.uuid(),question:z.string().min(1).max(1000),settings:sessionSettingsSchema,
  snapshot:broadcastSchema,state:searchStateSchema,version:z.number().int().nonnegative(),phase:z.enum(['ready','recovering']),pending_token:z.uuid().nullable(),
  pending_until:z.string().nullable(),last_request_id:z.uuid().nullable(),expires_at:z.string(),created_at:z.string()});
export type SearchRow=z.infer<typeof searchRowSchema>;
export const assessmentSchema=z.object({verdict:z.enum(['match','partial_match','no_match','unclear']),summary:z.string().trim().min(1).max(300),
  matchingDetails:z.array(z.string().trim().min(1).max(180)).max(4),unclearDetails:z.array(z.string().trim().min(1).max(180)).max(4),
  evidence:z.array(z.object({timestampSeconds:z.number().finite().nonnegative(),detail:z.string().trim().min(1).max(180),modality:z.enum(['visual','audio','transcription'])}).strict()).max(4)}).strict();
export type Assessment=z.infer<typeof assessmentSchema>;
export const analysisPartSchema=z.object({id:z.uuid(),indexedAssetId:z.string().min(1).max(200),assetId:z.string().min(1).max(200).optional(),
  chunkStartSeconds:z.number().finite().nonnegative(),startSeconds:z.number().finite().nonnegative(),endSeconds:z.number().finite().positive(),
  storage:broadcastSchema.shape.chunks.element.shape.storage,storageKey:broadcastSchema.shape.chunks.element.shape.storageKey}).strict();
export type AnalysisPart=z.infer<typeof analysisPartSchema>;
export const partCheckpointSchema=z.object({phase:z.enum(['queued','submitting','processing','ready']),taskId:z.string().min(1).max(200).optional(),assessment:assessmentSchema.optional(),failureKind:z.enum(['provider_terminal','invalid_evidence']).optional()}).strict();
export const analysisCheckpointSchema=z.object({parts:z.array(partCheckpointSchema).max(3)}).strict();
export type AnalysisCheckpoint=z.infer<typeof analysisCheckpointSchema>;
export const analysisRowSchema=z.object({id:z.uuid(),owner_id:z.uuid(),broadcast_id:z.uuid(),cache_key:z.string().regex(/^[a-f0-9]{64}$/),question:z.string().min(1).max(1000),
  model:z.enum(['pegasus1.5','pegasus1.6']),revision:z.string(),plan:z.array(analysisPartSchema).max(3),checkpoint:analysisCheckpointSchema,
  status:z.enum(['queued','processing','ready','failed','needs_review','unavailable','skipped']),result:assessmentSchema.nullable(),error:z.string().nullable(),job_id:z.uuid().nullable(),created_at:z.string(),attempt_started_at:z.string().optional()});
export type AnalysisRow=z.infer<typeof analysisRowSchema>;
export class InvalidAnalysisOutputError extends Error {}
export interface AnalysisProvider {
  readonly model:'pegasus1.5'|'pegasus1.6';
  create(part:AnalysisPart,question:string,url?:string):Promise<string>;
  retrieve(taskId:string):Promise<{status:'queued'|'pending'|'processing'|'ready'|'failed'|'canceled';assessment?:Assessment}>;
}
