import {setTimeout as wait} from 'node:timers/promises';
import type {Job} from './jobs.js';
import type {SearchRepository} from './search-store.js';
import type {AnalysisProvider} from './search-types.js';
import {assessmentSchema,InvalidAnalysisOutputError} from './search-types.js';
import type {ChunkStorage} from './chunk-storage.js';
import {HttpError} from './http-errors.js';
import {LeaseLostError} from './jobs.js';
import {combineAssessments} from './search-ranking.js';

export async function advanceAnalysis(options:{job:Job;repository:SearchRepository;provider:AnalysisProvider;storage:ChunkStorage;guard:()=>Promise<void>;retryMs?:number}) {
  const {job,repository,provider,storage,guard}=options;const analysis=await repository.analysis(String(job.input.analysisId));
  if(!analysis||analysis.job_id!==job.id||analysis.broadcast_id!==job.broadcast_id)throw new HttpError(409,'Analysis job identity is invalid.');
  if(analysis.model!==provider.model)throw new HttpError(409,'Analysis model configuration changed.');
  const cp=structuredClone(analysis.checkpoint);
  const persist=async(status:'processing'|'ready'|'failed'|'needs_review'|'skipped'='processing',result?:ReturnType<typeof combineAssessments>,error?:string)=>{
    for(let attempt=0;;attempt++){await guard();try{await repository.checkpoint(job,cp,status,result,error);return;}catch(e){if(!(e instanceof HttpError)||e.statusCode!==503||attempt>=4)throw e;await wait(Math.min(8000,(options.retryMs??1000)*2**attempt));}}
  };
  const index=cp.parts.findIndex(part=>part.phase!=='ready');
  if(index<0){await persist('ready',combineAssessments(analysis.plan.map((plan,i)=>({plan,assessment:cp.parts[i]!.assessment!}))));return 'completed' as const;}
  const part=analysis.plan[index]!,state=cp.parts[index]!;
  if(state.phase==='submitting'){await persist('needs_review',undefined,'Analysis task creation was interrupted. Reconcile the task before requesting it again.');return 'completed' as const;}
  if(Date.now()-Date.parse(analysis.attempt_started_at??job.created_at)>3600_000){await persist('failed',undefined,'Candidate analysis exceeded its one-hour deadline.');return 'completed' as const;}
  if(state.phase==='queued') {
    if(!await repository.active(analysis.id)){await persist('skipped');return 'completed' as const;}
    let url:string|undefined;
    if(!part.assetId){if(!part.storage)throw new HttpError(409,'Candidate media is unavailable.');url=(await storage.sign(analysis.broadcast_id,part.indexedAssetId,part.storage,part.storageKey)).url;}
    state.phase='submitting';await persist();await guard();
    try{state.taskId=await provider.create(part,analysis.question,url);}catch(error){if(error instanceof LeaseLostError)throw error;await persist('needs_review',undefined,'Analysis task submission could not be confirmed. Playback remains available.');return 'completed' as const;}
    state.phase='processing';await persist();return 'pending' as const;
  }
  if(!state.taskId)throw new HttpError(409,'Analysis task checkpoint is missing.');
  await guard();let task;
  try{task=await provider.retrieve(state.taskId);}catch(e){if(!(e instanceof InvalidAnalysisOutputError))throw e;state.failureKind='invalid_evidence';await persist('failed',undefined,'Analysis output was incomplete or invalid.');return 'completed' as const;}
  if(task.status==='failed'||task.status==='canceled'){state.failureKind='provider_terminal';await persist('failed',undefined,'Pegasus could not assess this candidate. Playback and recovery remain available.');return 'completed' as const;}
  if(task.status!=='ready')return 'pending' as const;
  let assessment;
  try{assessment=assessmentSchema.parse(task.assessment);if(assessment.evidence.some(e=>e.timestampSeconds<part.startSeconds-0.1||e.timestampSeconds>part.endSeconds+0.1))throw Error('Evidence outside source window');}
  catch{state.failureKind='invalid_evidence';await persist('failed',undefined,'Analysis returned incomplete or invalid evidence. Review the video directly.');return 'completed' as const;}
  state.phase='ready';state.assessment=assessment;
  if(cp.parts.every(p=>p.phase==='ready')){await persist('ready',combineAssessments(analysis.plan.map((plan,i)=>({plan,assessment:cp.parts[i]!.assessment!}))));return 'completed' as const;}
  await persist();return 'pending' as const;
}
