import {mkdir,rm,stat} from 'node:fs/promises';
import {join} from 'node:path';
import {setTimeout as wait} from 'node:timers/promises';
import type {Job} from './jobs.js';
import {LeaseLostError} from './jobs.js';
import type {IngestionRepository} from './ingestions.js';
import {ingestStorageKey,type IngestCheckpoint} from './ingestion-types.js';
import type {ChunkStorage} from './chunk-storage.js';
import type {VideoSearchProvider} from './types.js';
import {chunkJobFile} from './staged-media.js';
import {fileIdentity} from './segmenter.js';
import {probeChunkMedia} from './media.js';
import {HttpError} from './http-errors.js';

export async function advanceIngestChunk(options:{job:Job;repository:IngestionRepository;storage:ChunkStorage;provider:VideoSearchProvider;dataDir:string;guard:()=>Promise<void>;retryMs?:number}) {
  const {job,repository,storage,provider,guard}=options;
  const chunk=await repository.chunk(String(job.input.chunkId));
  if(!chunk||chunk.job_id!==job.id||chunk.ingestion_id!==job.broadcast_id)throw new HttpError(409,'Queued ingestion chunk mapping is missing.');
  const ingestion=await repository.get(chunk.ingestion_id);if(!ingestion)throw new HttpError(409,'Ingestion is missing.');
  const cp:IngestCheckpoint=structuredClone(chunk.checkpoint);
  const persist=async(complete=false)=>{
    for(let attempt=0;;attempt++) {
      await guard();
      try {await repository.checkpoint(job,cp,complete);return;}
      catch(error){if(!(error instanceof HttpError)||error.statusCode!==503||attempt>=4)throw error;await wait(Math.min(8000,(options.retryMs??1000)*2**attempt));}
    }
  };
  const file=chunkJobFile(options.dataDir,chunk.file_id);const key=ingestStorageKey(chunk.id);
  const ensureFile=async()=>{
    await mkdir(join(options.dataDir,'chunk-jobs'),{recursive:true});
    let missing=false;try{await stat(file);}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')missing=true;else throw error;}
    if(missing){if(!cp.storage||!storage.download)throw new HttpError(422,'Staged source is missing and has no recoverable Storage copy.');await guard();await storage.download(ingestion.id,key,cp.storage,file);}
    const identity=await fileIdentity(file);
    if(identity.bytes!==chunk.bytes||identity.sha256!==chunk.sha256)throw new HttpError(422,'Staged chunk failed its content check.');
    try{const media=await probeChunkMedia(file,chunk.end_seconds-chunk.start_seconds);if(media.durationSeconds<4)throw Error('Too short');}
    catch{throw new HttpError(415,'Staged chunk media does not match its source interval.');}
  };
  if(cp.phase==='creating_upload'||cp.phase==='submitting_index') {
    cp.error='Provider submission has an uncertain outcome. Inspect TwelveLabs and reconcile its IDs before retrying.';await persist();return 'needs_review' as const;
  }
  if(Date.now()-Date.parse(job.created_at)>4*3600_000){cp.phase='failed';cp.error='Chunk processing exceeded four hours; inspect retained provider resources.';await persist();return 'failed' as const;}
  if(cp.phase==='failed') {
    cp.phase=cp.indexedAssetId?'indexing':cp.upload?(cp.upload.completedChunks<cp.upload.totalChunks?'uploading':'asset_processing'):cp.assetId?'asset_processing':cp.storage?'stored':'queued';
    delete cp.error;await persist();
  }
  if(cp.phase==='queued') {
    await ensureFile();await guard();
    const uploaded=await storage.upload(ingestion.id,key,file,chunk.sha256,chunk.bytes);
    cp.storage=uploaded.reference;cp.phase='stored';await persist();
  }
  if(cp.phase==='stored') {
    await ensureFile();cp.phase='creating_upload';await persist();
    try {cp.upload=await provider.createUpload(file,`${ingestion.id}_${chunk.filename}`,chunk.bytes);cp.assetId=cp.upload.assetId;}
    catch(error){if(error instanceof LeaseLostError)throw error;cp.error='TwelveLabs upload creation was not confirmed. Review its resources before retrying.';await persist();return 'needs_review' as const;}
    cp.phase='uploading';await persist();
  }
  if(cp.phase==='uploading') {
    if(!cp.upload||!cp.assetId)throw new HttpError(409,'Provider upload checkpoint is missing.');
    await ensureFile();await provider.transfer(file,chunk.bytes,cp.upload,async completed=>{cp.upload!.completedChunks=completed;await persist();});
    cp.phase='asset_processing';await persist();
    // The private source and upload checkpoint are durable before local deletion.
    try{await rm(file,{force:true});}catch{console.warn('Ingestion retained a staged copy for cleanup.',{jobId:job.id});}
    return 'pending' as const;
  }
  if(cp.phase==='asset_processing') {
    cp.providerStatus=await provider.assetStatus(cp.assetId!);
    if(cp.providerStatus==='failed'){cp.phase='failed';cp.error='TwelveLabs asset processing failed. Inspect the retained asset.';await persist();return 'failed' as const;}
    if(cp.providerStatus!=='ready'){await persist();return 'pending' as const;}
    cp.phase='submitting_index';await persist();
    try{cp.indexedAssetId=await provider.indexAsset(ingestion.index_id,cp.assetId!);}
    catch(error){if(error instanceof LeaseLostError)throw error;cp.error='TwelveLabs indexing submission was not confirmed. Review its resources before retrying.';await persist();return 'needs_review' as const;}
    cp.phase='indexing';await persist();return 'pending' as const;
  }
  if(cp.phase==='indexing') {
    cp.providerStatus=await provider.indexStatus(ingestion.index_id,cp.indexedAssetId!);
    if(cp.providerStatus==='ready'){cp.phase='ready';await persist(true);return 'completed' as const;}
    if(cp.providerStatus==='failed'){cp.phase='failed';cp.error='TwelveLabs indexing failed. Inspect the retained indexed asset.';await persist();return 'failed' as const;}
    await persist();return 'pending' as const;
  }
  throw new HttpError(409,'Ingestion checkpoint is not resumable.');
}
