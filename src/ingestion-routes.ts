import type {FastifyInstance,FastifyRequest} from 'fastify';
import {randomUUID,createHash} from 'node:crypto';
import {createWriteStream} from 'node:fs';
import {mkdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {Transform} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import type {IngestionRepository} from './ingestions.js';
import {ingestionCreationSchema,planChunkIntervals} from './ingestion-types.js';
import {chunkJobFile} from './staged-media.js';
import {probeChunkMedia} from './media.js';
import {HttpError} from './http-errors.js';
import type {VideoSearchProvider} from './types.js';

export async function registerIngestionRoutes(app:FastifyInstance,options:{dataDir:string;ingestions?:IngestionRepository|((req:FastifyRequest)=>Promise<IngestionRepository>);provider?:VideoSearchProvider;maxBytes?:number;allowedIndex?:string}) {
  const repository=async(req:FastifyRequest)=>{if(!options.ingestions)throw new HttpError(503,'Ingestion requires Supabase and the ingestion migrations.');return typeof options.ingestions==='function'?options.ingestions(req):options.ingestions;};
  const authorize=(headers:Record<string,unknown>)=>{if(headers['x-creator-authorized']!=='true')throw new HttpError(400,'Send X-Creator-Authorized: true for authorized footage.');};
  const lookup=async(req:FastifyRequest,id:string)=>{const value=await(await repository(req)).get(id);if(!value)throw new HttpError(404,'Ingestion not found.');return value;};
  app.post('/ingestions',async(req,reply)=>{
    authorize(req.headers);const parsed=ingestionCreationSchema.safeParse(req.body);if(!parsed.success)throw new HttpError(400,'Provide a valid ingestion ID, title, indexId, duration (or live mode), offset and source fingerprint.');
    if(options.allowedIndex&&parsed.data.indexId!==options.allowedIndex)throw new HttpError(403,'Use the index assigned by the hosted Rewindr service.');
    if(!options.provider?.validateIndex)throw new HttpError(503,'Configure TwelveLabs before starting an ingestion.');
    try{await options.provider.validateIndex(parsed.data.indexId);}catch{throw new HttpError(502,'Could not validate the existing Marengo 3.0 visual/audio index.');}
    const value=await(await repository(req)).create(parsed.data);
    return reply.code(201).send({ingestionId:value.id,broadcastId:value.id,statusUrl:`/ingestions/${value.id}`,searchUrl:`/broadcasts/${value.id}/search`});
  });
  app.get<{Params:{id:string}}>('/ingestions/:id',async req=>{const status=await(await repository(req)).status(req.params.id);if(!status)throw new HttpError(404,'Ingestion not found.');return status;});
  app.post<{Params:{id:string}}>('/ingestions/:id/complete',async req=>{authorize(req.headers);await lookup(req,req.params.id);await(await repository(req)).seal(req.params.id);return(await repository(req)).status(req.params.id);});
  let uploading=false;
  app.post<{Params:{id:string;ordinal:string}}>('/ingestions/:id/chunks/:ordinal',async(req,reply)=>{
    authorize(req.headers);const ingestion=await lookup(req,req.params.id);
    if(!/^\d{1,4}$/.test(req.params.ordinal))throw new HttpError(400,'Invalid chunk ordinal.');
    const ordinal=Number(req.params.ordinal);let expected;
    if(ingestion.mode==='live'){
      const from=req.headers['x-chunk-start-seconds'],to=req.headers['x-chunk-end-seconds'];const startSeconds=Number(from),endSeconds=Number(to);
      if(typeof from!=='string'||typeof to!=='string'||!from.trim()||!to.trim()||ordinal>719||!Number.isFinite(startSeconds)||!Number.isFinite(endSeconds)||startSeconds<0||endSeconds-startSeconds<4||endSeconds-startSeconds>65||endSeconds>43200)throw new HttpError(400,'Live chunks need valid X-Chunk-Start-Seconds and X-Chunk-End-Seconds, lasting 4–65 seconds.');
      expected={ordinal,filename:`chunk_${String(ordinal).padStart(4,'0')}.mp4`,startSeconds,endSeconds};
    }else expected=planChunkIntervals(ingestion.source_duration_seconds)[ordinal];
    if(!expected)throw new HttpError(400,'Chunk ordinal is outside the source plan.');
    if(!req.isMultipart())throw new HttpError(415,'Send one MP4 in the chunk multipart field.');
    if(uploading)throw new HttpError(429,'One ingestion file is already staging. Retry after it finishes.');
    uploading=true;const fileId=randomUUID();const file=chunkJobFile(options.dataDir,fileId);let attempted=false;
    try {
      await mkdir(join(options.dataDir,'chunk-jobs'),{recursive:true});let bytes=0;let received=false;const hash=createHash('sha256');
      for await(const part of req.parts({limits:{files:1,fields:0,parts:1,fileSize:Math.min(options.maxBytes??50_000_000,50_000_000)}})) {
        if(part.type!=='file'||part.fieldname!=='chunk'||part.filename!==expected.filename)throw new HttpError(400,'Upload the filename from the source chunk plan.');
        received=true;await pipeline(part.file,new Transform({transform(buffer:Buffer,_enc,done){bytes+=buffer.length;hash.update(buffer);done(null,buffer);}}),createWriteStream(file,{flags:'wx',mode:0o600}));
        if(part.file.truncated)throw new HttpError(413,'Chunk exceeds the 50 MB limit.');
      }
      if(!received||!bytes)throw new HttpError(400,'A nonempty chunk file is required.');
      try{const media=await probeChunkMedia(file,expected.endSeconds-expected.startSeconds);if(media.durationSeconds<4)throw Error('Too short');}
      catch{throw new HttpError(415,'Chunk must be H.264/AAC MP4 matching the planned source interval, at least four seconds long.');}
      attempted=true;
      let chunk;
      try{chunk=await(await repository(req)).accept(ingestion.id,{fileId,...expected,sha256:hash.digest('hex'),bytes});}
      catch(error){if(error instanceof HttpError&&error.statusCode<500)attempted=false;throw error;}
      if(chunk.file_id!==fileId)await rm(file,{force:true});
      reply.code(chunk.checkpoint.phase==='ready'?200:202).header('location',`/jobs/${chunk.job_id}`);
      return {ingestionId:ingestion.id,chunkId:chunk.id,jobId:chunk.job_id,jobStatusUrl:`/jobs/${chunk.job_id}`,phase:chunk.checkpoint.phase};
    } finally {uploading=false;if(!attempted)await rm(file,{force:true});}
  });
}
