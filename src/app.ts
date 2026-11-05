import Fastify from 'fastify';
import multipart from '@fastify/multipart';
import fastifyStatic from '@fastify/static';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { rename, rm, stat, writeFile } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Transform } from 'node:stream';
import { z } from 'zod';
import { createLocalPersistence } from './local-persistence.js';
import type { Persistence } from './persistence.js';
import { Processor, type MetadataRetryOptions } from './processor.js';
import type { Recording, VideoSearchProvider } from './types.js';
import { ClipExtractor } from './clips.js';
import { buildEncounters } from './search.js';
import { registerBroadcastRoutes } from './broadcasts.js';
import { searchSettingsSchema } from './chunk-search.js';
import type { SearchSettings } from './types.js';
import { HttpError } from './http-errors.js';
import type { ChunkStorage } from './chunk-storage.js';
import { registerMediaRoutes } from './media-routes.js';
import {visibleJob,type JobQueue} from './jobs.js';
import { registerJobRoutes } from './job-routes.js';
import type {IngestionRepository} from './ingestions.js';
import {registerIngestionRoutes} from './ingestion-routes.js';
import type {SearchRepository} from './search-store.js';
import {SearchService} from './search-service.js';
import {registerSearchRoutes} from './search-routes.js';
import {registerTwitchLocalRoutes} from './twitch-local-routes.js';
import type {TwitchRepository} from './twitch-store.js';
import type {TwitchApi} from './twitch-api.js';
import {registerMediaLifecycleRoutes,type MediaScope} from './media-lifecycle-routes.js';

export const MAX_UPLOAD_BYTES = 4_000_000_000; // Conservative current Marengo 3.0 limit, even with multipart.
const searchSchema = searchSettingsSchema.extend({ question: z.string().trim().min(1).max(1000), limit: z.number().int().min(3).max(50).default(30) }).strict();
export async function buildApp(options: { dataDir: string; persistence?: Persistence; storage?: ChunkStorage; jobs?: JobQueue; ingestions?:IngestionRepository;searches?:SearchRepository;media?:Omit<MediaScope,'persistence'|'channel'>;analysisConfigured?:boolean;analysisModel?:'pegasus1.5'|'pegasus1.6';twitch?:{ownerId:string;repository?:TwitchRepository;api?:TwitchApi}; maxChunkUploadBytes?: number; provider?: VideoSearchProvider; intervalMs?: number; processorRetry?: MetadataRetryOptions; maxUploadBytes?: number; logger?: boolean }) {
  const app = Fastify({ logger: options.logger ? {serializers:{req:(req:{method:string;url:string})=>({method:req.method,url:req.url.split('?')[0]})}} : false, bodyLimit: 16 * 1024, requestTimeout: 3600_000, trustProxy: false });
  const persistence = options.persistence ?? createLocalPersistence(options.dataDir);
  await persistence.init();
  await options.storage?.init();
  const store = persistence.recordings;
  const clips = new ClipExtractor(options.dataDir);
  if(options.jobs)await options.jobs.status();
  const processor = options.provider && !options.jobs ? new Processor(store, options.provider, options.intervalMs, options.processorRetry) : undefined;
  const maxBytes = Math.min(MAX_UPLOAD_BYTES, options.maxUploadBytes ?? MAX_UPLOAD_BYTES);
  await app.register(multipart, { limits: { files: 1, fields: 0, parts: 1, fileSize: maxBytes, fieldNameSize: 64, headerPairs: 100 } });

  app.addHook('onRequest', async (req, reply) => {
    // Binding alone does not protect a local service from cross-site requests or DNS rebinding.
    const host = req.headers.host ?? '';
    if (!/^(localhost|127\.0\.0\.1)(:\d+)?$/.test(host)) throw new HttpError(403, 'Stage 1 accepts localhost hosts only.');
    const origin = req.headers.origin;
    const oauthNavigation=req.method==='GET'&&['/twitch/connect','/twitch/callback'].includes(req.url.split('?')[0]!)&&!origin&&
      (!req.headers['sec-fetch-mode']||req.headers['sec-fetch-mode']==='navigate')&&(!req.headers['sec-fetch-dest']||req.headers['sec-fetch-dest']==='document');
    if ((origin && origin !== `http://${host}`) || (req.headers['sec-fetch-site'] === 'cross-site'&&!oauthNavigation)) throw new HttpError(403, 'Cross-origin requests are disabled for this local prototype.');
    reply.header('cache-control', 'no-store').header('x-content-type-options', 'nosniff').header('x-frame-options','DENY');
  });
  app.setErrorHandler((error, _req, reply) => {
    const err = error as Error & { statusCode?: number; code?: string };
    const status = Number.isInteger(err.statusCode) && err.statusCode! >= 400 && err.statusCode! <= 599 ? err.statusCode! : 500;
    reply.code(status).send({ error: status < 500 || error instanceof HttpError ? err.message : 'Internal operation failed. Check local configuration and available disk space.' });
  });
  app.get('/health', async (_req, reply) => {
    await persistence.health();
    const queueStatus=await options.jobs?.status();
    const processing = queueStatus ? {state:'external-worker',...queueStatus} : processor?.status() ?? { state: 'disabled' };
    const degraded=processing.state==='halted'||Boolean(queueStatus&&queueStatus.pending>0&&!queueStatus.workerOnline);
    if (degraded) reply.code(503);
    return { status: degraded ? 'degraded' : 'ok', processing, chunkStorageConfigured: Boolean(options.storage),
      stage:options.searches&&options.ingestions?8:options.searches?7:options.ingestions ? 5 : options.jobs ? 4 : options.storage ? 3 : persistence.kind === 'supabase' ? 2 : 1, localOnly: true, providerConfigured: Boolean(options.provider),
      features:{liveIngestionConfigured:Boolean(options.ingestions),searchSessions:Boolean(options.searches),candidateAnalysisConfigured:Boolean(options.analysisConfigured),queryRecovery:'constrained_rules'},
      persistence: persistence.kind === 'supabase' ? `Supabase PostgreSQL metadata; ${options.storage ? 'private Storage chunks and local recording media' : 'local media'}; single process` : 'temporary local JSON and MP4; single process' };
  });
  await registerBroadcastRoutes(app, { store: persistence.broadcasts, persistence: persistence.kind, ingestions:options.ingestions,storageEnabled: Boolean(options.storage), provider: options.provider, recording: id => store.get(id) });
  await registerMediaRoutes(app, { dataDir: options.dataDir, broadcasts: persistence.broadcasts, storage: options.storage, jobs:options.jobs, maxBytes: options.maxChunkUploadBytes });
  await registerJobRoutes(app,options.jobs);
  await registerIngestionRoutes(app,{dataDir:options.dataDir,ingestions:options.ingestions,provider:options.provider,maxBytes:options.maxChunkUploadBytes});
  const searches=options.searches?new SearchService(options.searches,options.provider,{storageEnabled:Boolean(options.storage),analysisConfigured:Boolean(options.analysisConfigured),model:options.analysisModel??'pegasus1.5'}):undefined;
  await registerSearchRoutes(app,{broadcasts:persistence.broadcasts,service:searches});
  await registerTwitchLocalRoutes(app,options.twitch??{ownerId:'00000000-0000-4000-8000-000000000001'});
  if(options.media)await registerMediaLifecycleRoutes(app,async()=>({...options.media!,persistence,channel:(await options.twitch?.repository?.byOwner(options.twitch.ownerId))?.twitch_user_id}));

  app.post('/recordings', async (req, reply) => {
    if (!options.provider) return reply.code(503).send({ error: 'Set TWELVELABS_API_KEY before uploading. No mock retrieval is enabled.' });
    if (req.headers['x-creator-authorized'] !== 'true') throw new HttpError(400, 'Send X-Creator-Authorized: true to attest that the creator authorized indexing and provider upload.');
    if (!req.isMultipart()) throw new HttpError(415, 'Send multipart/form-data with one file named recording.');
    const id = randomUUID();
    const path = store.mediaPath(id);
    let filename = '';
    let bytes = 0;
    const hash = createHash('sha256');
    let committed = false;
    let metadataAttempted = false;
    try {
      for await (const part of req.parts()) {
        if (part.type !== 'file' || part.fieldname !== 'recording') throw new HttpError(400, 'Expected exactly one recording file.');
        if (!part.filename.toLowerCase().endsWith('.mp4')) throw new HttpError(415, 'Stage 1 accepts MP4 recordings. See README for conversion.');
        filename = part.filename.split(/[\\/]/).pop()!.replace(/[\x00-\x1f]/g, '').slice(0, 200);
        await pipeline(part.file, new Transform({ transform(chunk: Buffer, _encoding, done) { bytes += chunk.length; hash.update(chunk); done(null, chunk); } }), createWriteStream(`${path}.partial`, { flags: 'wx', mode: 0o600 }));
        if (part.file.truncated) throw new HttpError(413, `Recording exceeds ${maxBytes} bytes.`);
      }
      if (!filename || !bytes) throw new HttpError(400, 'A nonempty recording file is required.');
      await rename(`${path}.partial`, path);
      const now = new Date().toISOString();
      const recording: Recording = { id, filename, bytes, sha256: hash.digest('hex'), creatorAuthorized: true, authorizedAt: now, createdAt: now, updatedAt: now, phase: 'queued' };
      metadataAttempted = true; await store.put(recording); committed = true;
      processor?.kick();
      return reply.code(202).header('location', `/recordings/${id}/status`).send({ recordingId: id, phase: 'queued', statusUrl: `/recordings/${id}/status`,
        ...(options.jobs?{jobId:id,jobStatusUrl:`/jobs/${id}`}:{}) });
    } finally {
      if (!committed) { await rm(`${path}.partial`, { force: true }); if (!metadataAttempted) await rm(path, { force: true }); }
    }
  });
  const lookup = async (id: string) => { const r = await store.get(id); if (!r) throw new HttpError(404, 'Recording not found.'); return r; };
  app.get<{ Params: { id: string } }>('/recordings/:id/status', async req => {
    const r = await lookup(req.params.id);
    const { upload, ...visible } = r;
    const job=await options.jobs?.get(r.id);
    return { ...visible, uploadProgress: upload && { completedChunks: upload.completedChunks, totalChunks: upload.totalChunks }, sourceUrl: `/recordings/${r.id}/source`,
      ...(options.jobs?{jobStatusUrl:`/jobs/${r.id}`,processingJob:job?visibleJob(job):null}:{}) };
  });
  async function searchRecording(r: Recording, question: string, limit: number, referenceImagePath?: string, settings: SearchSettings = searchSettingsSchema.parse({})) {
    if (r.phase !== 'ready') throw new HttpError(409,'Recording is not ready for search.');
    if (!options.provider) throw new HttpError(503,'TwelveLabs credentials are missing.');
    try {
      const moments = await options.provider.search(r.indexId!, r.indexedAssetId!, question, limit, referenceImagePath, settings);
      if (moments.some(m => m.videoId !== r.indexedAssetId || m.startSeconds < 0 || m.endSeconds <= m.startSeconds || m.endSeconds > r.media!.durationSeconds + 1)) throw new Error('Invalid source timestamps');
      const sourceUrl=`/recordings/${r.id}/source`;
      return { recordingId: r.id, sourceSha256: r.sha256, question, provider: 'twelvelabs', model: 'marengo3.0', ...settings, queryMode:referenceImagePath?(question.trim()?'text-and-image':'image'):'text', evidence: 'live-provider', uncertainty: 'Ranked candidates require visual review. Context is added for playback, not a verified interaction boundary. Results do not prove an event occurred or was absent.', moments: moments.map(m => ({ ...m, recordingId: r.id, indexId: r.indexId, assetId: r.assetId, indexedAssetId: r.indexedAssetId, sourceUrl, playbackUrl: `${sourceUrl}#t=${m.startSeconds}` })), encounters:buildEncounters(moments,r.media!.durationSeconds).map(encounter=>({...encounter,sourceUrl,playbackUrl:`${sourceUrl}#t=${encounter.startSeconds}`})) };
    } catch { throw new HttpError(502,'TwelveLabs search failed or returned invalid source data. Check the query, reference image, credentials, and provider status.'); }
  }
  app.post<{ Params: { id: string } }>('/recordings/:id/search', async req => {
    const parsed = searchSchema.safeParse(req.body);
    if (!parsed.success) throw new HttpError(400, 'Provide question (1–1000 characters) and optional limit (3–50). The provider also enforces 500 tokens.');
    const { question, limit, ...settings } = parsed.data;
    return searchRecording(await lookup(req.params.id),question,limit,undefined,settings);
  });
  app.post<{ Params: { id: string } }>('/recordings/:id/search/reference', async req => {
    const r=await lookup(req.params.id);
    if(r.phase!=='ready')throw new HttpError(409,'Recording is not ready for search.');
    if(!options.provider)throw new HttpError(503,'TwelveLabs credentials are missing.');
    if(!req.isMultipart())throw new HttpError(415,'Send multipart/form-data with a PNG or JPEG file named reference.');
    let question='';
    let referencePath: string|undefined;
    try{
      for await(const part of req.parts({limits:{files:1,fields:1,parts:2,fileSize:5*1024*1024,fieldSize:4000}})){
        if(part.type==='field'){
          if(part.fieldname!=='question')throw new HttpError(400,'Only the question field is supported.');
          const parsed=z.string().trim().max(1000).safeParse(part.value);
          if(!parsed.success)throw new HttpError(400,'Question must contain at most 1000 characters.');
          question=parsed.data;
        }else{
          if(part.fieldname!=='reference')throw new HttpError(400,'Upload one PNG or JPEG named reference.');
          // Reference images are bounded to 5 MB; full video uploads remain streamed.
          const bytes=await part.toBuffer();
          const png=bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]));
          const jpeg=bytes[0]===255&&bytes[1]===216&&bytes[2]===255;
          if(!png&&!jpeg)throw new HttpError(415,'Reference must be a PNG or JPEG image.');
          referencePath=resolve(store.root,`${randomUUID()}.reference.${png?'png':'jpg'}`);
          await writeFile(referencePath,bytes,{mode:0o600,flag:'wx'});
        }
      }
      if(!referencePath)throw new HttpError(400,'A PNG or JPEG reference is required.');
      return await searchRecording(r,question,30,referencePath);
    }finally{if(referencePath)await rm(referencePath,{force:true});}
  });
  app.get<{ Params: { id: string } }>('/recordings/:id/source', async (req, reply) => {
    const r = await lookup(req.params.id);
    if (!r.media) throw new HttpError(409, 'Source has not passed media validation.');
    const path = store.mediaPath(r.id);
    const { size } = await stat(path);
    reply.type('video/mp4').header('accept-ranges', 'bytes');
    const range = req.headers.range;
    if (!range) return reply.header('content-length', size).send(createReadStream(path));
    const invalid = () => reply.code(416).header('content-range', `bytes */${size}`).send();
    const match = /^bytes=(\d*)-(\d*)$/.exec(range);
    if (!match || (!match[1] && !match[2])) return invalid();
    let start = match[1] ? Number(match[1]) : Math.max(0, size - Number(match[2]));
    let end = match[1] ? (match[2] ? Math.min(Number(match[2]), size - 1) : size - 1) : size - 1;
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start >= size || end < start || (!match[1] && Number(match[2]) === 0)) return invalid();
    return reply.code(206).header('content-range', `bytes ${start}-${end}/${size}`).header('content-length', end - start + 1).send(createReadStream(path, { start, end }));
  });
  app.post<{ Params: { id: string } }>('/recordings/:id/clips', async (req, reply) => {
    const r = await lookup(req.params.id);
    if (r.phase !== 'ready' || !r.media) throw new HttpError(409, 'Wait for the recording to be ready.');
    const parsed = z.object({ startSeconds: z.number().finite().nonnegative(), endSeconds: z.number().finite().positive() }).strict().safeParse(req.body);
    if (!parsed.success) throw new HttpError(400, 'Provide numeric startSeconds and endSeconds.');
    const { startSeconds, endSeconds } = parsed.data;
    if (endSeconds <= startSeconds || endSeconds > r.media.durationSeconds || endSeconds-startSeconds > 300) throw new HttpError(400, 'Choose a range inside the recording, up to five minutes long.');
    try {
      const clipId = await clips.extract(r.id, store.mediaPath(r.id), startSeconds, endSeconds);
      return reply.code(201).send({ clipId, recordingId:r.id, startSeconds, endSeconds, downloadUrl:`/recordings/${r.id}/clips/${clipId}/file` });
    } catch (error) {
      if (error instanceof Error && error.message === 'BUSY') return reply.code(429).send({error:'One clip is already being extracted. Wait for it to finish.'});
      return reply.code(500).send({error:'Clip extraction failed. Check FFmpeg and available disk space, or choose a shorter range.'});
    }
  });
  app.get<{ Params: { id: string; clipId: string } }>('/recordings/:id/clips/:clipId/file', async (req, reply) => {
    const r=await lookup(req.params.id);
    if (!/^[a-f0-9]{64}$/.test(req.params.clipId)) throw new HttpError(404,'Clip not found.');
    const path=clips.path(r.id,req.params.clipId);
    let size: number;
    try { size=(await stat(path)).size; } catch { throw new HttpError(404,'Clip not found.'); }
    return reply.type('video/mp4').header('content-length',size).header('content-disposition',`attachment; filename="rewindr-${r.id.slice(0,8)}-${req.params.clipId.slice(0,8)}.mp4"`).send(createReadStream(path));
  });
  const webRoot=resolve('dist/web');
  if (existsSync(webRoot)) await app.register(fastifyStatic,{root:webRoot});
  app.addHook('onClose', async () => { await processor?.stop(); });
  await processor?.start();
  return app;
}
