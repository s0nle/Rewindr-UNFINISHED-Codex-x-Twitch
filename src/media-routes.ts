import type { FastifyInstance } from 'fastify';
import { createHash, randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir, rename, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Transform } from 'node:stream';
import { z } from 'zod';
import type { BroadcastRepository } from './persistence.js';
import type { ChunkStorage } from './chunk-storage.js';
import { MAX_CHUNK_BYTES } from './chunk-storage.js';
import { HttpError } from './http-errors.js';
import { probeChunkMedia } from './media.js';
import { playbackPlan, signedPlayback } from './playback.js';
import { replayPage } from './replay-page.js';
import type { JobQueue } from './jobs.js';
import { chunkJobFile } from './staged-media.js';

const rangeSchema = z.object({ startSeconds: z.coerce.number().finite().nonnegative(), endSeconds: z.coerce.number().finite().positive() }).strict()
  .refine(value => value.endSeconds > value.startSeconds && value.endSeconds - value.startSeconds <= 300);
export async function registerMediaRoutes(app: FastifyInstance, options: { dataDir: string; broadcasts: BroadcastRepository; storage?: ChunkStorage; jobs?:JobQueue; maxBytes?: number }) {
  let uploading = false;
  const maxBytes = Math.min(options.maxBytes ?? MAX_CHUNK_BYTES, MAX_CHUNK_BYTES);
  const lookup = async (id: string) => { const value = await options.broadcasts.get(id); if (!value) throw new HttpError(404, 'Broadcast not found.'); return value; };
  const storage = () => { if (!options.storage) throw new HttpError(503, 'Supabase chunk storage is not configured.'); return options.storage; };
  app.post<{ Params: { id: string; assetId: string } }>('/broadcasts/:id/chunks/:assetId/media', async (req, reply) => {
    const broadcast = await lookup(req.params.id);
    if(broadcast.ingestionId)throw new HttpError(409,'Use the ingestion chunk endpoint for this broadcast.');
    const chunk = broadcast.chunks.find(value => value.indexedAssetId === req.params.assetId);
    if (!chunk) throw new HttpError(404, 'Chunk not found in this broadcast.');
    const target = storage();
    if (!options.broadcasts.attachStorage) throw new HttpError(503, 'The metadata store cannot attach chunk media.');
    if (req.headers['x-creator-authorized'] !== 'true') throw new HttpError(400, 'Send X-Creator-Authorized: true for authorized footage.');
    if (!req.isMultipart()) throw new HttpError(415, 'Send multipart/form-data with one MP4 file named chunk.');
    if (uploading) throw new HttpError(429, 'One chunk is already uploading. Retry when it finishes.');
    uploading = true;
    const directory = join(options.dataDir, 'chunk-uploads');
    const temporary = join(directory, `${randomUUID()}.mp4`);
    let staged:string|undefined;
    let enqueueAttempted=false;
    try {
      await mkdir(directory, { recursive: true });
      let bytes = 0; let received = false; const hash = createHash('sha256');
      for await (const part of req.parts({ limits: { files: 1, fields: 0, parts: 1, fileSize: maxBytes } })) {
        if (part.type !== 'file' || part.fieldname !== 'chunk' || part.filename !== chunk.filename) throw new HttpError(400, 'Upload the registered filename in the chunk file field.');
        received = true;
        await pipeline(part.file, new Transform({ transform(buffer: Buffer, _encoding, done) { bytes += buffer.length; hash.update(buffer); done(null, buffer); } }),
          createWriteStream(temporary, { flags: 'wx', mode: 0o600 }));
        if (part.file.truncated) throw new HttpError(413, 'Chunk exceeds the upload limit.');
      }
      if (!received || !bytes) throw new HttpError(400, 'A nonempty chunk file is required.');
      const sha256 = hash.digest('hex');
      if (chunk.storage && chunk.storage.sha256 !== sha256) throw new HttpError(409, 'Different content is already attached to this chunk.');
      try { await probeChunkMedia(temporary, chunk.endSeconds - chunk.startSeconds); }
      catch { throw new HttpError(415, 'Chunk must be a browser-playable H.264/AAC MP4 matching the registered duration.'); }
      if(options.jobs) {
        const fileId=randomUUID();staged=chunkJobFile(options.dataDir,fileId);
        await mkdir(join(options.dataDir,'chunk-jobs'),{recursive:true});
        await rename(temporary,staged);
        enqueueAttempted=true;
        const job=await options.jobs.enqueue('chunk_upload',broadcast.id,{fileId,indexedAssetId:chunk.indexedAssetId,filename:chunk.filename,sha256,bytes});
        if(job.input.fileId!==fileId){await rm(staged,{force:true});staged=undefined;}
        reply.code(job.state==='completed'?200:202).header('location',`/jobs/${job.id}`);
        return {broadcastId:broadcast.id,indexedAssetId:chunk.indexedAssetId,jobId:job.id,jobStatusUrl:`/jobs/${job.id}`,
          state:job.state,stored:job.state==='completed',reused:job.state==='completed',bytes,sha256};
      }
      const uploaded = await target.upload(broadcast.id, chunk.indexedAssetId, temporary, sha256, bytes);
      try { await options.broadcasts.attachStorage(broadcast.id, chunk.indexedAssetId, uploaded.reference); }
      catch (error) {
        if (error instanceof HttpError && error.statusCode === 409) throw error;
        // Never delete an object after an uncertain DB commit; retry the same hash.
        throw new HttpError(503, 'Media uploaded but database confirmation failed. Retry the same chunk; local source files are retained.');
      }
      reply.code(uploaded.reused ? 200 : 201);
      return { broadcastId: broadcast.id, indexedAssetId: chunk.indexedAssetId,
        stored: true, reused: uploaded.reused, bytes, sha256 };
    } finally {
      uploading = false; await rm(temporary, { force: true });
      // An uncertain enqueue may have committed: keep its durable staged copy.
      if(staged&&!enqueueAttempted)await rm(staged,{force:true});
    }
  });
  app.get<{ Params: { id: string } }>('/broadcasts/:id/playback', async (req, reply) => {
    const broadcast = await lookup(req.params.id); const target = storage();
    const range = rangeSchema.safeParse(req.query);
    if (!range.success) throw new HttpError(400, 'Provide a valid startSeconds/endSeconds range of at most five minutes.');
    reply.header('cache-control', 'no-store').header('referrer-policy', 'no-referrer');
    return signedPlayback(broadcast, range.data.startSeconds, range.data.endSeconds, target);
  });
  app.get<{ Params: { id: string } }>('/broadcasts/:id/replay', async (req, reply) => {
    const broadcast = await lookup(req.params.id); storage();
    const range = rangeSchema.safeParse(req.query);
    if (!range.success) throw new HttpError(400, 'Provide a valid startSeconds/endSeconds range of at most five minutes.');
    if (!playbackPlan(broadcast, range.data.startSeconds, range.data.endSeconds)) throw new HttpError(409, 'Requested footage is not completely stored.');
    return reply.header('cache-control', 'no-store').header('referrer-policy', 'no-referrer').type('text/html').send(replayPage);
  });
}
