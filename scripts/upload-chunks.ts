import { openAsBlob } from 'node:fs';
import { realpath, stat } from 'node:fs/promises';
import { resolve, sep } from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import { z } from 'zod';
import { MAX_CHUNK_BYTES } from '../src/chunk-storage.js';
import { broadcastSchema } from '../src/metadata.js';
class UploadInputError extends Error {}

async function run() {
  const args = process.argv.slice(2); const values = new Map<string, string>();
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i]!, value = args[i + 1];
    if (!['--broadcast-id', '--directory', '--limit'].includes(key) || value === undefined || values.has(key)) throw new UploadInputError('Invalid upload arguments.');
    values.set(key, value);
  }
  const parsedId = z.uuid().safeParse(values.get('--broadcast-id'));
  if (!parsedId.success) throw new UploadInputError('Provide --broadcast-id with the registered broadcast UUID.');
  const id = parsedId.data;
  const directory = await realpath(resolve(values.get('--directory') || 'chunks'));
  const limit = Number(values.get('--limit') || 2000); const port = Number(process.env.PORT || 3000);
  if (!Number.isInteger(limit) || limit < 1 || limit > 2000 || !Number.isInteger(port) || port < 1 || port > 65535) throw new UploadInputError('Invalid upload limit or port.');
  const base = `http://127.0.0.1:${port}`;
  const status = await fetch(`${base}/broadcasts/${id}/status`, { signal: AbortSignal.timeout(20000) });
  if (!status.ok) throw new UploadInputError('Could not read the registered broadcast. Start the backend and check its ID.');
  const broadcast = broadcastSchema.parse(await status.json());
  let count = 0;
  for (const chunk of broadcast.chunks.slice(0, limit)) {
    const file = await realpath(resolve(directory, chunk.filename));
    if (!file.startsWith(directory + sep)) throw new UploadInputError('Chunk path leaves the selected directory.');
    const info = await stat(file);
    if (!info.isFile() || info.size > MAX_CHUNK_BYTES || !info.size) throw new UploadInputError('A chunk is missing, empty, or exceeds 50 MB.');
    for (let attempt = 0; ; attempt++) {
      const form = new FormData(); form.set('chunk', await openAsBlob(file, { type: 'video/mp4' }), chunk.filename);
      const response = await fetch(`${base}/broadcasts/${id}/chunks/${encodeURIComponent(chunk.indexedAssetId)}/media`,
        { method: 'POST', headers: { 'x-creator-authorized': 'true' }, body: form, signal: AbortSignal.timeout(180000) });
      const body = await response.json() as { error?: string; reused?: boolean;jobId?:string };
      if(response.status===202) {
        if(!body.jobId||!z.uuid().safeParse(body.jobId).success)throw new UploadInputError('Backend returned an invalid upload job.');
        console.log(`${chunk.filename}: queued; waiting for the background worker.`);
        const deadline=Date.now()+10*60*1000;
        for(;;) {
          if(Date.now()>deadline)throw new UploadInputError(`Job ${body.jobId} is still pending. Check /jobs/${body.jobId} and start the worker; repeating this upload is safe.`);
          await wait(1000);
          const polled=await fetch(`${base}/jobs/${body.jobId}`,{signal:AbortSignal.timeout(20000)});
          if(polled.status===503)continue;
          if(!polled.ok)throw new UploadInputError(`Could not read upload job ${body.jobId}.`);
          const job=await polled.json() as {state:string};
          if(job.state==='completed')break;
          if(job.state==='failed'||job.state==='needs_review')throw new UploadInputError(`Job ${body.jobId} stopped (${job.state}). Check its status and retry endpoint.`);
        }
      }
      if (response.ok) { count++; console.log(`${chunk.filename}: ${body.reused ? 'already stored' : 'uploaded'} (${count})`); break; }
      if (![429, 503].includes(response.status) || attempt >= 2) throw new UploadInputError(`${chunk.filename}: upload failed (${response.status}). See the backend response and check storage setup.`);
      await wait(1000 * 2 ** attempt);
    }
  }
  console.log(`Finished ${count} chunk(s). Original local files were retained. No TwelveLabs indexing was requested.`);
}
run().catch(error => { console.error(error instanceof UploadInputError ? error.message : 'Chunk upload stopped. Check the selected directory, media format, backend connection and logs.'); console.error('Local files were retained; repeating the command is safe.'); process.exitCode = 1; });
