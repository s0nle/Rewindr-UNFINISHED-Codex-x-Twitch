import { configuredSupabaseClient } from '../src/supabase-persistence.js';
import { configuredChunkStorage, MAX_CHUNK_BYTES } from '../src/chunk-storage.js';

async function run() {
  const client = configuredSupabaseClient(); const storage = configuredChunkStorage();
  const existing = await client.storage.getBucket(storage.bucket);
  if (!existing.data) {
    const status = Number((existing.error as { status?: number; statusCode?: string })?.statusCode ?? (existing.error as { status?: number })?.status);
    if (status !== 404 && status !== 400) throw new Error('Cannot read storage buckets.');
    const created = await client.storage.createBucket(storage.bucket, { public: false, fileSizeLimit: MAX_CHUNK_BYTES, allowedMimeTypes: ['video/mp4'] });
    if (created.error) throw new Error('Cannot create private storage bucket.');
  }
  await storage.init();
  console.log('Private chunk bucket is ready. Existing bucket permissions were not changed.');
}
run().catch(() => { console.error('Storage setup failed. Check backend credentials and bucket name; an existing public bucket will not be reused.'); process.exitCode = 1; });
