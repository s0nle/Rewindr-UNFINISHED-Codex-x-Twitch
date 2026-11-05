# Order 3: private chunk storage and replay

This stage stores the exact straight-cut MP4 files already indexed. It does not
create chunks, call TwelveLabs indexing, capture live footage, or connect the main
React website to broadcast APIs. It adds a small replay page for backend testing
before the Twitch interface is built.

Order 4 runs production Supabase uploads through a durable queue and separate
worker. Start both processes as described in [the worker guide](queue-worker.md).
The Storage layer and signed playback contracts below remain in use.
Order 5 also stores newly generated recorded chunks before indexing them; see
[automatic recording ingestion](ingestion.md). Its stable chunk UUID supplies the
Storage identity until the provider ID is available.

## Implemented pipeline

```text
Existing FFmpeg chunks + measured CSV manifest
  -> register against the existing ready TwelveLabs index
  -> broadcast and chunk metadata in PostgreSQL
  -> sequential local upload CLI
  -> validate filename, duration, codec, byte limit and SHA-256
  -> durable staged file + queue job
  -> worker uploads private Supabase Storage object
  -> atomically attach its reference to the chunk row

Question -> TwelveLabs search -> up to three encounters
  -> replay page / playback manifest -> expiring signed MP4 URLs
  -> browser plays relevant portions of one or more chunks
```

Full recording sources and extracted clips still remain local. Registered chunks
and order 5 ingestion chunks use the private bucket. Local originals are retained.
Workers remove their own staged copies after the relevant durable checkpoints;
producer workspaces remain available for resume.

## Setup

The existing Supabase URL, backend secret and verified database connection are
reused. No additional provider key is required. Optional backend settings:

```dotenv
SUPABASE_STORAGE_BUCKET=rewindr-chunks
SIGNED_URL_TTL_SECONDS=300
```

These are the defaults; URL lifetime must be 60..3600 integer seconds. Never put
privileged credentials in frontend VITE variables. From the project directory:

```powershell
npm.cmd run db:migrate
npm.cmd run storage:init
npm.cmd run storage:verify
npm.cmd run build
npm.cmd start
```

Start `npm.cmd run worker:start` in a second terminal with the same DATA_DIR.

The new `202610300002_chunk_storage.sql` migration follows the original metadata
migration. Never edit applied migrations. Bucket setup uses the Supabase Storage
API to create a dedicated **private** bucket, a 50,000,000-byte limit and allowed
type `video/mp4`. It rejects an existing public bucket instead of changing access.
Existing private bucket limits are left unchanged; the application independently
enforces its own 50 MB limit.

`storage:verify` uses an isolated temporary owner and two tiny synthetic four-second
MP4s. It tests actual uploads, reuse, restart reads, signed replay, byte ranges and
public-access denial. Video search is mocked; no TwelveLabs indexing is requested.
Only that temporary owner's objects and metadata are removed. It never empties
the bucket or uploads your archive.

## Upload prepared footage

Start the backend. Register the manifest if it has not been registered already:

```powershell
npm.cmd run register:chunks -- --manifest chunks/manifest.csv --index-id YOUR_INDEX_ID --title "Rewindr test"
```

Use the returned broadcast ID. With API and worker running, start with one file,
then repeat without the limit:

```powershell
npm.cmd run upload:chunks -- --broadcast-id YOUR_BROADCAST_ID --directory chunks --limit 1
npm.cmd run upload:chunks -- --broadcast-id YOUR_BROADCAST_ID --directory chunks
```

Use the original straight-cut files and matching manifest, not the overlap
experiment. Uploads are sequential, with one active chunk upload per API process.
The API returns a queued job; the CLI waits for its completion. File-backed Blobs
stream media without loading entire videos into JavaScript
Buffers. ffprobe checks H.264/yuv420p MP4, optional AAC audio, valid resolution,
duration at most 66 seconds and within one second of the registered interval.
Registered intervals themselves remain limited to 65 seconds.

Filename/duration checks cannot prove that local content matches a TwelveLabs
asset. Supplying the exact indexed files remains a requirement. Once attached,
the stored content hash detects later replacements.

## Search and replay

```powershell
$id = 'YOUR_BROADCAST_ID'
$result = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:3000/broadcasts/$id/search" -ContentType 'application/json' -Body '{"question":"Guy in a hoodie asks for a photo"}'
$result.encounters | Select-Object rank,matchStartSeconds,matchEndSeconds,replayAvailable,playbackUrl
# Open only a candidate whose replayAvailable is true:
Start-Process ("http://127.0.0.1:3000" + $result.encounters[0].playbackUrl)
```

Stored candidates report `replayMode: "stored-chunks"`, `playbackFormat: "html"`
and a `playbackUrl` for the preview page. This is **a page**, not an MP4 for a
`<video src>`. `manifestUrl` supplies JSON segments for the future React player.
Existing full-recording fallback candidates retain their local source links.

To inspect a range using recording-relative seconds:

```powershell
Invoke-RestMethod "http://127.0.0.1:3000/broadcasts/$id/playback?startSeconds=59&endSeconds=63"
Start-Process "http://127.0.0.1:3000/broadcasts/$id/replay?startSeconds=59&endSeconds=63"
```

Ranges must lie inside stored footage and span at most five minutes. Each manifest
segment has chunk-local seeks, recording times, original broadcast time and an
expiring URL. The preview advances across chunks; a brief transition may occur
while another MP4 loads. It is not a concatenated downloadable clip.

The entire matched core must be stored before a candidate gets Storage playback.
Optional context is trimmed to contiguous available footage and reported with
`contextTrimmed: true`. Missing matched footage is never silently skipped. Explicit
ranges with missing chunks or gaps return 409. The existing 0.1-second muxing
tolerance is retained. **Refresh playback links** renews expired URLs.
Cloud clip export is not implemented; existing extraction needs a local source.

## Persistence, privacy and retries

Immutable object keys contain:

```text
owner UUID / broadcast UUID / SHA256(indexed asset ID) / SHA256(file bytes).mp4
```

PostgreSQL stores bucket, path, hash, byte count and first confirmed upload time.
It does not store signed URLs. The attachment RPC locks the parent and validates
ownership/media identity. Ordinary metadata saves preserve attachments and reject
removing or remapping stored chunks; injected storage references are ignored.

No anonymous/browser Storage policies are added. Signing requires a reference
whose path matches the server-selected owner, broadcast, asset and content hash.
Signed URLs are temporary bearer access: anyone holding one can use it until
expiry. Playback responses use no-store/no-referrer. The HTTP API remains
localhost-only; Twitch authentication and public deployment remain future work.

Uploads never overwrite. The same hash reuses an existing object. Transient Storage
upload errors get up to three attempts with backoff; the CLI also retries 429/503.
If upload succeeds but database confirmation is uncertain, retry the same file.
The object is kept because the database write may have committed. Temporary request
files are cleaned, while durable staged files remain until the worker confirms
attachment/completion. Failed jobs retain their staged source; originals remain.
A process crash can leave extra files or unattached objects; orphan cleanup is
future work.

This uses standard uploads with bounded streaming retries, not resumable TUS.
Supabase recommends resumable uploads above 6 MB; current files fit the 50 MB cap
but failed transfers restart that file. Add TUS for unreliable remote uploaders.
See the official [standard upload](https://supabase.com/docs/guides/storage/uploads/standard-uploads)
and [resumable upload](https://supabase.com/docs/guides/storage/uploads/resumable-uploads)
guides. Storage capacity and playback egress count toward project usage.

## File roles

| File | Purpose |
| --- | --- |
| `src/chunk-storage.ts` | Private bucket access, object keys, upload retries/reuse and signing. |
| `src/media-routes.ts` | Stream/validate uploads and expose playback/preview endpoints. |
| `src/playback.ts` | Map encounters across stored chunks and generate signed manifests. |
| `src/replay-page.ts` | Browser preview player with seeking and URL refresh. |
| `src/media.ts` | Share ffprobe checks for full recordings and short chunks. |
| `src/types.ts` | Define the saved chunk media reference. |
| `src/metadata.ts` | Validate stored bucket/path/hash/size/time fields. |
| `src/persistence.ts` | Define the media-attachment repository operation. |
| `src/supabase-persistence.ts` | Attach media with owner-scoped RPCs and configure clients. |
| `src/local-persistence.ts` | Support attachment metadata in offline/test repositories. |
| `src/broadcasts.ts` | Add stored counts and replay information to status/search. |
| `src/app.ts` | Register media endpoints and report storage configuration. |
| `src/server.ts` | Initialize Storage in Supabase mode. |
| `supabase/migrations/202610300002_chunk_storage.sql` | Add media constraints and RPCs; preserve attachments during metadata updates. |
| `scripts/storage-init.ts` | Create/check the dedicated private bucket. |
| `scripts/upload-chunks.ts` | Sequentially upload existing registered files without reindexing/deletion. |
| `scripts/storage-verify.ts` | Exercise real Storage with temporary synthetic footage and clean up. |
| `test/chunk-storage.test.ts` | Check uploads, retries, private signing and cross-chunk playback. |
| `test/database.test.ts` | Apply all migrations and test ownership/immutability/preservation. |
| `package.json` | Expose setup, upload and verification commands. |
| `.env.example` | Document optional bucket and URL lifetime settings. |
| `README.md`, `docs/chunk-backend.md`, `docs/supabase-postgres.md` | Explain the updated workflow and link this guide. |
