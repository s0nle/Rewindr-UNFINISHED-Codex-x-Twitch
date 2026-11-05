# Order 5: FFmpeg recording ingestion

This stage connects a completed local recording to automatic straight-cut chunks,
private Storage, queued TwelveLabs indexing, incremental search and replay.
It uses the database, bucket and worker from orders 2–4. No additional API key
or npm dependency is required.

## Pipeline

Chunk plans quantize the canonical source-duration decimal once to integer
microseconds, with half-microsecond ties rounded up, then apply the 4.1-second
tail threshold using integers. PostgreSQL acceptance and sealing use the same
rule through migration 007. For example, 64.1 seconds produces 60 + 4.1 seconds;
64.099999 seconds stays one merged chunk. Applied migration files are unchanged.

```text
Completed local video (4 seconds–12 hours)
  -> resumable local workspace + source identity
  -> FFmpeg encodes a source interval into a temporary MP4
  -> close, ffprobe, hash, atomic rename, then publish its manifest entry
  -> authorized HTTP intake validates and stages the exact file
  -> chunk metadata + durable queue message commit together
  -> worker stores the private source and confirms its checkpoint
  -> TwelveLabs multipart upload -> asset readiness -> indexing -> ready
  -> ready chunk + queue acknowledgment commit together
  -> existing broadcast search and signed replay can use that chunk
```

The producer encodes/uploads incrementally. It does not wait for the entire source
to finish before queuing the first chunk. At most four unfinished jobs are allowed
before the producer waits for progress, bounding the ingestion backlog. The API
and worker still share DATA_DIR, so use the same machine for this demo.

This command takes a **completed local file**. Live RTMP/HLS capture is now
available separately in [order 8](live-capture.md). Indexed streams supports the
resulting ready broadcasts; Upload recording retains the full-recording path.

## Setup

Use the existing saved Supabase/TwelveLabs settings. Apply migrations and build:

```powershell
npm.cmd run db:migrate
npm.cmd run storage:init
npm.cmd run build
```

Run the API and worker in separate terminals in the same project:

```powershell
# Terminal 1
npm.cmd start
```

```powershell
# Terminal 2
npm.cmd run worker:start
```

Restart both after updating their code. Ingestion requires an existing index with
Marengo 3.0 visual/audio enabled. Index validation is read-only; the command does
not create a provider index. Use the Playground or an existing compatible index.

## First test: local preparation only

```powershell
npm.cmd run ingest:recording -- --file "rewindr-test.mp4" --prepare-only
```

This performs local FFmpeg work and saves chunks, but sends no HTTP, Supabase or
TwelveLabs requests. It prints a workspace path such as:

```text
.local/rewindr/ingestion-workspaces/UUID/
  session.json
  chunks/chunk_0000.mp4
  chunks/chunk_0001.mp4
  chunks/manifest.csv
```

The original input is retained. The workspace owns its generated files. By default
each source interval is one minute; a tail shorter than about 4.1 seconds is joined
to the previous interval. Files remain non-overlapping, no footage is discarded,
and the longest merged chunk remains below 65 seconds. Marengo 3.0 requires at
least four seconds; see its [input requirements](https://beta.docs.twelvelabs.io/docs/concepts/models/marengo/marengo-3-0).

Each interval is encoded independently with accurate input seeking and bounded
duration. FFmpeg completes and ffprobe validates the MP4 before the producer
renames it or calls the uploader. File appearance or a stable file size alone is
not used as proof of completion. The CSV records the explicit source seek ranges;
encoded container duration is checked against each interval. Existing segment-muxer
files must keep their original measured manifest rather than mixing both methods.

Output is H.264/yuv420p at 30 fps with optional AAC audio, dimensions at most
1280 per side without upscaling, and a 4 Mbps video/128 kbps audio bitrate target.
Encoding/filtering uses two threads to limit CPU contention. The application
enforces a 50 MB chunk cap and validates resolution, aspect ratio and duration.

## Ingest new footage

For a new authorized recording:

```powershell
npm.cmd run ingest:recording -- --file "new-authorized-recording.mp4" --index-id YOUR_INDEX_ID --title "Rewindr demo" --creator-authorized
```

Fresh runs can also use TWELVELABS_INDEX_ID. To ingest the workspace prepared
above, reuse its printed path:

```powershell
npm.cmd run ingest:recording -- --resume "PATH_TO_WORKSPACE" --index-id YOUR_INDEX_ID --creator-authorized
```

This uploads new assets and requests provider indexing, which consumes allowance.
For footage **already indexed** in the Playground, keep using `register:chunks`
and `upload:chunks`. Running this new command against it creates another ingestion;
it is not a migration/reuse command for the old thirty-file test index.

Use `--source-offset-seconds 7200` on a fresh run when the local excerpt starts
two hours into the original stream. Search timestamps add that offset; playback
still uses the excerpt/chunk-local time. Optional `--workspace PATH` creates a fresh
directory at an explicit location; it refuses to overwrite an existing directory.

## Resume and backpressure

The session saves the ingestion/broadcast UUID before the first backend request.
Create and chunk intake are idempotent within that ingestion. Each accepted ordinal
is bound to its source interval, filename, byte count and SHA-256. A repeated hash
reuses the existing job; changed bytes are rejected.

On resume, saved settings and source identity are checked. Closed chunks are reused
without re-encoding or overwriting them. Cached completed jobs are skipped;
unfinished jobs are awaited. Explicitly resuming a failed chunk can restage its
original bytes and retry known checkpoints. `needs_review` requires inspecting and
reconciling provider resources first; the producer does not bypass that state.

The source fingerprint uses canonical path, byte size and modification time; it
is not a full source-content hash. Each generated chunk has its own content hash.
Keep the source unchanged while a run is active. Restoring/changing source metadata
manually can defeat that coarse source check; do not use it to mix different files.

The workspace has a PID lock. Stop the other producer before resuming. A confirmed
stale PID lock is reclaimed; malformed locks require inspection. Ctrl+C stops the
producer and removes its current partial file/lock through normal cleanup. Already
accepted jobs can continue in the worker. Forced termination may leave partial
files, which are not treated as completed chunks.

The producer waits when four jobs are outstanding and polls a job for up to ten
minutes at a time. A producer timeout does not cancel remote work. Inspect status,
fix/start the worker and resume. This is per-ingestion deduplication: deliberately
starting a second fresh workspace for the same source can upload/index it again.

## Worker checkpoints and media identity

New `ingest_chunk` jobs have these phases:

```text
queued -> stored -> creating_upload -> uploading -> asset_processing
       -> submitting_index -> indexing -> ready
```

Storage is confirmed before any provider upload creation. Upload session/asset IDs,
transport progress and indexed asset IDs are fenced checkpoints. A failed metadata
write retries with the returned IDs in memory, rather than repeating the external
operation. Interrupted creation/submission with an unknown result goes to
`needs_review`. Transient safe operations use the existing bounded job retries.

After provider transfer and its checkpoint are confirmed, the worker can remove
its staged local copy. If a later retry needs missing bytes, it downloads the
private Storage source, checks its length/hash and resumes the known session.
Producer chunks and the original recording remain available in the workspace.

An ingestion chunk has a stable UUID before a provider ID exists, so its Storage
key uses `ingest:CHUNK_UUID` as the hashed identity. Database constraints bind this
key to the owner, broadcast and source hash. Ready broadcast records carry the
corresponding storageKey for scoped signed replay; legacy imported chunks retain
their original provider-ID-based keys.

Ready promotion and queue acknowledgment are atomic. Stored, uploading or indexing
chunks are excluded from the existing broadcast search model. Returning results
never implies that all recently captured footage has finished indexing.

## APIs and progress

| Method | Route | Purpose |
| --- | --- | --- |
| POST | `/ingestions` | Validate an existing index and create/reuse a source ingestion. |
| GET | `/ingestions/:id` | Read source progress, per-chunk phases and safe job state. |
| POST | `/ingestions/:id/chunks/:ordinal` | Accept one exact planned MP4 as multipart field `chunk`. |
| POST | `/ingestions/:id/complete` | Seal source input after all planned intervals are accepted. |
| POST | `/broadcasts/:id/search` | Search only indexed-ready chunks with the original prompt. |
| GET | `/jobs/:id` | Inspect job state, retries or review requirements. |

All write routes require `X-Creator-Authorized: true`; this is footage attestation,
not Twitch authentication. The API remains localhost-only. The source creation
body includes `id`, `title`, `indexId`, `sourceDurationSeconds`, `sourceOffsetSeconds`
and `sourceFingerprint`; it never accepts a filesystem path or request-supplied owner.

Status separates:

- `capturedThroughSeconds`: contiguous source intervals accepted by the backend.
- `storedThroughSeconds`: contiguous accepted footage confirmed in private Storage.
- `searchableThroughSeconds`: contiguous indexed-ready footage from time zero.

A later ready chunk can still be searched while an earlier gap remains; coverage
stops at the gap instead of pretending the missing interval is searchable. Empty
searches before any chunk is ready return `pendingIngestion: true`, not evidence
that the remembered event is absent. A sealed ingestion is `ready` only when all
of its accepted jobs/chunks are ready. Failed/review jobs produce
`attention_required`. Public responses omit upload headers, lease tokens and local
staging identities.

## Verification and limits

```powershell
npm.cmd test
npm.cmd run ingestion:verify
```

The normal suite has 91 tests, including real FFmpeg/AAC cuts, tail merging,
resume/source checks, SQL transactions, ownership/fencing, ready-only search,
private replay identities and source recovery. Its video provider is mocked.
Tests run with bounded concurrency to limit embedded PostgreSQL/FFmpeg contention.

`ingestion:verify` creates a temporary owner/queue and a tiny synthetic 68-second
recording. It uses actual FFmpeg, HTTP, Supabase Queues, PostgreSQL, private Storage
and signed playback. It simulates missing staged media, resumes the original jobs,
and checks boundary replay after an API restart. TwelveLabs upload/index/search
are mocked, so it makes no paid video-provider requests. It cleans up only its own
temporary queue, objects, metadata and local fixture.

Source/staging disks must be shared by API and worker. Producer workspace files,
failed staged files and queue archives need a retention policy before continuous
operation. The API now sweeps eligible `chunk-jobs/` copies under the conservative
[staging cleanup policy](reliability.md); producer workspaces and queue archives
still need their own retention policy. Timing tests do not prove
Marengo retrieval accuracy, throughput for nine-hour archives, or live reliability.
Pegasus assessments and opt-in query recovery are now implemented in
[orders 6–7](search-analysis-recovery.md). Live capture is implemented in
[order 8](live-capture.md); Twitch connection and private UI are in [order 9](twitch.md).

Official encoding reference: [FFmpeg documentation](https://ffmpeg.org/ffmpeg.html).

## File roles

| File | Purpose |
| --- | --- |
| `src/ingestion-types.ts` | Validate source/checkpoint data and plan non-overlapping intervals with safe tails. |
| `src/segmenter.ts` | Run FFmpeg per interval, validate/hash closed files and publish the manifest atomically. |
| `src/recorded-ingestion.ts` | Manage source workspaces, locking, resumable state, bounded submission and progress. |
| `src/ingestion-client.ts` | Stream chunks to the local API with bounded retries and job polling. |
| `scripts/ingest-recording.ts` | Parse terminal arguments and handle cancellation for preparation/ingestion. |
| `src/ingestion-routes.ts` | Expose source creation, chunk intake, status and completion endpoints. |
| `src/ingestions.ts` | Provide owner-scoped database access and coverage/status calculations. |
| `src/ingest-processor.ts` | Advance durable Storage/provider checkpoints and recover missing source bytes. |
| `src/staged-media.ts` | Construct validated UUID-based paths for worker-owned staged files. |
| `supabase/migrations/202610310004_ingestion.sql` | Add source/chunk tables, atomic intake/promotion, ownership constraints and ready-only broadcast reads. |
| `scripts/ingestion-verify.ts` | Verify the real hosted pipeline with isolated synthetic footage and a mocked video provider. |
| `test/segmenter.test.ts` | Check real encoding, safe boundaries, preparation, resume and cancellation. |
| `test/ingestion-database.test.ts` | Check atomicity, deduplication, source identity, permissions and review/retry behavior. |
| `test/ingestion-flow.test.ts` | Exercise the complete producer/API/worker flow, recovery and cross-chunk search/replay. |
| `src/chunk-storage.ts` | Recover private source files with streamed hash checks and support stable ingestion keys. |
| `src/playback.ts` | Sign each ready segment using its validated storage identity. |
| `src/media.ts` | Inspect long recording inputs separately from full-recording prototype and chunk validation. |
| `src/types.ts`, `src/metadata.ts` | Extend shared contracts for ingestion broadcasts, empty pending timelines and stable keys. |
| `src/providers/twelvelabs.ts` | Validate existing index compatibility without creating another index. |
| `src/jobs.ts` | Add the ingestion job kind and redact its public status. |
| `src/job-worker.ts`, `src/worker.ts` | Dispatch ingestion jobs with guarded provider calls and scoped repositories. |
| `src/media-routes.ts` | Keep ingestion media on its own endpoint instead of the legacy attachment route. |
| `src/app.ts`, `src/server.ts` | Wire order 5 repositories/routes into Supabase mode and health reporting. |
| `src/broadcasts.ts` | Report ingestion progress and return honest pending searches. |
| `package.json` | Add ingestion/verification commands and bound test concurrency. |
| `README.md`, `docs/queue-worker.md`, `docs/storage.md`, `docs/chunk-backend.md`, `docs/supabase-postgres.md` | Update capabilities, commands and integration boundaries. |
| `docs/ingestion.md` | Describe order 5 usage, guarantees, limits and every file's role. |
