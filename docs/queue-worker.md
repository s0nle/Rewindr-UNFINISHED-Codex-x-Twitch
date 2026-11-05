# Order 4: durable jobs and a separate worker

Supabase mode now uses Supabase Queues (`pgmq`) and a separate Node process for
background work. The API accepts requests, persists work and serves search/replay.
The worker claims durable jobs and saves progress. Local JSON mode retains the
original in-process processor for offline development.

Order 5 adds a third job kind, `ingest_chunk`, for [automatic recorded-file
ingestion](ingestion.md). The following two original paths remain supported.

## Two implemented paths

**Already-indexed chunks:**

```text
Existing chunk + registered broadcast
  -> API validates and saves a staged MP4 under DATA_DIR/chunk-jobs
  -> PostgreSQL job + pgmq message commit together
  -> HTTP 202 with jobId and jobStatusUrl
  -> worker validates staged bytes and uploads private Supabase Storage
  -> media attachment + job completion + message archive commit together
  -> worker removes its staged copy
  -> signed replay is available
```

This path does not reindex the existing TwelveLabs test footage. It
retains the original file in your `chunks` folder. The worker removes only its
separate staged copy after confirmation.

**Original full-recording website:**

```text
20–30 minute recording -> local source MP4 + metadata insert
  -> database trigger atomically creates a recording job and message
  -> worker probes media and resumes TwelveLabs upload/indexing checkpoints
  -> incomplete processing is requeued with a delay
  -> ready/failed status reaches the existing website through its normal polling
```

Full recording sources remain local. Order 5 now automates straight chunk creation
and incremental indexing for completed local recordings. OBS/Mux capture and an
indefinite live timeline remain separate work. The worker handles all three job kinds.

## Run it

No new npm dependency or provider key is needed. Reuse the saved Supabase and
TwelveLabs backend settings. The migration enables pgmq and creates the dedicated
`rewindr_jobs` queue. It does not expose `pgmq_public` to browsers.

Stop any old API process before switching to the new version. From the project:

```powershell
npm.cmd run db:migrate
npm.cmd run storage:init
npm.cmd run build
npm.cmd run queue:reconcile
```

`queue:reconcile` creates missing jobs for this owner's unfinished recordings.
It reuses existing jobs and makes no provider requests itself. Ready/failed
recordings and already-indexed chunk registrations are skipped. Once a worker is
running, unfinished jobs can proceed with the existing footage authorization.

Start **two terminals in the same project**, both using the same `.env`:

```powershell
# Terminal 1: API
npm.cmd start
```

```powershell
# Terminal 2: persistent worker
npm.cmd run worker:start
```

For source development use `npm.cmd run dev` and `npm.cmd run worker` instead.
The worker source command is not watch mode. Restart it after changing its code.

Both processes must use the **same persistent DATA_DIR**. For this demo, run them
on the same computer. The queue is remote/durable but source MP4s and staging are
still local. Two unrelated hosting disks will not work. Move all required media
to shared/cloud storage before distributing API and workers across hosts.

The API retains its PID lock. Workers use database leases instead of that lock,
so API and worker can run together and multiple workers can claim different jobs.
The shared queue's worker processes all trusted owner IDs from queued database
rows; HTTP reads and writes remain scoped to the server's configured owner.
The capture/queue API stays private. [Order 9](twitch.md) adds Twitch linking and
a separate, channel-scoped Extension service around the search repositories.

## Upload, inspect and retry

The existing chunk uploader now waits for each background job to finish:

```powershell
npm.cmd run upload:chunks -- --broadcast-id YOUR_BROADCAST_ID --directory chunks --limit 1
```

It uploads sequentially and polls for up to ten minutes per job. A timeout stops
the CLI, not the remote job. Repeating a completed upload reuses its job/object.
Repeating a pending upload reuses the pending job and discards the duplicate
staged copy. Explicitly reuploading a failed chunk with the same identity restores
its staged source and queues it again; a changed hash is rejected.

```powershell
Invoke-RestMethod 'http://127.0.0.1:3000/jobs/YOUR_JOB_ID'
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:3000/jobs/YOUR_JOB_ID/retry' -Headers @{'X-Creator-Authorized'='true'}
Invoke-RestMethod 'http://127.0.0.1:3000/health'
```

The retry endpoint accepts failed jobs only. Recording retries resume known
upload/indexing IDs instead of starting again. Original source timestamps retain
the existing four-hour processing deadline. `needs_review` requires inspecting
provider resources and deliberately reconciling metadata; it cannot be blindly
retried through this endpoint. There is no reconciliation UI yet.

Job responses include state, target IDs, attempts, consecutive failures, error
and timestamps. They omit lease tokens, local file IDs, private upload headers
and signed video URLs. Recording status also includes `processingJob`.

`/health` reports stage 4 (or stage 5 with ingestion enabled) and
`processing.state: "external-worker"`, queue counts,
worker liveness and latest heartbeat. Pending work with no recent worker heartbeat
returns 503/degraded. A worker is considered online for 90 seconds after its last
heartbeat, so a dead process may take that long to appear offline. Failed/review
counts remain visible; a terminal job error is separate from API availability.

## Reliability rules

- **Atomic enqueue:** new recording metadata and its message share a transaction.
  Chunk intake saves its staged file first, then commits the job/message together.
  An uncertain enqueue retains the file because the transaction may have committed.
- **Deduplication:** one recording job per recording ID; one upload job per
  broadcast/indexed-asset target. It is not global content deduplication across
  separate full-recording uploads. If a full-recording response is uncertain,
  inspect metadata before uploading it again under a new ID.
- **Leases:** default 300 seconds, renewed every 30 seconds while busy. A normal
  idle worker polls every five seconds. Transient renewals retry up to three
  attempts within the known lease window. Invalid/expired tokens cannot checkpoint
  recording state, attach media, or acknowledge work.
- **Recovery:** an unacknowledged message becomes visible after lease expiry.
  Known provider IDs resume. Interrupted non-idempotent submissions go to
  `needs_review` to avoid repeating paid work with an unknown result.
- **Retries:** transient job failures use 2/4/8/16-second backoff and stop after
  five consecutive failures. Successful nonterminal progress resets that count.
  Provider polling retains its existing bounded retry policy and four-hour deadline.
  A database outage during outcome persistence leaves the message for redelivery.
- **Worker availability:** queue connection errors retry with backoff instead of
  permanently stopping the process. Ctrl+C/SIGTERM stops claiming new jobs and
  waits for the current operation. Forced termination relies on lease recovery.
- **Media safety:** chunk bytes are hashed again before uploading. Private objects
  are immutable/reused. Media attachment and queue acknowledgment commit together;
  an uncertain commit leaves staged media for safe retry rather than deleting it.

These are durable at-least-once processing semantics, not a promise of exactly-once
external provider side effects. A request already in flight cannot be recalled
when a lease is lost; fencing and ambiguous-submission review limit the damage.

## Retention and operating limits

Completed and failed jobs remain queryable. pgmq archives acknowledged messages.
The API now sweeps eligible staged files at startup and every 15 minutes. Old
orphans and completed copies are eligible after 24 hours; Storage-backed terminal
ingestion copies after seven days. Active work and unrecoverable sources remain
protected. File locks, global owner/queue references and replica checks guard
cleanup. See [the full policy](reliability.md). Database/queue archive retention
remains separate work.

Stop API and workers before metadata maintenance/imports. New imported nonterminal
recordings enqueue via the same trigger. Keep backend keys private. Existing
browser-role table/RPC restrictions remain in place; the worker uses trusted
database owner IDs and scoped persistence/Storage factories.

## Verification

```powershell
npm.cmd test
npm.cmd run queue:verify
```

Normal tests contact no hosted services. PGlite tests run the application SQL with
a clearly labeled test-only pgmq shim because PGlite cannot load the native
extension. They verify transactions, deduplication, leases/fencing, retry budgets,
access restrictions and review handling. Worker tests use synthetic MP4s and mock
Storage/provider behavior.

`queue:verify` requires a production build and saved database credentials. It
creates an isolated verification queue/owner and a tiny four-second synthetic MP4.
It verifies real pgmq, transaction rollback, queued HTTP intake, deduplication, API
restart, simulated lease expiry, two competing child worker processes, stale-token
rejection, private upload/signed byte-range replay, and staged-file cleanup. Child
workers receive no TwelveLabs key. It removes its own queue, objects, workers and
metadata; it never consumes the normal production queue or indexes your archive.

Official behavior reference: [Supabase pgmq documentation](https://supabase.com/docs/guides/queues/pgmq).

## File roles

| File | Purpose |
| --- | --- |
| `src/jobs.ts` | Typed job contracts, safe public status and the Supabase queue/RPC adapter. |
| `src/job-worker.ts` | Claim/renew jobs, run uploads or indexing, retry safely and clean staged copies after commit. |
| `src/worker.ts` | Configure and run the separate worker process with graceful shutdown. |
| `src/job-routes.ts` | Owner-scoped job status and explicit retry endpoints. |
| `src/processor.ts` | Add single-recording advancement without a timer or recovery of other jobs. |
| `src/media-routes.ts` | Stage validated chunk uploads and return durable job IDs. |
| `src/app.ts` | Use external processing in queue mode and expose queue health/status. |
| `src/server.ts` | Enable durable jobs for Supabase API startup. |
| `supabase/migrations/202610300003_jobs.sql` | Create pgmq queue/job/worker tables, atomic enqueue, fenced checkpoints, completion, retries and private permissions. |
| `scripts/upload-chunks.ts` | Wait for queued uploads while retaining the original files. |
| `scripts/queue-reconcile.ts` | Enqueue missing jobs for existing unfinished recordings without duplicating jobs. |
| `scripts/queue-verify.ts` | Verify real queue/worker/Storage behavior with isolated temporary data. |
| `test/queue-shim.ts` | Supply the explicitly test-only pgmq SQL substitute for embedded PostgreSQL. |
| `test/queue-database.test.ts` | Check queue transactions, retries, ownership and expired-worker fencing. |
| `test/job-worker.test.ts` | Check worker recovery, staging, lease loss and provider checkpoint handling. |
| `test/database.test.ts` | Apply the new migration through the shared test harness. |
| `package.json` | Add worker, reconciliation and verification commands. |
| `.env.example` | Explain the shared DATA_DIR requirement. |
| `README.md` | Update startup, API reference and current implementation status. |
| `docs/storage.md` | Explain that production chunk uploads now complete through queued workers. |
| `docs/supabase-postgres.md` | Document job tables and the separate worker stage. |
| `docs/chunk-backend.md` | Link queued upload operation and current backend boundaries. |
| `docs/queue-worker.md` | Describe order 4 architecture, commands, guarantees, limits and file roles. |
