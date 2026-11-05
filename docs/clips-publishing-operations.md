# Orders 10–12: clip exports, viewer publishing and operations

The remaining planned product features are implemented in code: trimming and MP4
export across private stored chunks, explicit Publish/Hide, a real viewer replay
panel, channel usage limits, hosted capture onboarding, archive deletion/retention,
and broadcast-search evaluation. Real Twitch installation, the creator's live
rehearsal and a deployed container still require external configuration/testing.

## Start the updated application

```powershell
npm run db:migrate
npm run media:init
npm run build
npm start
```

Keep `npm run worker` running in a separate terminal. The existing worker command
now runs both the original indexing/analysis loop and the new durable media loop.
The new loop uses leased PostgreSQL tasks, separate from pgmq jobs; a worker restart
does not lose pending exports/deletions. Start `npm run twitch:ebs` for Twitch as
described in [the Twitch guide](twitch.md).

`media:init` creates a **private** `rewindr-exports` bucket for MP4s (50 MB limit).
It leaves an existing bucket's permissions unchanged and refuses a public bucket.
Use `SUPABASE_EXPORT_BUCKET` to change its name; it must differ from the source
chunk bucket. Do not switch bucket names without accounting for existing references.

## Trim, export and share a moment

1. Search an indexed stream and play a candidate.
2. Expand **Trim, download or share** below its player.
3. Enter broadcast-clock start/end times, or pause and select **Start here** /
   **End here** at the playhead. The range must stay inside the available preview.
4. Choose a public title. The default is `Stream moment`; the private query is
   never automatically copied into a public title.
5. Choose **Create MP4**. The job continues independently of that browser tab.
6. Download the ready MP4, or explicitly choose **Publish to viewers**.
7. Use **Hide replay**, or **Clips & storage → Hide published replay**, to clear
   the current viewer selection.

The dashboard and Extension Live Config both include **Clips & storage** for
recent exports, preview/download, publishing, usage and archive cleanup. Expired
download links can be refreshed. The original full-recording download workflow
is preserved separately.

### Export behavior and bounds

- 1–180 seconds, at most eight contiguous source sections and 250 MB of source
  bytes. A range crossing a real capture gap or unstored section is rejected.
- Sources are downloaded using scoped, integrity-checked private references.
- FFmpeg makes accurate cuts, normalizes parts to 1280×720 H.264 at 30 fps with
  stereo AAC at 96 kbps, uses bounded video bitrate to fit the Storage limit, adds silence when a part lacks audio, and joins the parts into MP4.
- Output must match the requested duration within 0.35 seconds and remain under
  50 MB. Originals are retained. This is a re-encoded export, not lossless editing.
- Storage intent is checkpointed before upload. A committed upload with a lost
  response can be found and completed on retry without another encode/upload.
- Five failed processing attempts produce a visible failed task with explicit
  retry. Lost leases cannot checkpoint or complete another worker's task.
- Export metadata expires after seven days. Expired clips cannot be newly
  published or signed. Retention must run to reclaim their actual stored bytes.
- Changing the encoder build during recovery can produce different bytes. An
  immutable-checkpoint conflict is surfaced rather than silently overwriting a
  prior export; create a fresh export if its original output cannot be recovered.

## Viewer privacy and hide semantics

The viewer panel verifies a Twitch-signed browser JWT and reads only the approved
rendered MP4. It cannot search, enumerate archives, inspect prompts/assessments,
export clips or change the published selection. Moderator/viewer tokens cannot
use private broadcaster routes. An external server token is not a viewer session.

Publishing uses an optimistic version check so an outdated tab cannot silently
overwrite a newer selection. Only a ready, unexpired export from that channel's
owner may be published. Disconnecting removes the selection and capture credential.

The panel polls every five seconds, clears its replay when selection verification
fails, and uses 60-second signed URLs. Hide prevents new selection responses; it
cannot retract video bytes or URLs already received. Downloaded files and browser
caches remain outside Rewindr's control. This panel does not insert video into the
streamer's OBS output or Twitch VOD.

## Channel usage limits

The PostgreSQL guards deduplicate request identities and serialize reservations
across processes. Default daily limits reset at **00:00 UTC**:

| Work | Limit |
| --- | --- |
| Search requests | 100; each recovery reserves two additional searches. |
| Candidate analysis attempts | 60; a normal three-card search can request three. |
| MP4 exports | 20, with at most two pending exports per channel. |
| New ingestions | 4. |
| Accepted source-upload bytes | 4,000,000,000. |

Initial search checks its budget before calling the retrieval provider. Analysis
limits prevent new paid jobs and leave cards explicitly unavailable, with replay
and a retry path still usable. Upload limits are enforced before indexing is queued.

These counters are conservative request allowances, **not a dollar billing cap**.
Provider failures can still consume reservations, and existing local prototype
endpoints are trusted desktop operations rather than a public billing gateway.
Set provider account spending/usage controls separately. The defaults are in the
new SQL migration; changing them is an operator-reviewed migration, not a browser
setting. `Clips & storage` shows the current channel's counters.

## Delete archives and apply retention

Select an archive in **Clips & storage**, type its exact title and choose Delete.
Deletion immediately tombstones the archive, closes its input and hides its
publication. It then durably removes that owner's referenced Rewindr Storage
objects and metadata. Active queued/running work must finish first; failed or
review-only work does not silently resume after deletion. Partial cleanup remains
visible and retryable; it does not pretend success after one Storage failure.

**Deletion deliberately retains TwelveLabs resources, shared provider indexes,
original local recordings and capture workspaces.** Those may be shared or manually
managed. The private purge audit retains indexed/unfinished provider identifiers
for separate review; do not interpret Rewindr archive deletion as provider-account
erasure or the end of all provider storage charges.

Retention is **dry-run by default**:

```powershell
npm run retention -- --days 14
npm run retention -- --days 14 --apply
```

The first command reports candidates. The second queues deletion of up to 100 old
archives and 100 expired exports for the configured internal owner. Keep the worker
running to finish them. Active/conflicting items are reported as blocked. Repeat
the dry run to see what remains. `--watch` repeats daily; `--all-owners` is an
explicit trusted-operator option in batches of 100 hosted owners. The optional Docker
retention profile enables these destructive operations; it is not enabled by default.

Apply also prunes usage events older than 31 days, expired OAuth states/agent
credentials and completed media-task audits older than 90 days. Expired searches
receive a one-day grace period, and unreferenced terminal analysis caches expire
after seven days. Active or review-required work is preserved. Pending/failed
cleanup tasks are retained. No retention-apply command is run against your existing
archives as part of building/testing this feature.

Temporary export workspaces are swept by the media worker at startup and every
30 minutes. Only recognizable workspaces older than 24 hours, with no verifiably
active task, are eligible; failed database checks preserve files. Inspect manually:

```powershell
npm run media:sweep
npm run media:sweep -- --apply
```

The existing chunk staging sweeper also runs from the single ingestion service:
the local API, or the hosted EBS when hosted capture is enabled. Their shared PID
lock prevents both from owning the same staging directory simultaneously.

## Measure real broadcast retrieval

The old full-recording evaluator remains unchanged. A new evaluator exercises
the actual broadcast search sessions and one opt-in recovery round:

```powershell
npm run evaluate:broadcast -- init YOUR_BROADCAST_UUID
# Manually label .local/broadcast-evaluation.json, then:
npm run evaluate:broadcast -- run .local/broadcast-evaluation.json
npm run evaluate:broadcast:review -- .local/evaluation-reports/YOUR_REPORT.json
```

Initialization makes no provider searches. It saves an intentionally unverified
template bound to the current index/chunk IDs, source times, byte hashes and offset.
Fill in source-relative expected intervals, modalities, verifier/date and at least
five manually verified questions. Include absent queries with empty expected ranges.
Use the correct audio/transcription modality for spoken words or music.

The run command makes real retrieval requests and records unchanged-query results,
latency percentiles and recovery candidates. It does not automatically request
Pegasus analyses or claim an accuracy pass. Temporal overlap is only a review aid;
inspect the candidates and fill every human-review row before scoring. A similar
person can occupy the same labeled interval without being the intended event.
Run against a stable, fully indexed archive; changing footage requires re-labeling.
Reports stay under ignored `.local/`, not in synced project reference material.

## File purposes for audit

| File | Purpose |
| --- | --- |
| `src/media-store.ts` | Typed owner-scoped export/task/publication/usage repository and safe errors. |
| `src/export-storage.ts` | Private immutable export upload, scoped signing and owner-bounded Storage removal. |
| `src/export-encoder.ts` | Normalize and trim stored parts, join MP4, validate duration/size. |
| `src/media-worker.ts` | Claim/renew/fence durable media tasks, encode/upload or resume cleanup, sweep temporary work. |
| `src/media-lifecycle-routes.ts` | Export, publication, task retry, usage and title-confirmed archive-deletion APIs. |
| `src/media-sweeper.ts` | Safely identify stale export workspaces without deleting active/unverifiable data. |
| `src/clip-time.ts` | Parse/format broadcast-clock clip boundaries consistently. |
| `src/hosted.ts` | Verified-channel onboarding and hashed, revocable capture credentials with narrow ingestion routes. |
| `src/ingestion-lock.ts` | Give one local API or hosted EBS ownership of the staging directory. |
| `src/broadcast-evaluation.ts` | Bind benchmark labels to source identity and calculate overlap/latency aids. |
| `web/src/ClipTools.tsx` | Boundary controls, durable export status, downloads and explicit Publish/Hide. |
| `web/src/MediaLibrary.tsx` | Recent clips, selected replay, usage, deletion confirmation and cleanup retry. |
| `web/extension/ViewerPanel.tsx` | Read only the approved replay, refresh authorization/links and clear stale selection. |
| `scripts/media-init.ts` | Create/validate the separate private export bucket. |
| `scripts/media-verify.ts` | Real Supabase/FFmpeg integration verification with disposable synthetic footage. |
| `scripts/media-sweep.ts` | Inspect/apply local media-workspace cleanup. |
| `scripts/retention.ts` | Report or queue owner-scoped archive/export cleanup; optional daily loop. |
| `scripts/evaluate-broadcast.ts` | Generate verified-label templates and collect real broadcast search/recovery results. |
| `scripts/review-broadcast.ts` | Score explicitly reviewed candidates without claiming automatic identity recognition. |
| `supabase/migrations/202611010011_media_lifecycle.sql` | Add private media/publication/credential/usage tables and leased task RPCs. |
| `supabase/migrations/202611010012_archive_guards.sql` | Fence archive mutations, meter search/analysis/ingestion and revoke access on unlink. |
| `supabase/migrations/202611010013_search_budget.sql` | Reserve search before provider calls, reject deleted search scopes and meter live creation. |
| `supabase/migrations/202611010016_search_retention.sql` | Prune expired search snapshots and unreferenced terminal analysis caches while preserving active/review-required work. |
| `supabase/migrations/202611010015_export_size.sql` | Enforce the 50 MB exported-object limit without changing applied migrations. |
| `supabase/migrations/202611010014_export_expiry.sql` | Queue independent expired-export cleanup, prune operational records and fence job retries. |
| `deploy/Dockerfile`, `deploy/compose.yaml`, `deploy/Caddyfile` | Package EBS/worker with persistent media and HTTPS; see [deployment](deployment.md). |
| `.dockerignore` | Exclude credentials, source footage, local data and reports from image build context. |
| `test/media-lifecycle.test.ts` | Real FFmpeg boundary export, recovery, viewer isolation, deletion, credentials, budgets and expiry tests. |
| `test/media-sweeper.test.ts` | Verify dry-run behavior and preservation of active/recent/unverifiable workspaces. |
| `test/broadcast-evaluation.test.ts` | Verify source fingerprinting, manual-label requirements and overlap/latency calculations. |

Existing `app/server`, Twitch EBS/server/JWT, worker, ingestion client/routes,
recorded/live sessions, search repository/service and React workspace files wire
these modules into the existing workflow. Existing `src/media-routes.ts` remains
the original source-chunk upload/replay module; lifecycle routes are separate.

## What verification does and does not prove

Unit/integration tests use synthetic video, a real local FFmpeg binary and embedded
PostgreSQL. `npm run media:verify` additionally checks the real configured Supabase
PostgreSQL, PostgREST and private Storage, including exported MP4 playback and
Publish/Hide, then removes only its disposable owner's data. Its task claim is
owner-restricted, so it cannot consume other media jobs. It makes no TwelveLabs
index/analysis calls and does not demonstrate an actual Twitch login.

After credentials are configured, manually rehearse OAuth/Extension authorization,
iframe media restrictions, the viewer panel, token refresh and a 30–60-minute OBS
session. Container configuration is not evidence of a successful deployment. Real
retrieval quality still requires labeled footage and human review.

### Implementation verification — November 1, 2026

- All **161 automated tests passed**, including real local FFmpeg exports and
  SQL ownership, lease fencing, partial-deletion progress, credential and quota checks.
- Backend/frontend compilation and both Vite bundles passed. The Extension asset
  check found all three helper-enabled entry points, relative asset paths and no
  configured backend credential values in the browser bundle.
- Real Supabase PostgreSQL/PostgREST/Storage verification passed for source
  download, MP4 encoding/upload/playback, Publish/Hide and scoped cleanup. Disposable
  verification owners and objects were removed. Existing archives were not reset.
- Real Supabase identity/search isolation verification passed using synthetic
  Twitch/model inputs. No paid retrieval/index/analysis calls were made by those checks.
- Docker/Caddy execution, real Twitch browser rendering/login and the creator's
  OBS stream remain unverified here. These results are not retrieval-accuracy evidence.
