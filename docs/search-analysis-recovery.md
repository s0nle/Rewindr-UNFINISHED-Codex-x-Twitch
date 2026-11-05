# Orders 6–7: candidate assessments and opt-in recovery

## What the streamer gets

Open **Indexed streams**, choose a prepared broadcast and describe a moment. The
first request uses exactly that description (apart from trimming outer spaces).
Up to three grouped encounters appear with their original broadcast timestamps
and private replay. A separate request queues short assessments of those cards,
so replay does not wait for video analysis.

Each assessment has a brief sentence, a match/partial match/no match/unclear
verdict, supported details, unclear details and optional timestamped evidence.
These are Pegasus observations of the candidate footage, not Marengo's internal
reason for retrieving it. They can be wrong; the replay remains the evidence.
An assessment does not silently change the ranking or rewrite the query.

Click **Clip not found?** only when those moments are wrong. Rewindr hides their
matched intervals for this search, tries two wording variants of the original
description, merges the retrieved pool and shows up to three other encounters.
The input still contains the streamer's original words. The UI shows the variants
under **Wording used for this search** after recovery.

For example, `Lady with black floral dress and black sunglasses walks by` can
become `woman with black flower-patterned dress and black sunglasses walks past`.
It cannot acquire a man in white clothing, a location or an extra action unless
those facts were already in the original description.

This workflow also runs in Twitch Live Config through the channel-scoped
[order 9 integration](twitch.md). [Orders 10–12](clips-publishing-operations.md) add
cross-chunk exports, viewer publishing and operations. Local RTMP and
existing HLS capture are now implemented in [order 8](live-capture.md).
The **Upload recording** tab preserves the original full-recording workflow;
orders 6–7 operate on registered/ingested broadcasts in **Indexed streams**.

## Run it

Use the existing Supabase database, private chunk bucket, database certificate
and `TWELVELABS_API_KEY`. No new text-model key or subscription is introduced.
Pegasus analysis consumes your TwelveLabs usage; access depends on that account.
The source index must support the modalities selected in **Search sources**.

```powershell
npm.cmd run db:migrate
npm.cmd run build
npm.cmd start
```

In a second terminal in the same project:

```powershell
npm.cmd run worker
```

Open the server address printed by startup. Use the existing `register:chunks`
workflow for an already-indexed archive or `ingest:recording` for a new recording.
Indexing a video only in the TwelveLabs Playground does not register its source
mapping with Rewindr. Private replay requires uploaded chunk media (or a linked
local full recording), not just indexed provider IDs.

Optional `.env` settings, with these defaults:

```dotenv
CANDIDATE_ANALYSIS_ENABLED=true
TWELVELABS_ANALYSIS_MODEL=pegasus1.5
```

The supported alternative is `pegasus1.6`. API and worker should use the same
model/configuration and existing shared `DATA_DIR`. Setting analysis to false
keeps search, recovery and replay available. Restart both processes after changes.

## Request pipeline

```text
Original description
  → POST /broadcasts/:id/searches
  → Marengo searches only registered ready source IDs
  → validate/map source timestamps → group/deduplicate → three cards
  → POST /searches/:id/analyses (separate UI request)
  → cached assessment OR durable candidate_analysis jobs in pgmq
  → worker → clipped Pegasus async tasks → validate evidence → stored assessment
  → GET /searches/:id polling updates the cards

Explicit “Clip not found?” click
  → reserve recovery + persist rejection intervals atomically
  → constrained variants → Marengo → combine old/new pool
  → exclude rejected intervals → three new cards → assess those cards separately
```

The source snapshot is fixed when a search starts, including IDs, offsets and
then-ready chunks. New chunks become searchable in a **new search**. An empty
snapshot reports footage still indexing rather than searching unrelated assets.
Playback manifests read current immutable storage attachments. If a search began
before its chunk media was attached, start a new search to refresh its replay
availability; uploading that media does not require reindexing footage.

## API contracts

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/broadcasts` | List up to 100 recent broadcasts for the configured owner. |
| POST | `/broadcasts/:id/searches` | Create/reuse a search using `{requestId, question}` plus supported search settings. |
| GET | `/searches/:id` | Read current cards, recovery state, variants and assessment states. |
| POST | `/searches/:id/analyses` | Queue/cache current cards using `{version}`; returns without waiting for Pegasus. |
| POST | `/searches/:id/recover` | Explicit recovery using `{version, requestId}`. |
| POST | `/searches/:id/close` | Expire this session; unused analysis is skipped when the worker reaches it. |

Generate a fresh UUID `requestId` for each new search or recovery operation.
Reuse the same recovery ID and version after a lost response. An in-progress
duplicate returns 202; a completed duplicate returns the committed result without
running another retrieval. Conflicting versions return 409. Expired sessions
return 410. Recovery reservations last five minutes and may be retried after
expiry; provider failure preserves the rejected intervals.

Search settings are `modalities` (`visual`, `transcription`, `audio`), `operator`
(`or`/`and`), `transcriptionOptions` (`lexical`, `semantic`) and `candidateLimit`
(3–50, default 30). Existing stateless `/broadcasts/:id/search` remains available
for evaluation; it does not create assessments or a recovery session.

## Accuracy controls and limits

- The local rewriter uses a small explicit synonym/template set. It preserves
  quoted phrases, proper-name sequences and supplied details, with no runtime
  language-model call. It may improve retrieval but cannot guarantee a hit.
- Recovery runs at most three rounds, with at most two unused variants each
  round. The combined raw pool is capped at 400. Duplicate hits are merged and
  ranked through reciprocal-rank fusion; these scores are not probabilities.
- Rejections cover matched encounter intervals, with a two-second exclusion
  margin for shifted duplicates. They do not exclude an entire one-minute file;
  another event in that file may still appear. Nearby events may be suppressed
  by that margin. No video is removed from TwelveLabs or Storage.
- Rejections apply only to that search. A new search starts without them. Search
  sessions expire after six hours; expiry limits use, not physical data retention.
  The opt-in [retention command](clips-publishing-operations.md) now prunes expired
  snapshots after a one-day grace period and unreferenced terminal cache rows
  after seven days; active/review-required work is preserved.
- Return fewer than three cards if necessary. Pool exhaustion does not establish
  that the event never happened. The UI distinguishes exhaustion from the
  three-round recovery limit.
- An assessment inspects the matched interval plus five seconds on either side,
  across at most three source chunks. Each source part is at least four seconds;
  tiny boundary intersections are expanded within that same chunk. Long/gapped
  or unavailable plans can leave an assessment unavailable.
- Existing TwelveLabs asset IDs are preferred. Older imports without asset IDs
  use one-hour signed private Storage URLs. No fresh upload/index is required for
  this fallback. Replay URLs retain the normal shorter expiry and can be refreshed.
- A three-card batch usually needs three async tasks, but boundary encounters
  can require more: at most nine source-part tasks for one batch. Repeated
  recovery can add further usage. There is no claim of fixed latency or price.
- The queue permits at most two concurrently leased analysis worker operations
  per owner. External async tasks may remain pending after an operation releases
  its lease; this is not a strict cap of two provider-side tasks.

## Worker durability and assessment cache

Cache identity includes owner, footage/source identity, original question,
interval, model and prompt revision. Matching searches can share an assessment.
Rejected/inactive candidates skip parts that have not been submitted; already
submitted tasks can finish and be cached. Skipped work can be reactivated for a
new active search. Unavailable entries can queue after configuration is enabled.

Before task creation the worker stores `submitting`; after a confirmed response it
stores the task ID, with bounded metadata retries. Known task IDs are immutable,
and polling resumes them after restart. Part IDs are sent as custom/idempotency
headers, but unknown submissions are never blindly recreated. An interrupted or
unconfirmed create becomes `needs_review`. Provider reconciliation is manual;
there is no analysis-specific reconciliation CLI yet.

Checkpoints require the current job lease token. Assessment completion and queue
acknowledgment commit together. Transient queue/provider failures use the existing
bounded retry flow; generic failures do not discard known tasks. Processing has
a one-hour deadline per assessment attempt. Re-requesting a failed cache entry
requeues it with a renewed deadline; known active tasks resume, confirmed terminal
tasks get fresh identities and unknown submissions remain review-only. See
[retry and cleanup details](reliability.md).

Structured output is validated before display, including evidence timestamps
inside the requested source interval. Evidence maps from chunk-local time to
recording time and then to the original broadcast clock. Mixed judgments across
parts produce an `unclear` aggregate without another model call. Model statements
are untrusted plain text in React. Validation cannot prove their visual accuracy.

Private tables use RLS and revoke browser-role access. Only the backend's scoped
service client calls the RPCs. The application still binds to localhost with a
configured development owner; authenticate Twitch ownership before deployment.

## Verification

```powershell
npm.cmd test
npm.cmd run build
npm.cmd run search:verify
```

`search:verify` creates a temporary owner, isolated native pgmq queue and six
small synthetic media objects in the configured Supabase project. It exercises
HTTP search, clipped analysis jobs, signed playback, cached evidence, recovery,
idempotency, ownership and API restart, then removes its own data. Marengo and
Pegasus are mocked: this verifies integration, not real-world retrieval quality.
Existing archive data is not targeted.

`npm.cmd run search:verify -- --preview` in an interactive terminal leaves the
synthetic server available for UI inspection; press Enter to clean up (or it
closes after 15 minutes).
The title identifies the mocked provider results. Build first for current UI.

## File purposes

| File | Purpose |
| --- | --- |
| `src/search-types.ts` | Define and validate search sessions, candidates, assessments, part plans and checkpoints. |
| `src/query-variants.ts` | Generate bounded faithful synonyms/templates from only the original prompt. |
| `src/search-ranking.ts` | Deduplicate/fuse hits, reject encounter intervals and plan/cache/combine scoped assessments. |
| `src/search-store.ts` | Provide owner-scoped Supabase reads and search/analysis RPC calls. |
| `src/search-service.ts` | Coordinate exact initial retrieval, versioned opt-in recovery and asynchronous assessment requests. |
| `src/search-routes.ts` | Expose broadcast choices and session/search/recovery/assessment endpoints. |
| `src/providers/pegasus.ts` | Submit clipped async Pegasus tasks and parse their structured output using the installed SDK. |
| `src/analysis-config.ts` | Validate optional enable/model settings with no additional API key. |
| `src/analysis-processor.ts` | Advance durable assessment parts, retain task IDs, validate evidence and finish atomically. |
| `src/jobs.ts` | Extend job types and safe status output with candidate-analysis work. |
| `src/job-worker.ts` | Dispatch analysis jobs through the existing lease, retry and heartbeat worker. |
| `src/app.ts` | Wire session routes and stage-seven feature reporting into Fastify. |
| `src/server.ts` | Configure scoped search persistence and analysis availability for the API process. |
| `src/worker.ts` | Configure scoped repositories, Pegasus and longer-lived private analysis URLs in the worker. |
| `supabase/migrations/202610310005_search_analysis.sql` | Add private session/cache tables and atomic recovery, analysis and extended queue RPCs. |
| `web/src/BroadcastSearch.tsx` | Render indexed-stream choices, three cards, async assessments and opt-in recovery. |
| `web/src/SegmentPlayer.tsx` | Play signed source segments across chunk boundaries with a broadcast-clock timeline and refresh controls. |
| `web/src/main.tsx` | Add workspace navigation while preserving the original recording-upload view. |
| `web/src/style.css` | Style responsive cards, evidence, navigation and recovery controls. |
| `test/search-ranking.test.ts` | Verify faithful variants, interval exclusions, fusion, plans and mapped evidence. |
| `test/search-database.test.ts` | Check session scoping/idempotency, assessment caching, fencing and private database permissions. |
| `test/search-flow.test.ts` | Exercise API/worker/cache/recovery together, including metadata and provider failures. |
| `test/pegasus-provider.test.ts` | Verify installed SDK serialization and structured output against mock HTTP. |
| `scripts/search-verify.ts` | Verify native hosted queue/database/Storage integration with synthetic media and mocked models. |
| `.env.example` | Document optional assessment enable/model configuration. |
| `package.json` | Add the repeatable `search:verify` command. |
| `README.md`, `docs/search-analysis-recovery.md` | Describe current behavior, setup, limits, tests and file responsibilities. |
