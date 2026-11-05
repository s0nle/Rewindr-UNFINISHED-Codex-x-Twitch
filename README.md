# Rewindr - UNFINISHED (Codex x Twitch)

**Unfinished project snapshot.** Core search, ingestion, storage, and Twitch Extension code are implemented. End-to-end Twitch/OBS demo setup is still in progress; the project is not a finished release. Credentials, recordings, and local runtime data are excluded from this repository.

**Find a moment from a stream by describing what happened.**

Rewindr is a streamer-focused video memory and retrieval tool for the Codex x
Twitch hackathon. A streamer should be able to type a short question such as
"When did the guy wearing a Barcelona jersey come up to me?" and get a small
set of playable moments with useful timestamps.

The intended product lives inside Twitch through a Twitch Extension. Search and
preview are private broadcaster tools. Viewers see an encounter only when the
broadcaster chooses to publish it to the Extension's viewer panel.

**Status as of November 1, 2026:** a recorded-video prototype, a chunk-based
search backend, Supabase PostgreSQL metadata, private chunk Storage/replay, and
durable Supabase Queues with a separate worker, automatic recorded-file
chunking/ingestion, asynchronous candidate assessments and opt-in search recovery
are implemented. The website now has **Indexed streams** for the three-card
workflow and **Upload recording** for the original prototype. Live RTMP capture,
optional HLS input, reconnect gaps and resumable ingestion are now implemented.
Twitch OAuth channel linking and the private broadcaster Extension UI are now
implemented with a separate, JWT-protected Extension backend. Developer
registrations, HTTPS configuration and a real Twitch installation rehearsal are
still required. Cross-chunk MP4 exports, explicit Publish/Hide, the viewer replay
panel, hosted capture credentials/onboarding, usage budgets, archive cleanup and
retention, broadcast evaluation and Docker deployment files are implemented.
Real Twitch/OBS rehearsal and container deployment remain to be verified.

| Build order | Implemented component |
| --- | --- |
| 1 | Chunk search backend and timestamp mapping. |
| 2 | Supabase PostgreSQL metadata. |
| 3 | Private Storage and signed chunk replay. |
| 4 | Durable queue and separate worker. |
| 5 | FFmpeg recorded-file chunking, resumable ingestion and incremental indexing. |
| 6 | Cached, asynchronous Pegasus assessments of the top-three candidate intervals. |
| 7 | Opt-in “Clip not found?” recovery with faithful variants and session-only exclusions. |
| 8 | Independent OBS/RTMP capture, optional HLS input, minute chunks and resumable live ingestion. |
| 9 | Twitch OAuth channel linking, scoped Extension backend and configuration/private Live Config UI. |
| 10 | Trim/export stored candidate ranges into private MP4s with durable leased media tasks. |
| 11 | Explicit Publish/Hide and a viewer panel that can read only the selected rendered replay. |
| 12 | Hosted capture onboarding, quotas, retention/cleanup, broadcast evaluation and deployment packaging. |

See [orders 6–7 setup, API, limits and every file's purpose](docs/search-analysis-recovery.md).
For failed-assessment recovery, UI retries, active-time live budgets and automatic
staging cleanup, see [reliability behavior and file purposes](docs/reliability.md).
For OBS setup and live commands, see [order 8: live capture](docs/live-capture.md).
Start Twitch registration and configuration with [order 9: Twitch setup and every file's purpose](docs/twitch.md).
For the completed features, operating limits and file-purpose audit, see
[orders 10–12: clips, publishing and operations](docs/clips-publishing-operations.md).
The optional [hosted deployment guide](docs/deployment.md) includes HTTPS and
capture-computer setup; no cloud deployment or real Twitch installation is claimed.

## Contents

- [The final product](#the-final-product)
- [What is built today](#what-is-built-today)
- [The planned pipelines](#the-planned-pipelines)
- [Search, explanations, and recovery](#search-explanations-and-recovery)
- [Tools and responsibilities](#tools-and-responsibilities)
- [Run the existing application](#run-the-existing-application)
- [Use the chunk-search backend](#use-the-chunk-search-backend)
- [API reference](#api-reference)
- [Repository guide](#repository-guide)
- [Development roadmap](#development-roadmap)
- [How Codex is used](#how-codex-is-used)
- [Accuracy and verification](#accuracy-and-verification)
- [Limitations and operating considerations](#limitations-and-operating-considerations)

## The final product

### A typical streamer session

1. The streamer broadcasts to Twitch using their normal setup and supplies a
   separate copy of the footage to Rewindr.
2. Rewindr continuously saves and indexes straight, approximately one-minute
   chunks in the background.
3. Later, the streamer opens Rewindr's private controls in Twitch and types a
   simple description of a remembered moment.
4. Rewindr searches the indexed portion of that broadcast and presents up to
   three distinct candidate encounters with timestamps and replay.
5. A separate video-analysis step adds a short explanation of the details that
   match, are missing, or are unclear in each candidate.
6. If none is useful, the streamer clicks **Clip not found?** to try faithful
   query variations and see different candidates, without having to write more.
7. The streamer can preview the encounter privately, adjust a clip's boundaries,
   download a clip, and optionally publish the selected encounter to viewers.

The primary interface is typed search. Optional reference images already exist
in the local prototype, but providing an image or remembering additional facts
must not become a requirement for the normal search workflow.

### The three Twitch views

| View | Intended user | Purpose |
| --- | --- | --- |
| Configuration | Broadcaster | Connect the channel and configure Rewindr. |
| Live Configuration | Broadcaster | Search, preview, recover missed searches, and publish or hide an encounter. |
| Viewer panel | Viewers | Watch only the encounter selected by the broadcaster. |

Private questions, rejected candidates, and unrestricted archive access are not
exposed through the viewer panel. The Extension is a Twitch page interface;
publishing a panel item does not burn that replay into the broadcast or its VOD.

## What is built today

| Capability | Current state |
| --- | --- |
| Local Node/TypeScript/Fastify backend | Implemented. |
| Local React/Vite upload and search website | Implemented with Indexed streams and the original Upload recording view. |
| Upload a recording and index it in TwelveLabs | Implemented; original upload path accepts a full 20–30-minute recording. |
| Register already-indexed one-minute chunks | Implemented through a manifest and an existing TwelveLabs index. |
| Search all registered chunks in a broadcast | Implemented through the broadcast search API. |
| Correct chunk-to-recording timestamp mapping | Implemented using manifest times, not filename numbering. |
| Visual, Audio, and Transcription settings | Available through text-search endpoints; the existing website defaults to Visual. |
| Return up to three grouped chunk-search candidates | Implemented in the broadcast API; the old website initially shows six single-recording groups. |
| Replay and MP4 clip extraction | Implemented for full recordings and 1–180-second ranges across private stored chunks. |
| Optional reference-image search | Implemented in the original single-recording workflow. |
| Connect the website to the broadcast endpoints | Implemented in Indexed streams with private segment replay. |
| Automatic FFmpeg chunk ingestion | Implemented for completed files through ingest:recording and live RTMP/HLS through capture:live. |
| Supabase PostgreSQL | Implemented for recordings, broadcasts, chunks and internal owner records. |
| Supabase Storage | Implemented for registered chunks with signed playback and a backend preview page; full recordings remain local. |
| Durable queue | Implemented with Supabase Queues/pgmq, persistent job state and leases. |
| Separate persistent processing worker | Implemented for stored-chunk uploads, new time-chunk indexing and full-recording indexing in Supabase mode. |
| Pegasus candidate explanations | Implemented as separate cached async jobs with supported/unclear details and evidence. |
| Clip not found? recovery | Implemented with explicit clicks, faithful local variants and session-scoped interval rejections. |
| Twitch Extension and broadcaster authentication | Implemented for the local demo architecture; real developer registration/installation still needed. |
| Independent live input | Local RTMP receiver and optional existing HLS playlist implemented; automatic Mux provisioning is planned. |
| Viewer publishing and hiding | Implemented with versioned explicit selection and an authenticated, read-only viewer panel. |
| Hosted onboarding and capture | Implemented behind an opt-in flag with hashed, expiring, revocable channel credentials. |
| Usage and archive retention | Implemented with daily database budgets, durable title-confirmed deletion and dry-run-first retention. |
| Cloud deployment | Docker/Caddy packaging implemented; actual image/runtime/deployment still needs host verification. |

The broadcast workflow extends the existing prototype. Existing test footage is
not automatically re-uploaded or reindexed. Register its mapping and attach media
to use it in Indexed streams.

## The planned pipelines

### Live footage: Twitch remains the main broadcast destination

```text
Camera and microphone
         |
        OBS / compatible broadcaster
         |
         +--> Main output --> Twitch --> viewers
         |
         +--> Additional output --> local Rewindr RTMP receiver
                                      |
                                      v
                         FFmpeg capture process + local spool
                                      |
                                      v
                         Closed-file validation + HTTP intake
                                      |
                                      v
                         Completed straight one-minute MP4
                                      |
                                      v
                       Supabase Storage + database + queue
                                      |
                                      v
                          TwelveLabs upload and indexing
                                      |
                                      v
                            Searchable broadcast footage
```

This is the implemented same-computer live-input arrangement. OBS can send its
Custom FFmpeg Recording output to Rewindr while streaming directly to Twitch.
Mux and an OBS plugin are not required. See [the exact OBS setup](docs/live-capture.md).

An optional HLS pull input can consume an already-configured Mux playback feed.
Automatic Mux provisioning remains future work. FFmpeg creates the MP4 chunks.
The Twitch Extension supplies the interface and channel context, not the raw feed.
Twitch account connection alone is not the video-capture mechanism.

The local RTMP copy uses loopback bandwidth and additional encoding work. A cloud
copy or HLS pull additionally uses internet bandwidth. The main Twitch output has
no routing dependency on Rewindr, while both outputs share computer resources.

The live CLI sends completed local chunks through the existing intake API and
durable worker. It never uploads a file merely because that file appeared on disk.

### Recorded footage

```text
Authorized recording
        |
        v
FFmpeg straight one-minute chunks + measured manifest
        |
        v
Storage + chunk records + indexing jobs
        |
        v
TwelveLabs indexing
        |
        v
The same search, explanation, recovery, and replay workflow
```

Prerecorded tests skip Mux. New completed recordings can now use `ingest:recording`
for automatic chunking, queued Storage/indexing and incremental ready-only search.
For the footage already uploaded through the
TwelveLabs Playground, the current registration command reads the existing index
and attaches its asset IDs to the local manifest without indexing again.

### Implemented lifecycle of each live chunk

1. FFmpeg writes a local segment and then its closed-file CSV completion row.
2. The capture process validates, hashes and records the completed MP4 in its spool.
3. HTTP intake atomically saves the source interval and durable indexing job.
4. The separate worker uploads private Storage media and confirms its checkpoint.
5. The capture process can then prune its confirmed local copy. Indexing retries
   can retrieve the stored copy; unconfirmed and undersized fragments are retained.
6. The indexing worker uploads/imports the chunk into TwelveLabs, submits indexing,
   tracks progress, and marks it searchable only after the provider reports ready.

The worker must bound concurrency, retry recoverable failures, preserve submission
checkpoints, and avoid duplicate uploads or indexing submissions. A file merely
appearing in a folder is not proof that FFmpeg has finished writing it.

### Chunking and time

The default is **straight, non-overlapping, approximately one-minute chunks** for
both recorded and live ingestion. Five-second overlap remains an experiment.
It did not improve overall top-three success in the five-query comparison.

Store measured source start/end times because encoded boundaries can differ from
exact whole minutes. A match at local second 29 in a chunk starting at 1140.04
seconds belongs at recording second 1169.04, regardless of its filename.

```text
recording time = manifest chunk start + provider's chunk-local match time
original broadcast time = recording time + excerpt source offset
```

For a recording extracted from two hours into a larger video, the excerpt offset
is 7200 seconds. Display can show the original broadcast time while replay still
seeks within the local excerpt's own timeline.

For live use, show how much footage is actually searchable. A new moment must
wait for its chunk to close, upload, and finish indexing; it cannot be found
immediately just because viewers have seen it. Reconnects and missing intervals
must remain visible in the timeline rather than shifting all later timestamps.

## Search, explanations, and recovery

### Initial search

1. Accept and validate the streamer's original description.
2. Check access to the broadcaster's recording.
3. Search its registered indexed chunk IDs with the selected modalities.
4. Retrieve a larger internal candidate pool, normally 30 matches and at most 50
   per request through the current endpoint.
5. Map local times into the recording timeline and group adjacent matches.
6. Display up to three distinct candidate encounters.

The initial prompt is **not automatically rewritten**. A larger retrieval pool
can expose more possible matches; it does not itself make Marengo more accurate.
The current grouping is a timestamp heuristic, not proof that a complete encounter
has been identified. Provider ranks are preserved, not presented as probabilities.

| Search option | Evidence searched | Example |
| --- | --- | --- |
| Visual | Appearance, objects, movement, and visual context | A person wearing a floral dress passes the camera. |
| Transcription | Spoken words or their meaning | Someone asks for a photo. |
| Audio | Non-speech sounds and music | A song plays in the car. |

The index must support the selected options. OR/AND combines selected modalities,
not individual clothing words in the sentence. Transcription can use lexical
(word-based), semantic (meaning-based), or both matching options. The backend now
exposes these controls for evaluation; it does not automatically choose them yet.

### Brief candidate explanations: implemented in order 6

Marengo retrieves candidates. Separate asynchronous **Pegasus** tasks inspect each
selected interval against the original question and produce concise match details.
For example, it may say that sunglasses are visible but the requested floral
pattern cannot be verified. It must also be able to say that a candidate does not
match or that the evidence is unclear.

These are observations from another analysis step, not Marengo's internal search
reasoning. Display cards promptly, then load explanations asynchronously. Analysis
failure does not block playback. Requests are scoped to the matched interval plus
five seconds, split across at most three source parts and cached by footage,
question, model and prompt revision. The worker preserves known task IDs and
requires review for unknown submissions. Existing TwelveLabs credentials are used.

Analysis of the top three cannot recover a correct encounter that never entered
the retrieved pool. Broader retrieval and reranking are separate experiments to
measure, not claimed improvements already delivered by the current code.

### Clip not found?: implemented in order 7 and opt-in

Clicking the button:

1. Reject the currently displayed encounter intervals for this search session.
2. Generates up to two unused faithful variations of the original description, using
   natural sentences, synonyms, and clearer wording without inventing facts.
3. Search again, merge results, and suppress rejected intervals and duplicates.
4. Return up to three new candidates and analyze their intervals as well.

Preserve meaningful brands and the streamer's constraints. Do not invent clothing
colors, companions, actions, or locations. Do not borrow details from a rejected
or unconfirmed candidate to describe the intended encounter.

The exclusions last across retries within that search only. They do not delete
files, remove videos from the TwelveLabs index, or affect independent searches.
Reject encounter intervals rather than whole one-minute files when possible,
since another useful encounter may exist in the same file. If candidates run out,
show fewer results or an honest exhausted-results state.

The first search keeps the exact original wording. Recovery uses constrained
local synonyms/templates and requires no new runtime text-model key. It is
limited to three rounds; it cannot invent missing facts or guarantee retrieval.
See [the complete session/assessment contracts](docs/search-analysis-recovery.md).

## Tools and responsibilities

| Tool | Responsibility | Status |
| --- | --- | --- |
| Node.js 24+ and TypeScript | Run and implement the backend. | In use. |
| Fastify | HTTP endpoints, uploads, validation flow, and local media delivery. | In use. |
| Zod | Validate requests and provider responses. | In use. |
| TwelveLabs Node SDK | Upload, inspect indexing, and search Marengo 3.0 indexes. | In use. |
| FFmpeg / ffprobe | Receive RTMP/HLS, encode closed chunks, extract clips and inspect media. | Implemented for live and recorded input. |
| React, Vite, and CSS | Existing local prototype interface; foundation for future views. | In use. |
| Twitch Extension Helper and backend verification | Native Twitch views, channel context, and authorized broadcaster actions. | Implemented with verified broadcaster JWTs and per-request channel-to-owner lookup. |
| Twitch OAuth / Helix | Channel connection and necessary broadcast metadata. | Implemented identity-link handshake and app-authenticated live status; separate from Extension JWT validation. |
| Mux | Receive the additional live feed and expose video to the worker. | Planned. |
| Supabase Storage | Retain private imported/ingested chunks and sign playback links. | Implemented for recorded and live sources. |
| Supabase PostgreSQL | Owner, recording, broadcast and chunk metadata; sessions/published selections come later. | Implemented for current metadata. |
| Supabase JS client and node-postgres | Runtime database API access and verified SQL migration connections. | In use. |
| Supabase Queues | Persist leased background upload/indexing jobs. | Implemented with backend-only pgmq wrappers. |
| TwelveLabs Pegasus | Assess candidate footage and produce match details. | Implemented in the separate worker. |
| Railway or another suitable host | Run the API and persistent worker as separate services. | Planned; hosting choice remains revisitable. |

No custom vector database, GPU server, or model training is required for the
initial managed-search architecture. Secrets stay on the backend. Node's native
environment-file loading is already used; dotenv is not required by this project.

## Run the existing application

### Requirements

- Node.js **24 or later** and npm.
- A TwelveLabs API key with access and allowance for the intended operations.
- Enough local disk space for source media and generated clips.
- Creator-authorized footage for indexing and analysis.

The npm dependencies include FFmpeg and ffprobe binaries. Optional binary-path
overrides are supported. Commands below use PowerShell on Windows; use `npm`
instead of `npm.cmd` on other platforms.

### Setup

```powershell
cd path\to\rewindr
npm.cmd ci
```

Create or update `.env` locally with your own values. Preserve an existing `.env`
rather than replacing working credentials. Example values below are placeholders:

```dotenv
TWELVELABS_API_KEY=YOUR_API_KEY
TWELVELABS_INDEX_ID=
PORT=3000
DATA_DIR=.local/rewindr
FFPROBE_PATH=
FFMPEG_PATH=
# Optional for local-only work; required to select hosted PostgreSQL metadata.
SUPABASE_URL=
SUPABASE_SECRET_KEY=
SUPABASE_DB_URL=
SUPABASE_DB_CA_PATH=
```

| Setting | Meaning |
| --- | --- |
| TWELVELABS_API_KEY | Backend credential used for real provider requests. |
| TWELVELABS_INDEX_ID | Optional existing index for the original upload workflow and default for the registration CLI. |
| PORT | API port, normally 3000. |
| DATA_DIR | Directory for local recordings, metadata, clips, checkpoints, and broadcast manifests. |
| FFPROBE_PATH / FFMPEG_PATH | Optional paths overriding the bundled binaries. |

The original upload workflow requires a Marengo 3.0 index with visual and audio
enabled when reusing an index. Otherwise it creates an index for that recording.
For chunk registration, `--index-id` explicitly selects the existing chunk index.

For PostgreSQL metadata, configure the Supabase values and downloaded CA, then
apply migrations, initialize private Storage, and import existing JSON before starting the server:

```powershell
npm.cmd run db:migrate
npm.cmd run storage:init
npm.cmd run db:import-local
npm.cmd run db:import-local -- --apply
```

The import is dry-run unless `--apply` is supplied. It retains local files and
existing provider IDs; it does not upload or re-index video. See
[the PostgreSQL guide](docs/supabase-postgres.md) for verified TLS, owner identity,
mode selection and the migration runner. Supabase settings select the database
automatically; broken settings fail instead of silently reverting to JSON.
Supabase mode also checks that the private chunk bucket exists before startup.
Optional `SUPABASE_STORAGE_BUCKET` and `SIGNED_URL_TTL_SECONDS` default to
`rewindr-chunks` and 300 seconds; see [the Storage guide](docs/storage.md).

Build and start the API:

```powershell
npm.cmd run build
npm.cmd start
```

In Supabase mode, start the persistent worker in a **second terminal in the same
project**, with the same `.env` and DATA_DIR:

```powershell
npm.cmd run worker:start
```

Run `npm.cmd run queue:reconcile` once to enqueue unfinished recordings from before
order 4. Ready/failed recordings are skipped. The API no longer processes Supabase
jobs itself; uploads wait until a worker runs. See [the worker guide](docs/queue-worker.md)
for retries, recovery and shared-media requirements.

Open **http://127.0.0.1:3000**. For development, use separate terminals running
`npm.cmd run dev` and `npm.cmd run dev:web`, then open
**http://127.0.0.1:5173**. Vite proxies `/api` to port 3000. Changing the API port
also requires updating that development proxy. `npm.cmd run local` builds and
starts the application in one command.
For Supabase development, also run `npm.cmd run worker` in another terminal.
Explicit local JSON mode keeps its original in-process processor.

### Existing full-recording website

1. Select an authorized 20–30-minute MP4 and confirm the authorization checkbox.
2. Choose **Upload & index** and wait for **Ready to search**.
3. Enter a description and choose **Find moment**.
4. Preview a candidate, adjust the download range, and extract an MP4 clip.
5. Optionally use a PNG/JPEG reference or the current playback frame for another
   search; this does not re-index the existing video.

The current upload path accepts a browser-playable H.264/yuv420p MP4 with optional
AAC audio, duration 1200–1800 seconds, and at most 4,000,000,000 bytes. Provider
storage and input limits may be lower. Clip extraction is limited to five minutes
per clip and one extraction at a time. Reference images are limited to 5 MiB.

This workflow uploads one video asset. Multipart byte-transfer pieces are not
one-minute time chunks. Uploading a recording again can consume provider storage;
use the registration workflow for chunks that are already indexed.

## Use the chunk-search backend

The original straight-cut files and their matching TwelveLabs index must already
exist. Start the backend, then register them in another terminal:

```powershell
npm.cmd run register:chunks -- --manifest chunks/manifest.csv --index-id YOUR_INDEX_ID --title "Rewindr straight-cut test"
```

Replace YOUR_INDEX_ID with the actual straight-cut index ID. The command reads
provider metadata; it does not upload, re-index, or delete footage. It returns a
`broadcastId` to use for subsequent requests.

Both the original FFmpeg headerless CSV and the header
`filename,start_seconds,end_seconds` are supported. Registration matches exact
filenames, validates durations and readiness, and rejects deliberate overlap and
ambiguous mappings. The current implementation permits a 0.1-second boundary
tolerance, individual intervals up to 65 seconds, and up to 2000 indexed assets.
Repeated registration creates a new local broadcast record, not a provider asset.

Search the returned broadcast:

```powershell
$body = @{
    question = 'Guy with a Polo Ralph Lauren hoodie comes up and asks for a photo'
    candidateLimit = 30
    modalities = @('visual', 'transcription')
    operator = 'or'
    transcriptionOptions = @('lexical', 'semantic')
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
    -Uri 'http://127.0.0.1:3000/broadcasts/YOUR_BROADCAST_ID/search' `
    -ContentType 'application/json' -Body $body
```

The result includes raw `moments`, up to three grouped `encounters`, time mappings,
and `moreCandidatesAvailable`. The default modality is Visual; explicitly select
other supported modalities when comparing queries. This legacy stateless route
still reports `analysisStatus: "not_implemented"`; use the session routes below
or Indexed streams to queue Pegasus assessments.

To attach replay from the same full recording already stored in the local app,
add `--source-recording-id YOUR_EXISTING_RECORDING_ID` when registering. The source
and chunk manifest must share timeline zero and contain the same footage. Duration
checks alone cannot establish content identity. If the excerpt begins partway
through a larger source broadcast, also supply `--source-offset-seconds`.

Without stored chunk media or an attached full recording, the API returns IDs and timestamps, with
`playbackUrl: null` and `replayAvailable: false`. It does not invent a working
media URL. The website is not connected to these broadcast endpoints yet.
See [the detailed chunk backend guide](docs/chunk-backend.md) for the contract.

### Order 3: store chunks and replay privately

Registered chunks can now be validated, uploaded to private Supabase Storage and
linked in PostgreSQL. A small preview page plays encounters across chunk boundaries
using expiring URLs. Local originals are retained; footage is not reindexed.

```powershell
npm.cmd run db:migrate
npm.cmd run storage:init
npm.cmd run storage:verify
# Start the backend, register the existing manifest/index, then:
npm.cmd run upload:chunks -- --broadcast-id YOUR_BROADCAST_ID --directory chunks --limit 1
```

Repeat without `--limit 1` for the full set. See [setup, replay examples and each
file's role](docs/storage.md). Stored candidates return `playbackFormat: "html"`:
open `playbackUrl` as a page, or consume `manifestUrl` for a segment player.
The main React website still uses the single-recording workflow.

### Order 4: durable jobs and a separate worker

Supabase chunk uploads now return HTTP 202 with a job ID; the uploader waits for
the worker to store the file and attach its database reference. New full-recording
metadata atomically enqueues its TwelveLabs processing workflow. Jobs have leases,
heartbeats, bounded retries, safe checkpoints and visible terminal states.
Ambiguous paid submissions require review instead of automatic resubmission.

```powershell
npm.cmd run queue:verify
```

This uses an isolated queue, tiny synthetic video and two child workers, then
cleans up. It makes no TwelveLabs requests. See [order 4 commands and each file's
role](docs/queue-worker.md). API and worker still share local source/staging media;
local RTMP and existing-HLS ingestion are implemented in order 8. Recorded time-chunk indexing is
implemented by order 5 below.

### Order 5: automatic recorded-file ingestion

Prepare a local recording without backend/provider requests:

```powershell
npm.cmd run ingest:recording -- --file "rewindr-test.mp4" --prepare-only
```

The command prints a workspace containing resumable state, straight-cut MP4s and
a source-timestamp manifest. To ingest a new recording using an existing compatible
index, run:

```powershell
npm.cmd run ingest:recording -- --file "new-authorized-recording.mp4" --index-id YOUR_INDEX_ID --title "Rewindr demo" --creator-authorized
```

Each closed chunk is validated and queued, stored privately, uploaded/indexed in
TwelveLabs and promoted to searchable only after readiness is confirmed. Small
tails stay with the preceding chunk; sources retain complete, non-overlapping
coverage. Resume uses `--resume PATH_TO_WORKSPACE --creator-authorized` and reuses
saved jobs. API and worker must both be running with the same DATA_DIR.

This new path consumes indexing allowance. For the existing already-indexed test
chunks, keep using registration instead of creating another ingestion. See
[order 5 commands, API contracts, limits and every file's purpose](docs/ingestion.md).
Input is a completed local video, not an OBS/Mux live feed.

### Order 8: live capture

Start API and worker, then run `npm.cmd run capture:live -- --creator-authorized`.
Supply `--index-id YOUR_INDEX_ID` if `TWELVELABS_INDEX_ID` is absent. Send a separate
OBS output to the printed loopback RTMP URL. Default duration is one hour;
`--max-seconds 1800` selects a 30-minute rehearsal. Closed minute chunks enter the
existing worker pipeline while capture continues. Press Ctrl+C to finish capture.

Resume pending footage with `--resume WORKSPACE --finish --creator-authorized`.
Use `--input-env LIVE_INPUT_URL` for an already-configured authorized HLS source.
No Mux dependency or new API key is required for local RTMP. See
[OBS setup, timestamps, disk limits, verification and every file's role](docs/live-capture.md).
`npm.cmd run live:verify` uses a 68-second synthetic broadcast, real hosted
Supabase/pgmq/Storage and mocked TwelveLabs operations, then removes its resources.

### Orders 6–7: assessments and “Clip not found?”

Apply migrations with `npm.cmd run db:migrate`, build, and run the API and worker
in separate terminals. Open **Indexed streams** and choose a registered broadcast.
The first search uses your exact wording; three replay cards appear before
Pegasus finishes. Assessments load separately with supported and unclear details.
Click **Clip not found?** to hide those moments only in this search and try two
faithful wording variants. It never removes footage from your index.

The existing TwelveLabs key is used. Optional settings are
`CANDIDATE_ANALYSIS_ENABLED=true` and `TWELVELABS_ANALYSIS_MODEL=pegasus1.5`.
No additional text-model key is needed. Pegasus calls consume TwelveLabs usage.

`npm.cmd run search:verify` checks real hosted database/queue/Storage integration
with temporary synthetic media and mocked Marengo/Pegasus calls, then cleans up.
Add `-- --preview` for an interactive synthetic UI preview. See
[the complete workflow, limits and individual file purposes](docs/search-analysis-recovery.md).

## API reference

Useful local checks are `npm.cmd run typecheck`, `npm.cmd run build`, and
`npm.cmd test`. The normal tests use mock provider responses; the separate
evaluation commands described below make real provider requests.

All current routes are local prototype endpoints, not authenticated public APIs.

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/health` | Configuration and local service status. |
| GET | `/broadcasts` | List recent owner-scoped streams for the UI. |
| POST | `/broadcasts/:id/searches` | Create an exact-query search session with up to three cards. |
| GET | `/searches/:id` | Poll cards, assessments and recovery state. |
| POST | `/searches/:id/analyses` | Queue/cache assessments for the displayed version. |
| POST | `/searches/:id/recover` | Reject displayed intervals for this session and explicitly try faithful variants. |
| POST | `/searches/:id/close` | Expire the session and make unused analysis eligible to skip. |
| POST | `/broadcasts/import` | Register existing indexed chunks and their source manifest. |
| GET | `/broadcasts/:id/status` | Read chunk mapping, coverage, and registration-time readiness verification. |
| POST | `/broadcasts/:id/search` | Search registered chunks and return up to three grouped candidates. |
| POST | `/broadcasts/:id/chunks/:assetId/media` | Stage a registered MP4 (at most 50 MB), then queue its Storage upload; return jobId. |
| GET | `/broadcasts/:id/playback?startSeconds=...&endSeconds=...` | Get signed segments for stored footage, up to five minutes. |
| GET | `/broadcasts/:id/replay?startSeconds=...&endSeconds=...` | Open the small backend preview player. |
| GET | `/jobs/:id` | Read owner-scoped job status without private input or lease tokens. |
| POST | `/jobs/:id/retry` | Explicitly retry a failed job; require creator authorization. |
| POST | `/ingestions` | Create/reuse an ingestion for a completed recording and validate its existing index. |
| GET | `/ingestions/:id` | Read accepted/stored/searchable source coverage and per-chunk status. |
| POST | `/ingestions/:id/chunks/:ordinal` | Stage one exact planned chunk and atomically enqueue indexing work. |
| POST | `/ingestions/:id/complete` | Seal input after every planned source interval is accepted. |
| POST | `/recordings` | Upload one full recording; return 202 before provider indexing completes. |
| GET | `/recordings/:id/status` | Processing phase, metadata, provider IDs, and error state. |
| POST | `/recordings/:id/search` | Search one indexed recording with configurable modalities. |
| POST | `/recordings/:id/search/reference` | Search a recording using a PNG/JPEG reference and optional text. |
| GET | `/recordings/:id/source` | Stream a local source recording with byte-range seeking. |
| POST | `/recordings/:id/clips` | Extract a selected interval as an MP4. |
| GET | `/recordings/:id/clips/:clipId/file` | Download an extracted clip. |

All footage intake routes require `X-Creator-Authorized: true`. This is an authorization
attestation for footage, not an authenticated broadcaster identity.

Broadcast import accepts `title`, `indexId`, and a `chunks` array containing
`filename`, `startSeconds`, `endSeconds`, and optional `indexedAssetId`.
`sourceRecordingId` and `sourceOffsetSeconds` are optional. Its body limit is 1 MiB.
Broadcast search uses `question`, optional `candidateLimit` (3–50), `modalities`,
`operator`, and `transcriptionOptions`. Single-recording search uses `limit`
instead of `candidateLimit`; the reference-image route remains Visual only.

Imported broadcast status `searchableThroughSeconds` means contiguous coverage
verified at registration. Ingested broadcasts use indexed-ready chunks and also
include ingestion progress with separate captured/stored/searchable coverage.
This is not an OBS/Mux live-feed freshness monitor.

## Repository guide

| Path | Responsibility |
| --- | --- |
| `src/server.ts` | Read settings, create the provider, and start the local server with a process lock. |
| `src/app.ts` | Connect API routes, validate requests, and serve local playback and downloads. |
| `src/broadcasts.ts` | Register chunk mappings, persist local broadcasts, and expose broadcast status/search. |
| `src/chunk-search.ts` | Validate search settings, map timestamps, and group chunk matches. |
| `src/chunk-storage.ts` | Scope private objects, upload with retries, reuse hashes and sign URLs. |
| `src/media-routes.ts` | Stream validated uploads and expose private playback/preview routes. |
| `src/playback.ts` | Map stored encounter intervals across chunk boundaries. |
| `src/replay-page.ts` | Provide a small browser preview player for the chunk backend. |
| `src/jobs.ts` | Define jobs, redact public status and access Supabase job RPCs. |
| `src/job-worker.ts` | Claim/renew jobs, run background work, retry and complete atomically. |
| `src/worker.ts` | Start the separate persistent worker process. |
| `src/job-routes.ts` | Expose scoped job status and explicit retry routes. |
| `src/ingestion-types.ts` | Define source/checkpoint schemas and plan non-overlapping chunk intervals. |
| `src/segmenter.ts` | Encode, validate and hash closed MP4s before publishing their manifest entries. |
| `src/recorded-ingestion.ts` | Manage resumable source workspaces, locking and bounded submission. |
| `src/ingestion-client.ts` | Stream files to local ingestion APIs with retry/polling. |
| `src/ingestion-routes.ts` | Expose ingestion creation, intake, progress and completion. |
| `src/ingestions.ts` | Access owner-scoped ingestion metadata and calculate source coverage. |
| `src/ingest-processor.ts` | Resume Storage/provider checkpoints and promote ready chunks atomically. |
| `src/staged-media.ts` | Construct validated staged-file paths. |
| `src/manifest.ts` | Parse supported chunk CSV formats. |
| `src/filenames.ts` | Validate plain MP4 names, preserving Unicode and punctuation while rejecting paths and controls. |
| `src/search.ts` | Group matches for the original single-recording workflow. |
| `src/providers/twelvelabs.ts` | Isolate SDK upload, catalog, indexing, and search operations. |
| `src/providers/errors.ts` | Convert provider failures into readable messages and safe diagnostics. |
| `src/http-errors.ts` | Represent intentional HTTP errors without exposing unexpected server details. |
| `src/types.ts` | Define recording, broadcast, chunk, and provider contracts. |
| `src/store.ts` | Retained asynchronous local recording JSON implementation. |
| `src/persistence.ts` | Contracts for asynchronous recording and broadcast repositories. |
| `src/metadata.ts` | Shared validation for saved and imported records. |
| `src/local-persistence.ts` | Local broadcast JSON repository and offline persistence factory. |
| `src/supabase-persistence.ts` | Owner-scoped Supabase repositories, configuration and database health. |
| `src/db-connection.ts` | Verified PostgreSQL migration connections and safe errors. |
| `src/import-local.ts` | Preflight and resumable metadata import logic. |
| `src/import-errors.ts` | Preserve safe import/conflict diagnostics and redact unexpected errors. |
| `supabase/migrations/` | Versioned SQL tables, constraints, indexes and private access rules. |
| `scripts/db-migrate.ts` | Apply SQL migrations with a private history table and hash checks. |
| `scripts/db-import-local.ts` | Preview or apply the retained-JSON import. |
| `scripts/db-verify.ts` | Verify live metadata operations using temporary rows and mocked video retrieval. |
| `scripts/storage-init.ts` | Create/check the private chunk bucket. |
| `scripts/upload-chunks.ts` | Upload registered MP4s sequentially without reindexing. |
| `scripts/storage-verify.ts` | Verify real Storage with tiny synthetic videos and temporary metadata. |
| `docs/storage.md` | Order 3 setup, contracts, limits, retry handling and file roles. |
| `scripts/queue-reconcile.ts` | Create missing jobs for unfinished recordings. |
| `scripts/queue-verify.ts` | Verify real pgmq and competing worker processes with temporary data. |
| `docs/queue-worker.md` | Order 4 setup, guarantees, failure handling and file roles. |
| `scripts/ingest-recording.ts` | Run local preparation or authorized recorded-file ingestion. |
| `scripts/ingestion-verify.ts` | Verify the hosted pipeline with synthetic footage and a mocked video provider. |
| `docs/ingestion.md` | Order 5 commands, architecture, boundaries and file descriptions. |
| `docs/supabase-postgres.md` | Database setup, ownership scope, import and operating instructions. |
| `src/processor.ts` | Advance original full-recording upload/indexing jobs inside the API process. |
| `src/media.ts` | Inspect and validate media through ffprobe. |
| `src/clips.ts` | Extract and cache MP4 clips through FFmpeg. |
| `src/evaluation.ts` | Validate and score the existing recorded-video evaluation workflow. |
| `web/src/main.tsx` | Workspace tabs plus original full-recording upload, preview, image-search and clip-download UI. |
| `web/src/BroadcastSearch.tsx`, `web/src/SegmentPlayer.tsx` | Indexed-stream search, three cards, assessments, recovery and private segment replay. |
| `src/search-service.ts`, `src/search-routes.ts` | Coordinate original retrieval, versioned recovery and separate assessment requests. |
| `src/search-store.ts`, `src/search-types.ts` | Scoped session/cache persistence and validated contracts. |
| `src/search-ranking.ts`, `src/query-variants.ts` | Fuse hits, reject intervals and produce faithful bounded wording variants. |
| `src/providers/pegasus.ts`, `src/analysis-processor.ts`, `src/analysis-config.ts` | Configure and durably process clipped async video assessments. |
| `supabase/migrations/202610310005_search_analysis.sql` | Private search/cache tables and atomic recovery/analysis/queue functions. |
| `scripts/search-verify.ts` | Hosted native queue/Storage verification with synthetic footage and mocked video models. |
| `src/live-segmenter.ts` | Receive RTMP/HLS and expose only finalized straight-cut media. |
| `src/live-capture.ts`, `scripts/capture-live.ts` | Run resumable capture, reconnects, bounded upload backlog and confirmed Storage cleanup. |
| `src/session-writer.ts` | Serialize capture-state writes without letting one rejected write disable later saves. |
| `src/live-clock.ts`, `src/request-retry.ts` | Track active capture time and retry browser requests without stale updates. |
| `src/staging-sweeper.ts`, `src/staged-media.ts` | Coordinate worker file use and clean eligible old staging copies. |
| Migrations `008`–`009` | Requeue failed analyses safely and classify global staging references. |
| `supabase/migrations/202610310006_live_capture.sql` | Extend ingestion to unknown-duration live sources and explicit gap-aware intervals. |
| `supabase/migrations/202610310007_chunk_plan_microseconds.sql` | Align recorded acceptance/sealing with the TypeScript integer-microsecond chunk plan. |
| `scripts/live-verify.ts`, `test/live-*.test.ts` | Verify real RTMP/HLS and hosted live ingestion with synthetic media. |
| `docs/live-capture.md` | OBS setup, commands, timing/disk limits and every live-capture file's purpose. |
| `test/search-*.test.ts`, `test/pegasus-provider.test.ts` | Search/assessment/recovery contracts, database permissions and SDK serialization. |
| `docs/search-analysis-recovery.md` | Orders 6–7 setup, API, limits and a sentence describing each file. |
| `web/src/style.css` | Style the local prototype. |
| `web/vite.config.ts` | Configure frontend builds and the development API proxy. |
| `scripts/register-chunks.ts` | Register a manifest and existing index from the terminal. |
| `scripts/evaluate.ts`, `scripts/review.ts` | Generate and review retrieval evaluation reports. |
| `test/` | Local API, SDK serialization, validation, and timestamp tests. |
| `test/processor.test.ts` | Metadata retry, checkpoint retention, shutdown and halted-health regressions. |
| `test/queue-database.test.ts` | Job enqueue/ack transactions, retry budgets, ownership and fencing. |
| `test/job-worker.test.ts` | Worker restart, staging, provider checkpoints and lease loss. |
| `test/queue-shim.ts` | Test-only pgmq substitute for embedded PostgreSQL; hosted checks use the real extension. |
| `test/segmenter.test.ts` | Actual FFmpeg/AAC encoding, safe tails, resume and cancellation. |
| `test/ingestion-database.test.ts` | Atomic chunk intake/promotion, ownership and provider/media identity. |
| `test/ingestion-flow.test.ts` | End-to-end producer/API/worker recovery and boundary replay. |
| `docs/chunk-backend.md` | Detailed chunk registration/search instructions. |
| `docs/providers.md` | Provider integration notes. |
| `docs/verification.md` | Historical implementation and diagnostic notes; use this README for current status. |
| `.local/rewindr/` | Runtime media, JSON metadata, checkpoints, and generated clips. |
| `chunks/` | Manually prepared straight-cut test footage and its manifest. |

## Development roadmap

### Phase 1: reliable recorded-video search

The current backend foundation belongs to this phase. Next milestones:

1. Register the real straight-cut index and verify source mappings end to end.
2. Connect the recorded-video interface to the broadcast endpoints and confirm
   playback seeks to the right source moment.
3. Run unchanged questions across controlled modality and candidate-pool settings.
4. Expand beyond the five initial examples and record top-three success, missed
   events, duplicates, timing accuracy, and search latency.

Exit milestone: a recorded broadcast can be searched and replayed with verified
timestamps, and retrieval limitations are measured rather than hidden by the UI.

### Phase 2: broadcaster workflow and durable backend

The private Twitch integration is implemented; configure and rehearse it next:

1. Register separate Twitch OAuth and Extension clients using [the setup guide](docs/twitch.md).
2. Configure the HTTPS EBS, Twitch asset paths and content allowlists.
3. Rehearse account linking and scoped search/replay in Twitch Live Config.
4. Verify asynchronous assessments, opt-in recovery and token refresh inside Twitch.
5. Rehearse the implemented OBS/RTMP or existing-HLS producer with a creator's
   30–60-minute broadcast and measure indexing lag, gaps and capture overhead.

Twitch OAuth tokens and Extension JWTs serve different purposes and must be
validated appropriately. Keep provider secrets and privileged storage credentials
on the server. Authenticate owners before deploying the local prototype publicly.

Exit milestone: an authorized broadcaster can use the Twitch interface to search,
preview, and recover missed queries while processing survives ordinary failures.

### Phase 3: live demonstration and viewer publishing

1. Configure the separate OBS output into the implemented local RTMP receiver;
   Mux remains an optional future managed input.
2. Start capture early enough to retain the intended demo timeline and preserve
   gaps/reconnects explicitly.
3. Run the planned **30–60-minute live test** using the creator's own broadcast.
4. Demonstrate a longer, approximately six-hour authorized prerecorded archive
   through the recorded ingestion path; do not imply this is a completed six-hour
   live reliability test.
5. Rehearse the implemented broadcaster-controlled Publish/Hide and viewer panel
   with separate allowed broadcaster/viewer test accounts.
6. Document demo setup, test-account access, provider usage, failure behavior, and
   the Codex-assisted development process for the submission.

Extension testing/release availability must be verified before the demo; a local
prototype is not evidence of a publicly released Twitch Extension. Keep the
demonstration honest about recorded vs. live input and which footage is indexed.

Exit milestone: show a real broadcaster question, correct timestamped replay,
brief candidate assessment, a working recovery example where supported, and an
explicitly published viewer replay inside Twitch.

## How Codex is used

Codex is the development assistant used to inspect the existing project, extend
the TypeScript backend, implement chunk registration and timestamp mapping, write
local tests, and maintain these instructions. It is not the production video
retrieval model: Marengo searches the footage, and the implemented Pegasus step
assesses candidates. A separate runtime text-model integration is still needed if
we choose model-generated query variations.

For the hackathon submission, document concrete development contributions and
include the required session evidence once confirmed against the submission
requirements. Do not imply that planned features are already functioning merely
because their implementation has been discussed with Codex.

## Accuracy and verification

### What has been checked

On October 30, 2026, `npm.cmd run build` and all **20 local tests** passed after the
chunk-search update. Tests cover request validation, local uploads and seeking,
mocked processing failures/restarts, measured chunk offsets, excerpt offsets,
boundary grouping, missing intervals, three-candidate output, persistence,
honest replay availability, and installed-SDK pagination/filter/modality encoding.

Those tests use synthetic media and mock provider responses. They establish
software behavior, not how accurately Marengo recognizes real stream footage.
No live uploads or indexing were performed as part of that backend update.

A same-day follow-up brings the suite to **30 passing tests**, covering safe
Unicode/punctuation filenames, quoted CSV parsing, and multipart reference-image
search including upload limits, both field orders, and cleanup on provider failure.

The PostgreSQL stage brings the suite to **41 passing tests**, including real SQL
constraint, transaction rollback and browser-role access checks in embedded
PostgreSQL. The hosted Supabase migration was also applied and verified through
real metadata writes and a fresh application instance, with a mocked video
provider. Four existing local recording records were imported; a repeat import
found all four already present. Local media and JSON were retained. These checks
do not establish real video retrieval accuracy or a Twitch login integration.

The resilience follow-up brings the suite to **52 tests**, adding bounded metadata
retries, provider-ID retention across failed checkpoints, visible halted-worker
health, safe conflict diagnostics and separate reporting for skipped stale imports.

Order 3 brings the suite to **60 tests**. New checks cover chunk upload validation,
immutable Storage keys, reuse/retries, database attachment preservation, owner
scoping, context trimming and cross-chunk playback. The new migration and private
bucket were also exercised against hosted Supabase using temporary synthetic
footage: uploads, restart reads, signed byte ranges and public-access denial passed.
Temporary test objects were removed. Video search remained mocked; this does not
establish retrieval quality or end-to-end live capture reliability.

Order 4 adds queue/worker regressions for a total of **77 tests**. Hosted checks also
passed real pgmq enqueue/rollback, deduplication, API restart, simulated lease expiry,
two competing child worker processes, stale acknowledgment rejection, private
Storage playback and staged-file cleanup. Only a tiny synthetic clip was uploaded;
no TwelveLabs calls were made. Temporary verification resources were removed.

Order 5 brings the suite to **91 tests**. Actual FFmpeg/AAC encoding, tail handling,
resume, bounded submission, metadata fencing, source recovery and ready-only search
are covered. Hosted verification passed real HTTP, pgmq, PostgreSQL, private Storage,
recovery and cross-chunk signed replay with a synthetic 68-second recording.
TwelveLabs upload/index/search were mocked; no archive was reindexed and these
checks do not establish video retrieval accuracy. Temporary resources were removed.

Orders 6–7 extend the suite to **107 tests** for search sessions, faithful variants,
assessment caching, durable async task checkpoints, evidence and opt-in recovery.
Order 8 extends it to **115 tests**, including real RTMP and HLS capture and an
RTMP reconnect through ingestion/search/replay. Hosted live verification passed
with a 68-second real-time synthetic broadcast and a full 60-second chunk, real
Supabase/pgmq/Storage, mocked TwelveLabs and automatic test-resource cleanup.
Both chunks reached ready after the short publisher had ended; this is not a
claim of sub-minute capture-to-search latency or a completed OBS/Twitch rehearsal.

Reliability regressions additionally cover exact and adjacent microsecond tail
boundaries, half-microsecond rounding, save recovery after a real file-write
failure, and resume past corrupt/oversized/missing raw segments. Rejected raw
media is retained with a durable quarantine ledger; accepted chunk identities
remain unchanged. See [live workspace recovery](docs/live-capture.md).

### Initial manual retrieval observations

These are five user-reported original-query tests, not a representative benchmark:

| Query | Straight cuts | Five-second overlap |
| --- | --- | --- |
| Woman in black floral dress and sunglasses | Missed the first ten results. | Missed the first ten results. |
| Man in Polo Ralph Lauren hoodie asking for a photo | Sixth result. | Not found in reported results. |
| Song playing in a car | First result. | First result. |
| Man in black shirt/sunglasses requesting a beach photo | Third result. | First result. |
| Phone showing a backflip recording | First result. | First result. |

Both versions found three of five original-query encounters within the top three.
This supports keeping straight cuts as the current baseline; it does not prove
that overlap is universally worse. A richer dress query recovered the encounter,
but added a nearby companion's appearance. That is additional evidence from the
streamer, not proof that faithful rewriting alone fixes the miss.

### How to evaluate next

Keep questions and ground truth fixed when comparing settings. Record the actual
matched interval, its rank, selected modalities, candidate pool, elapsed search
time, and whether the right encounter is playable. Include similar-looking
distractors, boundary encounters, brief appearances, audio/speech clues, and
confirmed absent events. Inspect the footage rather than trusting thumbnails.

Distinguish failure to retrieve the right event from failure to rank it highly.
Verification can reorder retrieved candidates but cannot inspect a missing event
that was never selected. A low-ranked result is not a confidence probability, and
an empty list is not proof an event never happened.

The existing evaluation tools target the original single-recording workflow:

```powershell
npm.cmd run evaluate -- evaluation/manifest.local.json
npm.cmd run evaluate:review -- PATH_TO_REPORT_JSON PATH_TO_REVIEW_JSON
```

First replace the unverified template's examples and empty intervals with inspected
footage and source identifiers. The existing gate asks for ten real positives,
three absent events, and a human-confirmed qualifying top-three match on at least
eight positives, using temporal intersection-over-union of at least 0.3. This is a
project acceptance target, not a result already achieved. A chunk-aware evaluation
path still needs to be connected before applying that gate to the new API.
Evaluation makes real provider requests and can consume allowance.

## Limitations and operating considerations

- **Local prototype:** the API binds to 127.0.0.1 and restricts nonlocal hosts and
  cross-origin browser requests. The footage-attestation checkbox is not login.
  The current service is not ready for public deployment.
- **Persistence:** configured Supabase stores current metadata in PostgreSQL;
  explicit local mode retains JSON. Registered chunks can also be stored privately
  in Supabase; full recording sources and old JSON remain on disk. One
  API process owns the upload directory; separate workers share that persistent
  DATA_DIR and use database leases. The separate EBS maps a verified Twitch
  broadcaster to that internal owner; the local ingestion API stays private.
  The internal development owner alone is not login.
- **Processing recovery:** known upload/indexing checkpoints can resume. Ambiguous
  interrupted submissions fail explicitly rather than blindly duplicating paid
  work. Persistent polling failures and a four-hour processing deadline terminate
  the original job. Deleting local files does not delete TwelveLabs resources.
  Supabase jobs use checkpoint retries, leases and up to five consecutive job
  failures before a visible failed/review state. Queue outages retry polling rather
  than permanently stopping the worker. Local JSON mode retains the original
  halted-processor policy. `/health` reports external worker liveness/backlog or
  local processor retry/halt state. Import remains new-records-only,
  reporting older matching remote records separately rather than overwriting them.
- **Retrieval quality:** clothing, logos, brief appearances, and similar people can
  still be confused. No perfect search, fixed latency, or calibrated confidence
  is claimed. Explanations must not rationalize a visibly wrong candidate.
- **Live freshness:** chunk completion, upload, provider indexing, and search are
  separate delays. Report ingestion lag and search latency separately.
  Recorded ingestion now reports separate accepted/stored/searchable coverage;
  live RTMP/HLS acquisition is implemented with bounded sessions; indefinite
  stream timelines remain future work.
- **Storage and usage:** uploaded source chunks are retained in Supabase and
  provider assets in TwelveLabs. Account for both, plus temporary worker storage,
  Mux video processing/delivery, analysis calls, retries, and hosting. Define
  retention and deletion before running large archives continuously. Consult
  current provider dashboards rather than treating old quoted prices as fixed.
- **Public replay:** access control, allowed media domains, playback behavior in
  Extension views, and explicit broadcaster approval must be verified during the
  Twitch integration. Never expose the full private archive through the panel.

### Common setup problems

| Symptom | What to check |
| --- | --- |
| Provider not configured / 503 | Set TWELVELABS_API_KEY in the backend's `.env` and restart the server. |
| Import cannot read the index | Check the actual index ID, API key permissions, and provider availability. |
| Import reports missing or ambiguous filenames | Use the CSV belonging to these exact files; provide explicit indexedAssetId values when necessary. |
| Duration mismatch or overlap rejection | Use the straight-cut index and its original manifest, not the overlap experiment's files. |
| Candidate has no playback URL | Upload its registered chunks with upload:chunks or attach a matching local source recording; registration alone does not import media. |
| Provider storage capacity exceeded | Inspect current storage usage and allowance; a valid local file can still exceed account capacity. |
| Server says another process owns the data directory | Stop the existing server normally before starting another; do not remove an active process lock. |
| Upload stays queued / no worker in health | Run worker:start in a second terminal with the same project, .env and persistent DATA_DIR. |
| Job says needs_review | Inspect the interrupted provider submission and reconcile its IDs before retrying; automatic resubmission is blocked. |
| Recorded ingestion stops midway | Resume the printed workspace with the same source/index; inspect failed/review jobs and keep source/staged files. |
| Frontend still shows the old recording workflow | That UI has not yet been connected to the new broadcast endpoints. |

The metadata database stage includes versioned migrations, owner-scoped reads,
atomic broadcast writes, local-data import and database-backed health checks.
`npm.cmd run db:verify` checks the real hosted database with a temporary owner and
mocked video retrieval; it removes only its temporary rows. Normal tests add real
SQL constraint/rollback/access checks in embedded PostgreSQL. See the PostgreSQL
guide for the current verification procedure.

The immediate next milestone is to register the existing straight-cut index and
verify searches and timestamps against the actual footage, then wire the new
broadcast endpoints into the interface.
