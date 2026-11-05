# Order 8: live capture

## Pipeline and scope

```text
OBS streaming output ───────────────────────────────→ Twitch
OBS secondary output → local FFmpeg RTMP receiver
  → closed straight-cut MP4 about every 60 seconds
  → ffprobe validation + content hash + durable spool manifest
  → existing localhost ingestion API
  → atomic chunk record + durable pgmq job
  → worker → private Supabase Storage → TwelveLabs upload/index
  → ready chunks → Indexed streams search, assessments and signed replay
```

Rewindr receives a separate copy. It does not forward your primary Twitch stream,
change your Twitch stream key, or require Mux. The default receiver is bound to
`127.0.0.1`, for OBS and Rewindr on the same computer. The existing API and worker
must share their `DATA_DIR`. Other encoders can publish the same RTMP format.

An optional HLS pull input also accepts an existing authorized HTTPS `.m3u8`
source, including a Mux playback feed if you separately configure one. This stage
does not create Mux live streams, manage Mux credentials or acquire Twitch video.
Do not point it at an arbitrary Twitch page URL: that is not a media playlist.

## Start the services

Use the existing Supabase/TwelveLabs settings. Migration 006 has been added;
applied migration files 001–005 are unchanged. No npm dependency or API key was
added for the RTMP receiver: the existing bundled FFmpeg is used.

```powershell
npm.cmd run db:migrate
npm.cmd run build
npm.cmd start
```

In a second terminal:

```powershell
npm.cmd run worker
```

In a third terminal, start a capture (one hour maximum by default):

```powershell
npm.cmd run capture:live -- --creator-authorized --max-seconds 3600
```

This uses `TWELVELABS_INDEX_ID` from `.env`. If it is absent, supply your existing
Marengo index explicitly:

```powershell
npm.cmd run capture:live -- --creator-authorized --index-id YOUR_INDEX_ID --title "Live demo" --max-seconds 1800
```

The CLI prints the workspace, broadcast ID and **local secondary-output URL**:

```text
rtmp://127.0.0.1:1935/live/<broadcast UUID>
```

Start Rewindr before starting the secondary encoder output. The RTMP path is a
session identifier, not a verified authentication mechanism. Network isolation
comes from binding only to loopback; do not expose this prototype to the internet.

## Send a separate copy from OBS

OBS has a Custom FFmpeg Recording output that can send to a URL. In a separate
OBS profile for the demo, configure:

1. **Settings → Output → Advanced → Recording**.
2. **Type: Custom Output (FFmpeg)** and **FFmpeg Output Type: Output to URL**.
3. Paste the complete local URL printed by Rewindr into **File path or URL**.
4. Use **FLV** container, **libx264** video and **AAC** audio. If necessary enable
   **Show all codecs** to find the encoders.
5. Use a reasonable secondary resolution such as 1280×720, 30 fps, about
   2,500–3,500 Kbps video, a two-second keyframe interval and 96–128 Kbps audio.
6. Click **Start Recording** to start the secondary feed. Your normal
   **Start Streaming** output can continue directly to Twitch.

This uses OBS's Recording output for the secondary feed, so that output is not
simultaneously a normal local recording. It also adds encoding work. If your
existing setup already has a separate-output plugin, it can instead publish to
the same local RTMP URL; no plugin is installed or required by Rewindr.

The receiver encodes its own bounded H.264/AAC output, with a 720p bounding box,
forced minute keyframes and an approximate 3.5 Mbps maximum video rate. A source
without audio is supported. Invalid finalized raw media is quarantined rather
than submitted; later valid footage can proceed. Missing tools, permissions and
disk failures still interrupt capture so they can be fixed before resuming.

Official references: [OBS Custom FFmpeg Record output](https://github.com/obsproject/obs-studio/wiki/Streaming-With-SRT-or-RIST-Protocols#option-2-stream-srt-or-rist-using-the-custom-ffmpeg-record-output),
[FFmpeg RTMP input](https://ffmpeg.org/ffmpeg-protocols.html#rtmp), and
[FFmpeg segment lists](https://ffmpeg.org/ffmpeg-formats.html#segment_002c-stream_005fsegment_002c-ssegment).

## Search during capture

Open **Indexed streams** and click **Refresh streams**. Choose the live broadcast
after it appears. Start searching after at least one chunk finishes indexing.
The first chunk closes at approximately 60 seconds; Storage, upload and indexing
add further latency. This is near-live archive search, not frame-by-frame search.

New searches use the currently ready chunks. An existing search session keeps
its original source snapshot, so submit a **new search** to include newer footage.
The current website's **Upload recording** view is a different full-file path.

`GET /ingestions/:id` reports:

| Field | Meaning |
| --- | --- |
| `mode: "live"` | An open-ended live source, rather than a known-duration recording. |
| `status` | Accepting, processing, ready or attention_required; not a producer heartbeat. |
| `sourceDurationSeconds` | Latest accepted interval end, not an invented final stream length. |
| `latestCapturedSeconds` | Latest accepted completed-chunk end. |
| `latestStoredSeconds` | Latest stored-chunk end. |
| `latestSearchableSeconds` | Latest ready-chunk end. |
| `capturedThroughSeconds`, `storedThroughSeconds`, `searchableThroughSeconds` | Continuous coverage starting from zero; these stop at a gap. |
| `gaps` | Missing intervals before/between accepted live chunks, including initial connection wait. |
| `sealedAt` | Producer has finished sending accepted footage; indexing may still be running. |

Nothing is searchable until the existing worker promotes its chunk to ready.

## Stop, reconnect and resume

Stop the secondary OBS recording output, then press **Ctrl+C** in the capture
terminal. Leave the API and worker running. Capture asks FFmpeg to finalize its
current file, uploads the remaining valid closed chunks and seals the source.
It does not stop Twitch. The configured maximum duration also ends capture.

On input loss, capture opens a fresh FFmpeg attempt and retries with bounded
backoff. It stops after more than five consecutive attempts with no closed
segments. Each attempt has its own directory, so reconnection cannot overwrite
previous chunks. Closed media remains recoverable if the backend or network fails.

Resume an interrupted capture with the same saved source, index, title and timing:

```powershell
npm.cmd run capture:live -- --resume "C:\path\to\printed-workspace" --creator-authorized
```

To upload pending closed footage and finish without reopening the receiver:

```powershell
npm.cmd run capture:live -- --resume "C:\path\to\printed-workspace" --finish --creator-authorized
```

Repeat `--finish` after jobs reach Storage to clean up remaining durable local
copies. Completed/known jobs are reused. Failed or needs_review jobs require
inspection through the existing job endpoints; ambiguous provider submissions
are not silently repeated. A PID lock prevents two capture producers from using
the same workspace. An old lock is reclaimed only when its process is gone.

The CLI retains URLs only in process memory. For HLS, set a private environment
variable (or add it to the existing `.env`), then pass its **name**:

```powershell
npm.cmd run capture:live -- --creator-authorized --input-env LIVE_INPUT_URL --max-seconds 3600
```

`LIVE_INPUT_URL` must contain the HTTPS HLS playlist URL. Its value is not printed
or persisted in the workspace. Renew an expired signed URL in the same environment
variable before resuming. HLS capture starts near the live edge, so provider
buffering/latency affects which footage is initially available.

## Disk, boundaries and timestamps

- Completed capture files live in `<workspace>/chunks/chunk_0000.mp4`, etc.
  Attempt-local files and FFmpeg's completion CSV live under `attempt_0000/`.
  `live-session.json` preserves file hashes, times, attempts and accepted job IDs.
  Its serialized save queue surfaces each failed write and allows later saves
  to run, including the interruption checkpoint. A failed write is not success.
- Corrupt, oversized or missing raw segments are recorded in a persistent
  `quarantined` ledger and their completion-list positions advance only with that
  saved state. Existing files are copied to `<attempt>/quarantine/` before the
  ledger is saved; originals are removed afterward. Resume skips those entries,
  preserves their timeline gaps and assigns sequential IDs to later valid footage.
  Quarantine copies count toward the spool budget and need deliberate retention.
  Already-accepted chunk/job mappings are never quarantined or renumbered.
- Only rows fully written to FFmpeg's completion CSV are candidates for intake.
  An open file, a partial CSV row or an unfinalized crash fragment is never indexed.
- A local completed copy is pruned only after the backend reports its private
  Storage checkpoint. Its bytes are checked against the saved hash before removal.
  `--keep-local` retains those copies. The worker independently manages staging.
- At most four not-yet-stored jobs are submitted ahead. Capture and upload run
  separately, allowing capture to spool while uploads catch up. The spool limit
  defaults to 1,000 MB; change it with `--max-spool-mb 500`, for example.
  Reaching the soft limit stops capture and retains closed footage; finalization
  can temporarily add another segment. It never deletes unconfirmed footage to
  make space. Workspace manifests and small crash fragments need later retention.
- Final/disconnected fragments under four seconds are retained in their attempt
  folder and recorded in `skipped`, but are not indexed. Already-indexed previous
  minutes are not altered to append a tail. There is no intentional overlap.
- Within a connection, timestamps come from the segment manifest. Across
  reconnects, connection start is estimated from FFmpeg progress and the host
  clock. If a crash occurred before that estimate was saved, resume uses the
  saved attempt start as a fallback. Gaps remain explicit, but this is not exact
  synchronization with Twitch's VOD clock. Clock drift and input buffering matter.
- Timeline zero is the original capture command's start, including initial wait.
  If the Twitch stream was already running then, supply its elapsed time using
  `--source-offset-seconds 7200`, for example. There is no Twitch API time sync yet.
- Maximum duration is an active capture-process budget: 5 seconds–12 hours
  (default one hour). Downtime between runs does not count; input waits and
  reconnects during a run do. The separate source timeline retains its 12-hour,
  720-chunk limit. At capacity, extra raw rows remain local while accepted work
  drains/seals. See [the exact retry/timing policy](reliability.md).

## API extension

The existing ingestion endpoints now support both modes. A live creation body is
`{mode:"live", id, title, indexId, sourceOffsetSeconds, sourceFingerprint}`;
it deliberately has no fixed `sourceDurationSeconds`.

Live chunk POSTs additionally require `X-Chunk-Start-Seconds` and
`X-Chunk-End-Seconds`. Filename and ordinal remain canonical and sequential;
intervals must be nonoverlapping, 4–65 seconds long and within 12 hours. The
database checks these under the source row lock and commits acceptance and
enqueue atomically. A repeated ordinal must retain its bytes and interval.
Sealing rejects new footage while allowing identical upload retries. Existing
recorded-source planning and validation remain supported.

## Verification and file purposes

```powershell
npm.cmd test
npm.cmd run build
npm.cmd run live:verify
```

The live verification publishes 68 seconds of synthetic RTMP footage in real time,
using the normal 60-second chunk size and a temporary Supabase owner, native queue
and private objects. It checks incremental indexing, search, boundary replay and
repeatable finish, then cleans up its own resources. TwelveLabs calls are mocked;
this consumes no video indexing/analysis allowance. It does use a small amount of
Supabase Storage/network traffic. Existing archive footage is not targeted.

Tests also cover real HLS input and an RTMP disconnect/reconnect with explicit
gaps, using shorter test-only segments. They do not establish real Marengo
accuracy, actual OBS encoder behavior, or reliability of a 30–60-minute session.
Run that creator-authorized rehearsal before recording the hackathon demo.

| File | Purpose |
| --- | --- |
| `src/live-segmenter.ts` | Run the loopback RTMP/HLS FFmpeg input and publish only finalized CSV-listed media. |
| `src/live-capture.ts` | Manage live workspaces, timestamps, reconnects, upload backlog, durable pruning and resume/finish. |
| `src/session-writer.ts` | Serialize session saves while allowing a later save after an earlier write failure. |
| `scripts/capture-live.ts` | Parse CLI options and handle capture shutdown without stopping Twitch. |
| `src/ingestion-types.ts` | Add live-source creation and database row contracts. |
| `src/ingestions.ts` | Persist live sources and report latest/continuous coverage and gaps. |
| `src/ingestion-routes.ts` | Validate live creation and completed chunk intervals through the existing intake API. |
| `src/ingestion-client.ts` | Send live source/chunk metadata and inspect stored-job progress. |
| `src/app.ts` | Report stage-eight backend capability without claiming a capture process is active. |
| `supabase/migrations/202610310006_live_capture.sql` | Add live mode and atomic dynamic-interval acceptance/sealing while preserving applied migrations. |
| `scripts/live-verify.ts` | Exercise real-time RTMP and hosted queue/Storage with synthetic footage and mocked indexing. |
| `test/live-database.test.ts` | Check live identity, gap/ordinal rules, deduplication, sealing and private permissions. |
| `test/live-segmenter.test.ts` | Check real RTMP/HLS capture, incremental file completion and safe input/CSV handling. |
| `test/ingestion-flow.test.ts` | Extend the API/worker tests through real RTMP reconnect, private search/replay and pruning. |
| `test/session-writer.test.ts` | Verify recovery and serialization after an actual atomic-file write error. |
| `package.json` | Add `capture:live` and `live:verify` commands. |
| `README.md`, `docs/live-capture.md` | Explain the implemented pipeline, OBS setup, operations, limits and file roles. |
