# Chunk search backend — October 30, 2026

The backend now searches already-indexed, straight one-minute chunks together.
Existing upload, reference-image search, playback and clip routes remain. No new
dependencies are needed; existing npm commands load `.env` using Node.

Order 3 also supports private stored-chunk replay. See [Storage setup and replay](storage.md)
for the upload CLI, signed manifests, preview page and each new file's role.
The main React website is still on the original full-recording workflow.
Order 4 queues Supabase chunk uploads to a separate worker; start API and worker
with the same DATA_DIR. See [durable jobs and worker commands](queue-worker.md).
Order 5 adds [automatic recorded-file ingestion](ingestion.md); newly generated
chunks enter this search API only after indexing is ready. An ingestion with no
ready chunks returns an honest pending search with an empty result list.

## Register existing footage

Start the backend with `npm.cmd run dev`. In a second terminal in the project:

```powershell
npm.cmd run register:chunks -- --manifest chunks/manifest.csv --index-id YOUR_INDEX_ID --title "Rewindr straight-cut test"
```

The output includes `broadcastId`, status/search URLs and replay availability. This
reads the CSV's measured timestamps and the existing index's filenames, IDs,
durations and readiness. It never uploads, re-indexes or deletes footage. Use
creator-authorized footage. For ambiguous filenames, send a JSON import request
with explicit `indexedAssetId` values.

Both the original headerless FFmpeg CSV and `filename,start_seconds,end_seconds`
CSV are accepted. Plain MP4 filenames may include Unicode, parentheses, apostrophes
and commas; CSV fields containing commas must be quoted. Paths, control characters,
reserved Windows device names and malformed CSV quoting are rejected. Matching
preserves the exact filename rather than silently normalizing it.
Overlap beyond a 0.1-second muxing tolerance, duplicate names/IDs,
unready assets, invalid intervals and duration mismatches are rejected. Individual
intervals must be positive and at most 65 seconds. Up to 2000 indexed assets are
supported. Records persist in PostgreSQL when Supabase is configured. Explicit local mode uses DATA_DIR/broadcasts, default `.local/rewindr/broadcasts`.
Repeated registration creates a new local broadcast, not a new provider asset.

## Search

Replace YOUR_BROADCAST_ID with the returned ID:

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

The initial question is not rewritten. Defaults are visual-only, OR, lexical plus
semantic transcription, and 30 raw matches. Choose visual for appearance/actions,
transcription for speech, audio for non-speech sound/music; the index must support
selected modalities. OR/AND combines modalities, not individual words.
Transcription options are sent only when transcription is selected.

One provider request searches the registered IDs in that index. `candidateLimit`
(3–50) controls the raw pool. `moments` preserves ranks and exposes chunk-local,
recording-relative and original-broadcast-relative times. `encounters` returns up
to three groups, never padded with fake matches. `moreCandidatesAvailable` reports
whether the pool contained additional groups. `analysisStatus: "not_implemented"`
means Pegasus has not run.

Recording time = manifest start + chunk-local match time. Original broadcast time
adds `sourceOffsetSeconds`; use `--source-offset-seconds 7200` for an excerpt from
two hours into the broadcast. That offset does not change seeking within the
excerpt. Upload order and filename numbering never determine timestamps.

Adjacent matches within three seconds are grouped, capped at 90 matched seconds.
Grouping spans continuous chunk boundaries but stops across known gaps. Thirty
seconds of context is added for replay. Context/grouping does not verify encounter
boundaries; increasing the pool does not establish better retrieval accuracy.

## Optional replay

Add `--source-recording-id YOUR_EXISTING_RECORDING_ID` when registering to attach
the same full recording already uploaded through the existing app. Its timeline
must start at the same zero as the chunks. The server checks local validated-media
metadata and duration, but cannot prove equal-duration videos contain the same
footage. Playback uses the existing byte-range route and recording-relative times.

Alternatively upload the registered chunks through `upload:chunks`; stored
encounters get a preview-page URL and a signed playback manifest, including
cross-chunk segments. See [the Storage guide](storage.md). Without stored chunks
or a local source, `playbackUrl` is null and `replayAvailable` is false. Registration
alone does not copy media. The main React website is not connected to these routes.

## Routes and boundaries

| Method | Route | Purpose |
| --- | --- | --- |
| POST | `/broadcasts/import` | Register ready chunks; require `X-Creator-Authorized: true`. |
| GET | `/broadcasts/:id/status` | Return manifest, coverage and registration-time readiness verification. |
| POST | `/broadcasts/:id/search` | Search chunks; return up to three candidates. |

Import JSON requires `title`, `indexId`, and `chunks` containing `filename`,
`startSeconds`, `endSeconds`, optional `indexedAssetId`; optional top-level fields
are `sourceRecordingId` and `sourceOffsetSeconds`. Body limit is 1 MiB.
`searchableThroughSeconds` is contiguous coverage from second zero, checked at
registration; it is not a live freshness monitor.

The existing `/recordings/:id/search` also accepts modalities, operator and
transcriptionOptions while preserving its original `limit` field and defaults.
The old reference-image route stays visual-only. Indexed streams now uses
session-based broadcast routes with assessments and opt-in recovery; Upload
recording retains the original full-recording workflow. See
[orders 6–7](search-analysis-recovery.md) for current UI and session contracts.

This is a localhost-only prototype with configurable PostgreSQL metadata or local
JSON, not a public deployment. Private chunk Storage/replay is implemented;
durable upload jobs and automatic FFmpeg ingestion of completed recordings are
implemented. Pegasus assessments and recovery are implemented in orders 6–7.
Local RTMP/existing-HLS capture is implemented in [order 8](live-capture.md).
Twitch Extension authentication/UI is implemented in [order 9](twitch.md);
automatic Mux provisioning remains future work.

## Verification

All 30 local tests and backend compilation passed on October 30, 2026. Tests use
synthetic media and mock provider HTTP responses. They cover measured timestamps,
offsets, boundaries/gaps, three candidates, invalid manifests, source scoping,
persistence, replay availability and the installed SDK's pagination and modality
serialization. Two stale legacy assertions were updated to existing behavior.
Follow-up tests cover safe Unicode/punctuation filenames, quoted CSV values and
the actual multipart reference route: both field orders, image-only search,
provider-failure cleanup, rejected input, and upload limits. Non-multipart
reference requests return 415, and missing credentials are rejected before upload.
No live TwelveLabs calls, uploads, indexing or retrieval-quality evaluation ran.

References: [search API](https://docs.twelvelabs.io/v1.3/api-reference/any-to-video-search/make-search-request),
[indexed assets](https://docs.twelvelabs.io/v1.3/api-reference/index-content/list).
