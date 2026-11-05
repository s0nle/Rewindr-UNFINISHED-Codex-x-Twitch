# Implementation status — October 27, 2026

This file is the historical Stage 1 log. Current implementation and verification
are documented in [README](../README.md), [live capture](live-capture.md),
[reliability](reliability.md) and [Twitch integration](twitch.md). Later builds
include Supabase, workers, ingestion, search recovery and the Extension backend;
the statements below describe the earlier state, not today's feature inventory.

Implemented Stage 1 only: streamed recording intake, creator authorization attestation, ffprobe validation, TwelveLabs asset upload and asynchronous indexing adapter, status tracking, visual question search, ranked moments with original source timestamps and playback, React upload/search UI, and FFmpeg clip downloads.

`npm.cmd run build` completed successfully after the UI and clip extraction were added. It compiles backend TypeScript, compiles frontend TypeScript, and bundles the React app with Vite. This establishes compilation only.

The user requested no further testing. No test suites or retrieval evaluations were run after that request. Before the request, 12 local checks passed using real ffprobe on synthetic video and a mocked search provider. One SDK HTTP fixture used the wrong report-chunks route; it was corrected to match the installed SDK, without another test run. The final UI and clip-extraction behavior remain untested.

The evaluation manifest is an explicitly unverified template, with 10 illustrative positive questions and three illustrative absent-event questions. Answer ranges are empty because no real footage was supplied. The evaluator rejects this template until a human replaces and verifies the examples.

Live diagnosis update: the user supplied credentials and footage. TwelveLabs accepted index creation, then rejected upload-session creation with HTTP 422, code `asset_storage_limit_exceeded`: account storage capacity 3,221,225,472 bytes, requested file size 3,698,141,561 bytes, current usage 0. A diagnostic session request sent only the filename and size and reproduced that response; no video was uploaded by that diagnostic. Local compression to a separate 720p copy was started to reduce file size while preserving the timeline. Provider failures now retain a safe HTTP status/code and failed operation instead of only a generic message.

Live TwelveLabs indexing and retrieval have not yet completed. The top-three 8/10 quality gate has not been evaluated or passed. No mock results are displayed by the application, and no mock results are presented as retrieval-quality evidence.

Compression completed successfully: `rewindrtest-720p.mp4`, 449,568,154 bytes, 1280×720 H.264 video and AAC audio, duration 1539.534 seconds. FFprobe read back those output properties. The original source was retained, and the new copy was not automatically uploaded. The API was rebuilt and restarted with the more specific error handling; no test suites were run.

User-reported retrieval update: the smaller recording reached `ready` and real search returned candidates. The user reported one useful candidate out of six, two appearances of the intended person, incomplete interaction coverage, and repeated automatic pauses. The attached `C:/Users/sonle/Downloads/rewindrfound.mp4` is about 16 seconds. Local low-resolution pixel alignment, followed by visual source-frame inspection, places this excerpt at approximately 854–870 seconds (14:14–14:30) in the indexed recording. This is source alignment, not proof that semantic retrieval found the right person.

Playback changes remove both the explicit `onTimeUpdate` pause at the original provider end and the end-bounded media fragment. Previews now seek into the full source and continue normally. Download ranges are independent, with context padding and controls to set their boundaries from the playhead. Search fetches 30 provider segments, groups adjacent matches, preserves raw provider ranks/timestamps, adds 30 seconds on each side, and exposes additional candidate groups instead of limiting retrieval to six segments.

The reference image `.local/search-diagnostics/hoodie-reference-cropped.jpg` was extracted from the supplied clip. Live text-plus-image and image-only requests against the existing indexed video succeeded, and their raw results are saved locally. Text-plus-image returned a grouped candidate whose padded context covers the reference excerpt; raw ranks still contain distractors. Image-only did not establish improved ranking. The UI therefore defaults to combining a supplied reference with the user's description and offers image-only as an explicit option. These observations do not establish two correct encounters, general retrieval quality, precision, recall, or a passed quality gate. No automated test suites were run. Backend/frontend compilation completed successfully; browser behavior has not been automated or manually exercised by the agent.

Supabase, a separate queue worker, ownership/authentication, Mux/Twitch, transcription, chunking, and Railway deployment have not been started.
