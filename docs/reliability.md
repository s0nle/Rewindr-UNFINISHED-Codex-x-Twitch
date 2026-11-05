# Retry, live-budget and staging-cleanup behavior

## Failed candidate assessments

Migration 008 makes a new `/searches/:id/analyses` request requeue a failed cached
assessment. Queued/processing/ready entries remain idempotent; repeated requests
do not create additional jobs. `needs_review` entries never automatically retry.

- Transient polling failures retain the known task ID and resume that task.
- Confirmed failed/canceled tasks or ready tasks with invalid output receive
  fresh submission identities on re-request. Earlier task IDs/plans remain in
  private `attempt_history`. This can consume additional TwelveLabs usage.
- Unknown submissions remain review-only; no blind recreation.
- Each requeue resets the one-hour assessment-attempt deadline. Job identity
  remains stable and old worker lease tokens remain fenced.

The same rules apply to explicit failed analysis job retries. Old failed tasks
without terminal-outcome markers may first be polled to establish their outcome,
then need a further request before replacement.

## Search UI recovery

Search polling retries network failures, HTTP 408/429 and server failures with
backoff capped at 30 seconds. Expired/missing sessions and other permanent client
errors stop polling. Switching/unmounting a search cancels future updates.

Failed assessment requests release their requested-key marker and retry up to
three times. **Retry match details** allows an explicit retry after exhaustion
or a failed assessment. Polling never automatically recreates failed model tasks,
avoiding an endless paid-task loop. Playback and search recovery remain usable.

## Live capacity and elapsed-time decision

The budget counts **active capture-process time**, including input waits,
reconnect attempts and backoff during a run. Backend setup, backlog-only draining
and downtime between stopped runs do not consume it. `activeSeconds` is saved
periodically and at normal checkpoints. A crash can lose time since the last
checkpoint; this counter is not a billing clock.

Legacy workspaces without the counter start from the total duration of their
recorded/skipped/quarantined media; historical reconnect waits are unrecoverable.
Sealed sessions cannot be reopened as new input.

Source timestamps remain separate: wall-clock gaps and the existing 12-hour
timeline limit remain. Resume does not compress missing footage. Reaching 720
accepted chunks or the timeline limit is end-of-input: finish drains/seals
accepted work, retains extra raw files, and records `inputLimit`. No chunk/job
identity is renumbered or silently discarded.

## Full-recording provider retries

Safe upload transfers retry transient network/timeout, HTTP 408/429 and 5xx
failures with persisted progress and bounded exponential delay. Five consecutive
failures are allowed by default; successful part checkpoints reset the budget.
Restart resumes the same upload session. Permanent access errors, local
validation and unknown create/index submissions remain terminal or review-only.
Existing polling and per-part transport retries remain supported.

## Automatic staged-file cleanup

The single API process owning `DATA_DIR/api.lock` sweeps `chunk-jobs/` at startup
and every 15 minutes. It does not sweep full recordings, capture workspaces,
quarantine folders, Storage objects or the TwelveLabs index.

| Staged file | Automatic policy |
| --- | --- |
| No database reference | Eligible after 24 hours. |
| Completed job with attached Storage | Eligible after 24 hours and confirmation the object exists. |
| Failed/review ingestion with a recoverable Storage checkpoint | Eligible after seven days and remote-object confirmation; the worker can download it on retry. |
| Queued/running/retrying job | Retained. |
| Failed source without a replica, or failed legacy upload | Retained for recovery/inspection. |
| Missing reference result, metadata outage or Storage outage | Retained; a later sweep retries. |

Age uses the later of file modification and job update time. References cover
every owner and queue because workers can share the directory. Migration 009
exposes that lookup only to the backend. Unknown filenames and symlinks are
ignored; redirected staging directories are rejected.

Workers and cleanup use exclusive per-file locks. Fully written lock metadata is
published atomically. Busy workers wait without consuming their failure budget.
Only the API reclaims old locks from dead processes or released operations. The
existing single-API rule is required; do not run another cleanup process against
the same spool. Cleanup rechecks job state and file identity before removal.

Unrecoverable sources are intentionally retained rather than deleting their last
known copy. They need review and an explicit retention decision. Restart API,
worker and capture after installation. No new keys or runtime dependency is needed.

## File purposes

| File | Purpose |
| --- | --- |
| Migration `008_analysis_retry` | Requeue failed assessments with task history and renewed deadlines. |
| Migration `009_staging_sweep` | Classify references across owners with backend-only permissions. |
| `src/analysis-processor.ts`, `src/providers/pegasus.ts`, `src/search-types.ts` | Distinguish terminal/invalid output, transient polling and unknown submissions. |
| `src/request-retry.ts`, `web/src/BroadcastSearch.tsx` | Retry requests/polling and provide an explicit match-details retry control. |
| `src/live-clock.ts`, `src/live-capture.ts` | Persist active-time budgets and finish at capacity. |
| `src/processor.ts`, `src/providers/errors.ts`, `src/providers/twelvelabs.ts` | Classify transient failures and resume known recording uploads with backoff. |
| `src/staging-sweeper.ts`, `src/staged-media.ts` | Check references/replicas, coordinate file use and remove eligible copies. |
| `src/server.ts`, `src/job-worker.ts` | Schedule cleanup and protect worker file operations. |
| `test/search-database.test.ts`, `test/search-flow.test.ts` | Verify requeue, preserved IDs, fresh terminal retries and fencing. |
| `test/ui-retry.test.ts` | Verify retries, exhaustion, cancellation and permanent-error stops. |
| `test/live-state.test.ts` | Verify downtime exclusion and finishing a full workspace. |
| `test/processor.test.ts` | Verify upload budgets and retained identity. |
| `test/staging-database.test.ts`, `test/staging-sweeper.test.ts` | Verify permissions, cross-owner protection, replicas, locks and filesystem scope. |
