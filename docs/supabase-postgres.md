# Supabase PostgreSQL metadata

This stage replaces local JSON metadata with PostgreSQL. Video files and extracted
clips remain under DATA_DIR. Order 3 adds private Storage for registered chunks;
see [the storage guide](storage.md). Order 4 adds [durable jobs and a separate
worker](queue-worker.md). Order 5 adds [recorded-file ingestion](ingestion.md),
source/chunk checkpoints and ready-only broadcast views. [Order 9](twitch.md)
adds verified Twitch channel linking and a separately authenticated Extension backend.

## Configuration

Set these backend-only values in `.env`, never in frontend VITE variables:

```dotenv
SUPABASE_URL=https://YOUR_PROJECT_REF.supabase.co
SUPABASE_SECRET_KEY=YOUR_BACKEND_SECRET
SUPABASE_DB_URL=YOUR_POSTGRESQL_CONNECTION_STRING
SUPABASE_DB_CA_PATH=.local/rewindr/supabase-ca.crt
```

SUPABASE_URL and the backend secret drive the runtime Data API client. Use an
API project root such as `https://PROJECT_REF.supabase.co`, without `/rest/v1`.
The SDK adds the REST path itself. Use an
`sb_secret_...` key; legacy service-role keys remain compatible, while publishable
keys are rejected. The SQL connection is used for migration and live verification,
not normal HTTP requests. The migration connection and API must target the same
project. Keep all passwords in `.env`.

The migration driver validates TLS certificates and hostnames. Download the CA
from the project's Database Settings > SSL Configuration and set its path. No
certificate validation is disabled. Connection-string SSL options are removed
before applying the driver's explicit verified TLS configuration, so `sslmode`
cannot override the selected CA. Relative certificate paths resolve from the
project working directory. The certificate stored under `.local` is gitignored.

Without an explicit PERSISTENCE_MODE, the server selects Supabase when Supabase
settings are present. Partial/broken configuration fails rather than switching to
JSON. `PERSISTENCE_MODE=supabase` explicitly selects the database; `local` explicitly
selects the old JSON implementation for offline work. Switching modes does not
copy or synchronize records.

REWINDR_OWNER_ID defaults to `00000000-0000-4000-8000-000000000001`, a stable local
development owner. Keep it stable when importing/restarting. It is server-selected,
never accepted from request bodies, and is not an authenticated Twitch identity.
The ingestion HTTP API remains localhost-only. The separate Extension service
resolves verified Twitch channels to this internal owner before every archive request.

## Schema

| Table | Purpose |
| --- | --- |
| public.rewindr_broadcasters | Internal owner; verified Twitch association is kept in rewindr_twitch_links. |
| public.rewindr_twitch_links | Unique verified Twitch-channel-to-internal-owner mapping. |
| public.rewindr_twitch_oauth_states | Hashed, browser-bound, expiring one-use connection state. |
| public.rewindr_recordings | Full-recording metadata, source hash, processing state and provider checkpoints. |
| public.rewindr_broadcasts | Owner, index, title, excerpt offset and optional local source recording link. |
| public.rewindr_chunks | Ordered filenames, measured source times and indexed asset IDs for each broadcast. |
| public.rewindr_jobs | Durable job identity, attempts, leases, checkpoints and terminal state. |
| public.rewindr_workers | Queue-worker heartbeat/liveness timestamps. |
| public.rewindr_ingestions | Source identity, duration, offset and input-completion state. |
| public.rewindr_ingest_chunks | Stable source intervals, file hashes and fenced Storage/provider checkpoints. |
| rewindr_private.schema_migrations | Applied migration names and content hashes; not a public data API table. |

Typed fields enforce identities, valid phases and time intervals, unique mappings,
and owner-consistent foreign keys. Nested media/upload/error details use JSONB.
Source files are referenced by the existing recording ID and local DATA_DIR path.
Order 3 adds bucket, content hash, byte count and upload time beside storage_path.
These remain null until upload; signed URLs are never stored. The attachment RPC
checks ownership and media identity; metadata saves preserve attachments and reject
removing or remapping stored chunks.
Ingestion chunks have stable local UUIDs before a provider asset exists. Their
private references and provider IDs live in validated checkpoint JSONB. The
broadcast read function combines legacy chunks with indexed-ready ingestion chunks
in one snapshot. Normal legacy metadata saves cannot remap ingestion broadcasts.

All metadata and job tables have RLS enabled and revoke anon/authenticated/public
permissions. No browser policies are enabled. Backend service keys bypass RLS,
so repository reads and writes explicitly use the server's owner scope as well.
The Extension backend verifies Twitch identity and scopes requests through the
channel link; [hosted onboarding](deployment.md) now provisions separate channel
owners behind an explicit enable flag, capacity limit and optional allowlist.

`rewindr_save_broadcast` atomically saves a parent and all chunks. Invalid times,
overlap or duplicates roll back the operation, including replacement of an existing
mapping. `rewindr_get_broadcast` reads parent and ordered chunks in one snapshot,
without truncating at the Data API's table row limit. Only service_role may execute
these functions. They run with invoker privileges, not SECURITY DEFINER.

## Apply migrations

From the project directory, with dependencies installed:

```powershell
npm.cmd run db:migrate
```

This project's migration runner uses versioned SQL files in `supabase/migrations`
and its own private history table. It is not `supabase db push`. Do not mix migration
runners without reconciling their histories. The runner uses a transaction and an
advisory lock, checks hashes of applied files, and never resets the database.
Create a new migration instead of editing an already-applied file. The application
tables are prefixed; an unexpected existing table causes failure rather than being
silently adopted or overwritten. After commit, PostgREST is notified to reload.

Database commands require SUPABASE_DB_URL. Normal runtime needs only the API URL
and backend secret after the schema exists. Errors suppress connection secrets.

## Import the existing local metadata

Stop the API first. The command refuses to run while its local PID lock is active.

```powershell
npm.cmd run db:import-local
npm.cmd run db:import-local -- --apply
```

The first command is a dry run. The second imports missing records and broadcasts.
It preserves IDs, hashes, created/source timestamps, and provider IDs. Existing
matching recordings are skipped, preserving newer remote processing checkpoints.
This is a new-records-only import, not a synchronization or reconciliation tool.
If a matching remote recording has an older `updatedAt` than the local record, it
is not overwritten: `skippedStale` counts it separately and `staleRecordings`
lists its ID, local/remote phases, and update timestamps. `alreadyPresent` excludes
these older records, so the categories do not overlap. Staleness is based on
timestamps, not an assumed ordering of phases; clock differences can affect it.
Review stale entries before manually reconciling provider state. A later local
phase does not justify blindly resubmitting work or overwriting the remote row.
These fields appear in dry-run and apply output. Safe identity/mapping conflicts
and missing-source messages now include the affected IDs in CLI error output;
unexpected lower-level errors remain redacted.
Identity/mapping conflicts are checked before import writes. Interrupted imports
can be retried; each record write is atomic and each broadcast uses its transaction.
The complete archive import is not one giant transaction. Metadata write times
may update; source/encounter times do not change.

Local JSON and all video files are retained. Nothing is uploaded to TwelveLabs or
re-indexed. Source playback still needs the same local media files. A database on
another host does not make these local files available on that host.

## Verify and start

```powershell
npm.cmd run db:verify
npm.cmd run storage:init
npm.cmd run build
npm.cmd start
```

In Supabase mode run `npm.cmd run worker:start` in a second terminal, with the
same `.env` and persistent DATA_DIR. The API accepts work; the worker processes
it. Run `queue:reconcile` once for unfinished recordings created before order 4.

db:verify uses real Supabase metadata writes under a newly generated temporary
owner and a mocked video provider. It verifies registration, a new application
instance reading the database, timestamp mapping, health and owner scoping. It
removes only the temporary verification owner and its rows, retaining original
recordings. It does not exercise real video retrieval or media playback.
Supabase server mode now also requires the private chunk bucket from order 3.
Use `storage:verify` for an isolated real Storage/replay check; see the storage guide.

`GET /health` now probes the active persistence store and reports PostgreSQL vs.
local JSON explicitly. A database failure returns 503 instead of healthy or local
fallback. The `processing` object reports state, consecutive metadata failures,
retry limit/delay and any halt reason. A halted processor makes health return 503
with `status: "degraded"`, even if the database itself has recovered.

Remote metadata 503s retry the same read/write with exponential backoff: the
default is five consecutive failures maximum, waiting 1, 2, 4 and 8 seconds before
the next attempt (delay cap 30 seconds). A successful metadata operation resets
the failure count. Retries retain in-flight records and returned provider IDs;
the worker does not replay the entire indexing step. Startup metadata operations
also use this policy. Exhausting the limit halts processing until restart; local
storage errors still halt immediately. Shutdown cancels retry waits. No provider
failure state is fabricated for a storage failure.

If an upload's remote metadata write has an uncertain outcome, its
completed local media is retained for diagnosis rather than deleted.

Supabase mode now uses one local API with a PID lock and separate leased workers.
See the [queue/worker guide](queue-worker.md) for fencing and retry behavior.
Explicit local JSON mode retains its in-process processor. API and workers must
share the same persistent source/staging media directory; a remote database alone
does not make independent hosting disks safe.

The order 4 job functions are tightly scoped SECURITY DEFINER wrappers available
only to the backend service role. They couple pgmq delivery with validated job
state and reject expired lease tokens. They do not change the original metadata
RPCs' invoker privileges or add browser access. New nonterminal recording inserts,
including imports, enqueue atomically; stop workers during metadata maintenance.

## Files

| File | Responsibility |
| --- | --- |
| supabase/migrations/202610300001_metadata.sql | Tables, indexes, constraints, access restrictions and transactional functions. |
| src/persistence.ts | Async contracts shared by local and PostgreSQL persistence. |
| src/metadata.ts | Validate saved/imported records and broadcasts. |
| src/store.ts | Retained local recording JSON implementation. |
| src/local-persistence.ts | Local broadcast JSON implementation and offline factory. |
| src/supabase-persistence.ts | Supabase client setup, owner-scoped repositories and database health. |
| src/db-connection.ts | Verified SQL TLS connection and safe error reporting. |
| src/import-local.ts | Preflight and resumable local metadata import logic. |
| src/import-errors.ts | Preserve safe import diagnostics while redacting unexpected errors. |
| src/server.ts, src/app.ts, src/broadcasts.ts, src/processor.ts | Wire async persistence through startup, routes and processing. |
| scripts/db-migrate.ts | Apply hashed SQL migrations transactionally. |
| scripts/db-import-local.ts | Dry-run/apply the local metadata import. |
| scripts/db-verify.ts | Verify real Supabase persistence with isolated temporary rows. |
| test/database.test.ts | Execute real SQL constraints, rollback and access tests in embedded PostgreSQL. |
| test/persistence.test.ts | Test adapters, importer, health failures and uncertain-write file retention. |
| test/processor.test.ts | Check retry backoff/caps, recovery, retained provider IDs, shutdown and health. |

The normal test suite uses an embedded PostgreSQL engine and mock HTTP services;
it does not read `.env` or contact your hosted database. Live verification is an
explicit separate command. Check the README for the latest verification results.

## Initial verification result

On October 30, 2026, the first migration was applied to the configured hosted
project. Live metadata verification passed for save, restart/read, API timestamp
mapping, health and owner scoping. Temporary verification rows were removed.
The local import saved four existing recordings and zero local broadcasts; the
next dry run found all four already present. No video was uploaded or re-indexed.
The local build and 41 tests cover the integration and its retained fallback.
