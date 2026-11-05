# Hosted deployment and capture onboarding

The Docker files package a **single-host deployment**: one HTTPS EBS, one worker
process with indexing and media loops, and a shared persistent media volume.
Supabase continues to provide PostgreSQL/Storage/Queues and TwelveLabs provides
retrieval/analysis. This is not a claim of production load testing or an actual
cloud deployment. Docker/Caddy must be tested on the chosen host.

For the hackathon, the existing Windows app plus an HTTPS tunnel to port 3001 is
still sufficient. Hosted mode is optional and does not require Mux or forwarding
the primary Twitch broadcast through Rewindr.

## Hosted onboarding flow

1. Enable hosted mode on the EBS after configuring the developer app/Extension.
2. In Twitch Extension configuration, the broadcaster chooses **Generate capture
   credential**. The EBS verifies the broadcaster JWT, fetches that exact channel's
   public Helix identity and provisions or reuses its unique internal owner.
3. The EBS returns a random capture credential once. PostgreSQL stores only its
   SHA-256 hash. It expires after 30 days; rotation immediately invalidates the
   previous credential. Revocation/disconnect prevents new authenticated requests.
4. The streamer saves the credential and EBS origin on their capture computer.
   Their FFmpeg producer sends minute MP4s to narrow `/agent` endpoints over HTTPS.
   No Supabase service key or TwelveLabs API key is needed on that computer.
5. The EBS stages the chunks, queues indexing and exposes private search through
   the verified channel. Another channel cannot select this internal owner.

Hosted onboarding uses Twitch's signed broadcaster context, not the local OAuth
callback. The local OAuth flow remains available for a single-machine setup.

### Backend configuration

Keep the existing Supabase/TwelveLabs/Twitch values, then configure:

```dotenv
TWITCH_EBS_PUBLIC_URL=https://rewindr.example.com
TWITCH_EBS_PORT=3001
EBS_DOMAIN=rewindr.example.com
HOSTED_ONBOARDING_ENABLED=true
HOSTED_MAX_CHANNELS=1
TWITCH_CHANNEL_ALLOWLIST=YOUR_NUMERIC_TWITCH_CHANNEL_ID
TWELVELABS_INDEX_ID=YOUR_PREPARED_MARENGO_INDEX
SUPABASE_EXPORT_BUCKET=rewindr-exports
```

Use an existing Marengo 3.0 index with visual/audio enabled. Hosted clients can
upload only into that backend-assigned index; they cannot name a different one.
Search still scopes requests to the channel's registered asset IDs. No automatic
index creation or ambiguous paid provisioning retries occur during onboarding.

The default hosted capacity is one linked channel, counting existing local links.
Raise it deliberately (maximum 100) if serving others. The optional numeric-ID
allowlist controls who may onboard; an empty allowlist permits any broadcaster
within capacity. Existing credentials must be revoked or the channel disconnected
to revoke an already onboarded user. Per-channel daily quotas are described in
[the operations guide](clips-publishing-operations.md).

## Build and run on a Docker host

Use a DNS hostname pointing to the host, with inbound ports 80/443 available.
Provide the project-root `.env` securely on that host. It is mounted as runtime
environment data, never copied into the image. Run database/bucket setup from a
trusted machine with the configured CA certificate:

```powershell
npm run db:migrate
npm run storage:init
npm run media:init
```

From the project root on the Docker host:

```text
docker compose --env-file .env -f deploy/compose.yaml config --quiet
docker compose --env-file .env -f deploy/compose.yaml up --build -d
docker compose --env-file .env -f deploy/compose.yaml ps
```

Avoid printing the expanded Compose configuration: it contains runtime environment
secrets. The provided `--quiet` command validates syntax without dumping values.

The EBS binds inside its container at 3001; that port is not published directly.
Caddy is the only public ingress and obtains HTTPS certificates for the hostname.
The normal local upload/admin API on port 3000 is not deployed/exposed. The EBS
checks Host, exact allowed browser origins, JWTs/credentials and resource ownership.
`/health` reports process liveness; `/ready` additionally checks current database
and private source/export bucket access. Caddy preserves the public Host header.
See [Caddy reverse proxy documentation](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy).

`deploy/Dockerfile` builds public frontend configuration into Extension assets,
then runs the backend as a non-root user with system FFmpeg. `.dockerignore`
uses an allowlist so `.env`, `.local`, source videos, reports and existing
`node_modules` never enter the build context. The service filesystem is read-only
apart from the persistent media volume and bounded temporary directory. See
[Docker's Node.js guide](https://docs.docker.com/guides/nodejs/).

The shared `rewindr-data` volume is mandatory. Do not run workers against a queue
whose upload files live on another unshared disk. Use an isolated Supabase project
for a separate deployment, or migrate all pending work and its persistent files
together. Running an unrelated cloud worker against your current local queue can
claim jobs whose source files are only on your laptop.

The hosted EBS takes the same staging PID lock as the local API. Stop the local
API before enabling hosted ingestion on the same `DATA_DIR`. Keep one ingestion
service per shared directory. Multiple worker processes can use database leases,
but this deployment has not been benchmarked for many simultaneous channels.
Allow disk headroom for queued uploads and up to 250 MB of downloaded clip input
plus encoded output per active media worker. Default ingestion allowance is 4 GB
per channel/day; size the persistent volume and retention policy accordingly.

For schema changes, apply additive migrations before restarting updated services.
Back up PostgreSQL and the media volume, monitor queue failures and disk space,
and pin tested container image digests before operating beyond the hackathon.
Runtime database commands inside a container need their CA certificate supplied
separately; it is deliberately not baked into this image.

## Capture computer setup

Install the normal Rewindr Node/FFmpeg dependencies. Save only these hosted-client
values in its `.env`:

```dotenv
REWINDR_API_URL=https://rewindr.example.com
REWINDR_AGENT_TOKEN=THE_ONE_TIME_CREDENTIAL_FROM_EXTENSION_SETUP
TWELVELABS_INDEX_ID=THE_INDEX_ID_RETURNED_BY_SETUP
DATA_DIR=.local/rewindr-capture
```

Keep the credential out of terminal arguments, URLs, screenshots and source control.
The capture client rejects HTTP remote origins, paths embedded in the origin,
missing credentials and redirect-based credential forwarding. Saved workspaces
bind to the hosted origin and owner, so resuming with another channel/server fails
before uploading. Credential rotation for the same owner is supported. Existing
local sessions remain local; use a fresh workspace when switching destinations.

Then run the existing capture command:

```powershell
npm run capture:live -- --creator-authorized --title "Rewindr live demo" --max-seconds 3600
```

OBS still sends its main stream directly to Twitch. Its separate FFmpeg recording
output feeds the producer's local RTMP listener as described in
[the OBS guide](live-capture.md). The producer uploads completed chunks over HTTPS
to the hosted EBS; no public RTMP port is necessary. The prerecorded
`ingest:recording` command uses the same credential path when those environment
values are present. Remove both hosted variables to use the local API again.

## Twitch asset setup and rehearsal

Rebuild the Extension using the intended public origin/client ID. Configure
`config.html`, `live.html`, `panel.html`; allowlist the EBS for fetches and the
Supabase project for media. The existing ZIP instructions in [the Twitch guide](twitch.md)
still apply. A changed origin or client ID requires a rebuild and updated assets.

Manually verify: onboard the correct channel, ingest an authorized clip, search
with a short unchanged question, review assessments/recovery, trim across a minute
boundary, download an MP4, Publish/Hide from another tab and view the panel from
an allowed viewer account. Rotate/revoke the credential and confirm uploads stop.
Rehearse the creator's 30–60-minute stream and measure actual ingestion lag and
retrieval success; synthetic tests do not establish those numbers.

## Optional scheduled retention

First inspect `npm run retention -- --days 14` and choose a policy. Only when
deletion of qualifying data is intended, enable the optional profile:

```text
docker compose --env-file .env -f deploy/compose.yaml --profile retention up -d
```

That profile queues cleanup across hosted owners daily and applies a 14-day archive
horizon plus expired-export cleanup. It keeps the worker responsible for durable
deletion and retains blocked/failed work for inspection. Provider assets and local
capture originals remain outside this retention action; account for them separately.
Do not enable the profile merely to test whether Docker starts.

The build agent does not deploy infrastructure, upload Twitch assets, create
developer credentials, enable this destructive profile or run a real OBS stream
as part of implementing these files. Those actions require the actual configured
host/channel and an end-to-end rehearsal.
