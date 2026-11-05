# Order 9: Twitch account connection and Extension UI

Rewindr now has a local Twitch account-link tab, a separate authenticated
Extension Backend Service (EBS), and three packaged Twitch views. The Live Config
view reuses the existing top-three search, private chunk replay, asynchronous
match details and opt-in **Clip not found?** recovery.

Orders 10–12 now add private cross-chunk MP4 exports, explicit Publish/Hide and a
viewer panel that reads only the approved replay. See [clips and operations](clips-publishing-operations.md)
and [hosted deployment/onboarding](deployment.md). EventSub, chat and automatic
Twitch VOD time synchronization remain optional future work.

## Start here: register two different things

You do need a Twitch Developer Application for this implementation. The OAuth
application and Extension have different client IDs and secrets; do not swap them.

1. Open the [Twitch Developer Applications console](https://dev.twitch.tv/console/apps)
   and register an application named Rewindr (or an available variant).
2. Add this exact OAuth redirect URL: **`http://localhost:3000/twitch/callback`**.
   Select the appropriate application category, then create it and use Manage to
   obtain its Client ID and generate a Client Secret. Choose a confidential
   server application if the console asks for client type.
3. Save the application's values as `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`
   and `TWITCH_REDIRECT_URI` in the project-root `.env`.
4. Separately open the [Extensions console](https://dev.twitch.tv/console/extensions),
   create a **Panel** Extension/version, and obtain its **Extension** Client ID
   and shared secret. Save those as `TWITCH_EXTENSION_CLIENT_ID` and
   `TWITCH_EXTENSION_SECRET`. Copy the shared secret in its original base64 form.

Twitch requires a verified email and two-factor authentication for developer
registration. Client secrets belong only in the backend environment. See
[Twitch's registration instructions](https://dev.twitch.tv/docs/authentication/register-app/).

For local OAuth, keep the redirect host and port identical in `.env` and the
Twitch console. If you change `PORT`, change the redirect port too. The local
link handler redirects from the 127.0.0.1 alias to the configured callback host
before setting its browser cookie, so either dashboard URL can begin the flow.

```dotenv
TWITCH_CLIENT_ID=your_oauth_application_client_id
TWITCH_CLIENT_SECRET=your_oauth_application_client_secret
TWITCH_REDIRECT_URI=http://localhost:3000/twitch/callback
TWITCH_EXTENSION_CLIENT_ID=your_extension_client_id
TWITCH_EXTENSION_SECRET=your_extension_base64_shared_secret
TWITCH_EXTENSION_PREVIOUS_SECRET=
TWITCH_EBS_PUBLIC_URL=https://your-ebs-host.example
TWITCH_EBS_PORT=3001
TWITCH_EXTENSION_ORIGINS=
```

These are placeholders, not working credentials. Supabase mode and the existing
Supabase/TwelveLabs settings are still required. Never prefix backend secrets
with `VITE_` or put them in Extension HTML/JavaScript. Only the Extension client
ID, EBS public URL and local dashboard URL are compiled into the frontend.

## The two servers and complete pipeline

```text
OBS main broadcast ------------------------------------> Twitch viewers
  |
  + separate recording/output -> local RTMP capture -> FFmpeg minute segments
       -> local API :3000 -> private Supabase Storage + PostgreSQL + Queues
       -> worker -> TwelveLabs upload/index -> ready-only searchable broadcast

Local browser -> API :3000 -> Twitch OAuth -> verified channel-to-owner link

Twitch Live Config iframe -> Extension Helper broadcaster JWT
  -> HTTPS reverse proxy -> EBS :3001 -> verify signature/role/channel/expiry
  -> look up that channel's verified internal owner
  -> scoped search repository + TwelveLabs -> three private candidate cards
  -> signed Supabase replay + queued Pegasus details + optional recovery
```

**Expose only port 3001 through HTTPS.** Port 3000 remains loopback-only and
contains local upload, capture, administration and full-recording endpoints.
The EBS does not proxy those endpoints. An HTTPS reverse proxy/tunnel must
preserve the original public Host header matching `TWITCH_EBS_PUBLIC_URL`.
It must forward Authorization and Origin headers and OPTIONS requests, avoid
logging Authorization headers, and avoid caching private responses. Use a stable
HTTPS origin for a demo. Changing it requires updating `.env`, rebuilding the
Extension, restarting the EBS and updating Twitch's allowlists.

The default flow is a single-machine capture/demo architecture. The OAuth callback deliberately
returns to that machine's loopback dashboard. Other streamers can use the new opt-in, JWT-verified hosted onboarding and
credential-based capture producer; sharing this local API is not that service.

## Build and start

From the project root, after saving `.env`:

```powershell
npm run db:migrate
npm run media:init
npm run twitch:check
npm run build
npm run extension:check
```

`twitch:check` reports missing setting names without displaying credentials.
With no Twitch credentials the ordinary local application still starts. An
unconfigured Extension build shows setup instructions rather than mock results.

Run each persistent process in its own terminal:

| Terminal | Command | Role |
| --- | --- | --- |
| 1 | `npm start` | Local dashboard, account linking and ingestion API on 3000. |
| 2 | `npm run worker` | Durable indexing and candidate-analysis jobs. |
| 3 | `npm run twitch:ebs` | JWT-protected Extension service on loopback 3001. |
| 4 | Your HTTPS proxy/tunnel pointing at `http://127.0.0.1:3001` | Makes only the EBS reachable by the Twitch iframe. |
| 5, for live video | Your `npm run capture:live -- ...` command | Receives the independent OBS copy; see [live setup](live-capture.md). |

For a prerecorded demo, use an already registered/indexed broadcast instead of
starting live capture. Account linking itself does not ingest Twitch footage.
Mux is not required for this path.

Open `http://localhost:3000`, select **Twitch**, choose **Connect Twitch**, and
authorize the same channel that will host the Extension. Returning to Rewindr
shows the verified channel and its current live/offline status. Your existing
data stays under the configured `REWINDR_OWNER_ID`; linking maps that owner to
the verified Twitch channel. Disconnect first before changing channels.

## Configure the Extension version

Use these relative asset paths:

| Console setting | Value |
| --- | --- |
| Type | Panel |
| Panel Viewer Path | `panel.html` |
| Config Path | `config.html` |
| Live Config Path | `live.html` |
| Panel height | `300` for the replay panel; verify the layout inside Twitch |
| Local Test / Testing Base URI | `https://your-ebs-host.example/extension-ui/` |

The EBS serves the built assets at `/extension-ui/`. All three HTML entry points
include Twitch's official Extension Helper. Live Config is the private search
surface; its pop-out is useful for reviewing footage. A directly opened HTML
page outside Twitch cannot supply authentic broadcaster authorization.

In the developer console's content allowlists, add:

| Allowlist | Value for this implementation |
| --- | --- |
| URL Fetching Domains | Your exact HTTPS EBS origin. |
| Media Domains | Your Supabase project origin, such as `https://PROJECT.supabase.co`, for signed chunk video. |
| Image Domains | The domain serving the channel's profile image if not already allowed by Twitch. |
| Config URLs | The local Rewindr dashboard URL for its external setup link, where permitted in testing. You can always open the dashboard manually in another tab. |

Rewindr does not need viewer identity linking, Bits, chat permissions or a Twitch
Configuration Service required-version gate for this phase. Leave those disabled;
the backend independently blocks search until the verified channel link exists.
If you host test assets on another HTTPS origin, add that exact origin to
`TWITCH_EXTENSION_ORIGINS` and restart the EBS. The default allowlist already
includes the EBS origin and `https://EXTENSION_CLIENT_ID.ext-twitch.tv`.

These allowlists control Twitch's iframe CSP. CORS configuration in Rewindr does
not override Twitch's media/fetch restrictions. See
[Twitch's content restrictions](https://dev.twitch.tv/docs/extensions/#restrictions-on-content).

Install and activate the test Extension on the same channel, then open Live
Config. Refresh streams, choose your indexed broadcast and search. A different
channel is unlinked until connected by the local setup flow; a viewer or moderator
does not gain archive access by opening the private page URL.

### Hosted Test ZIP

After public IDs/URLs are configured and the build succeeds:

```powershell
New-Item -ItemType Directory -Force .local | Out-Null
Compress-Archive -Path .\dist\extension\* -DestinationPath .\.local\rewindr-extension.zip -Force
```

The ZIP root must contain `config.html`, `live.html`, `panel.html` and `assets/`;
do not ZIP the containing `extension` directory. Upload this ZIP in the version's
Files area and transition to Hosted Test. Keep the EBS, worker and capture/API
running for your demo. Test accounts can be allowlisted without a public release.
See [Twitch's Extension lifecycle](https://dev.twitch.tv/docs/extensions/life-cycle/).

A successful local build is not Twitch installation, review approval or public
release. Real OAuth consent, Twitch iframe rendering/CSP, live token refresh,
signed video playback and the demo channel still need an end-to-end rehearsal
after you register the app and Extension.

## Authentication and data ownership

The OAuth flow is a short identity-link handshake, not a retained user-token
session: one-use hashed state, browser-bound HttpOnly cookie, ten-minute expiry,
server-side code exchange, `/validate`, then Helix Get Users. The backend verifies
the app and user identity, saves channel metadata, attempts token revocation and
discards the access/refresh tokens. No user OAuth token is stored in PostgreSQL,
browser storage or Extension assets. Provider failures leave the link unverified;
start Connect Twitch again after the failure is resolved.

Helix live status uses a separate server-memory app token, obtained through the
client-credentials flow. New app tokens are validated before use, revalidated
at least hourly while retained, and replaced on an unauthorized response. A
live-status failure does not grant access or mark an unverified account linked.

Extension access uses a different token: `Twitch.ext.onAuthorized` supplies the
current short-lived JWT. The frontend retains it in memory and sends it as a
Bearer header; the EBS verifies HS256 with the base64-decoded shared secret,
expiry, broadcaster role and consistent channel identity. Frontend decoding is
only for interface messages. Every private request re-reads the channel link,
so disconnect immediately prevents subsequent archive requests. Already issued
signed replay URLs remain usable until expiry (normally five minutes); disconnect
cannot retract bytes or URLs already received by an authorized browser.

There are no browser-role grants to the Twitch link/state tables or RPCs. The
service key stays backend-only. EBS bodies cannot select an internal owner.
Local full-recording URLs are removed from Extension results; replay uses only
scoped signed chunk manifests. Extension search sessions are not persisted in
third-party localStorage. The viewer panel calls only `/viewer/selection`, never private archive APIs.
Current/previous Extension secrets can be configured during rotation; remove
the previous secret once Twitch's old JWTs have expired. See the
[Extension Helper and JWT reference](https://dev.twitch.tv/docs/extensions/reference/).

## Endpoints and file purposes

| Endpoint | Purpose |
| --- | --- |
| Local `GET /twitch/status` | Connection state and optional Helix live status. |
| Local `GET /twitch/connect` | Start browser-bound OAuth on the canonical local host. |
| Local `GET /twitch/callback` | Consume state, verify identity and save the channel link. |
| Local `POST /twitch/disconnect` | Remove the link and outstanding OAuth states. |
| EBS `GET /extension/account` | Authenticated broadcaster's channel/link state. |
| EBS `GET /extension/broadcasts` | Up to 100 broadcasts belonging to that linked owner. |
| EBS `POST /extension/broadcasts/:id/searches` | Exact initial question and three candidates. |
| EBS `GET /extension/searches/:id` | Scoped search state and assessment polling. |
| EBS `POST /extension/searches/:id/analyses` | Queue match details for the displayed version. |
| EBS `POST /extension/searches/:id/recover` | Opt-in faithful variants and session-only exclusions. |
| EBS `POST /extension/searches/:id/close` | Close a scoped private search. |
| EBS `GET /extension/broadcasts/:id/playback` | Sign only the owning broadcast's stored chunk range. |

| File | Purpose |
| --- | --- |
| `src/twitch-config.ts` | Validate separate OAuth and Extension settings without printing secrets. |
| `src/twitch-api.ts` | OAuth exchange/identity verification, token revocation and app-authenticated Helix stream status. |
| `src/twitch-store.ts` | Typed backend access to channel links and one-use OAuth state RPCs. |
| `src/twitch-local-routes.ts` | Loopback account-link/status/disconnect routes. |
| `src/twitch-jwt.ts` | Verify broadcaster Extension JWT signatures and identity claims. |
| `src/twitch-ebs.ts` | Narrow, CORS-controlled, channel-scoped search/replay service and static Extension assets. |
| `src/twitch-server.ts` | Start that EBS separately on loopback port 3001. |
| `src/app.ts`, `src/server.ts` | Wire local account routes and preserve localhost protection around ingestion/admin APIs. |
| `supabase/migrations/202610310010_twitch_identity.sql` | Add private identity/state tables, uniqueness rules and restricted RPCs. |
| `web/src/TwitchAccount.tsx` | Local connection tab, channel identity and live status. |
| `web/src/main.tsx`, `web/src/style.css` | Add the dashboard tab and its layout. |
| `web/src/BroadcastSearch.tsx`, `web/src/SegmentPlayer.tsx` | Reuse existing search/replay with injected authenticated transport and optional local session persistence. |
| `web/extension/config.html` | Install-time channel configuration entry point. |
| `web/extension/live.html` | Private broadcaster live controls entry point. |
| `web/extension/panel.html` | Read-only selected-replay viewer entry point. |
| `web/extension/main.tsx` | Helper authorization, current-token transport and role-aware views. |
| `web/extension/style.css` | Compact Extension layouts. |
| `web/extension.vite.config.ts` | Build relative assets and inject only public configuration into `dist/extension`. |
| `scripts/twitch-check.ts` | Report missing setup names without exposing values. |
| `scripts/check-extension-build.ts` | Check all three built HTML entry points, relative asset paths and accidental inclusion of configured backend credential values. |
| `scripts/twitch-verify.ts` | Check real PostgreSQL/PostgREST identity state and EBS scoping with disposable synthetic owners and no paid model calls. |
| `test/twitch.test.ts` | Exercise JWT rejection, OAuth state/identity, role/channel isolation, replay access and disconnect. |
| `.env.example`, `package.json` | Document settings and provide build/check/EBS/verification commands. |

## Verification and troubleshooting

`npm test` covers the full existing suite plus the Twitch tests. Tests use
simulated Twitch HTTP responses and signed test JWTs, not real user credentials.
`npm run twitch:verify` additionally checks the configured real PostgreSQL and
PostgREST service permissions with temporary owners and removes their dependent
records afterward. It makes no Twitch/TwelveLabs calls or Storage uploads.

If the Extension returns 401, check its client ID, base64 shared secret, clock,
latest Helper token and secret rotation. A 403 may indicate a non-broadcaster
role, absent channel link, disallowed origin or wrong reverse-proxy Host. For
an empty archive, confirm capture and OAuth use the same internal owner. For
video blocked by CSP, allowlist the Supabase origin as a Media Domain. For OAuth
cookie/state errors, restart Connect Twitch using the configured local host.
