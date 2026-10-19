# Transcriber: server to hostkey-russia

Migration date: 19 October 2026. Coordinator PostgreSQL, job audio and bot
PostgreSQL move together. WSL/Mac worker IDs, credentials and public TLS hostname
are preserved. No inference worker is enabled on either application server.

## Configuration

Russia backend `.env` uses `TRANSCRIBER_ROLE=server`, `WORKER_TLS_ENABLED=1`,
the protected existing credential registry and
`SERVER_OVERLAY=compose/hostkey-russia.yml`. Leave `COMPOSE_PROFILES` unset and
remove server-fallback from `WORKER_POLICIES`; only WSL/Mac pools remain.
Caddy uses `TRANSCRIBER_WORKER_UPSTREAM=h2c://transcriber-worker-api:50056` and
`TRANSCRIBER_WORKER_PROXY_OPTIONS=local_proxy`; CLIP/Stash keep their independent
TLS origin settings. The upload API binds only `100.64.0.5:50053`.

The bot uses Russia's shared local Telegram Bot API and its existing read-only
media volume. Set `TELEGRAM_LOCAL_API_URL=http://telegram-bot-api:8081`, an empty
`HTTPS_PROXY` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://100.64.0.1:4318` in its private
`.env`. The backend sends OTLP gRPC through the old server's tailnet port 4317.
The compatibility forwarder on server owns those tailnet-only Jaeger ports and
the existing `whisper-ingress` Docker alias for notes-bot and stash.

## Cutover

1. Prepare clean Git checkouts on Russia and move the projects' repository-level
   SSH deployment secrets there. Do not change shared organization secrets.
2. Back up private configuration, credential registry, both databases and job
   files off the VMs. Keep old images and volumes. Never publish these backups.
3. Drain the original fallback worker and wait until all active jobs finish.
   Stop bot and ingress, disable their restart policies and stop the worker.
   Take final PostgreSQL custom-format dumps and a consistent job-volume archive.
4. Restore both dumps and job files on Russia before starting either application.
   Use the same PostgreSQL major versions. Apply tested application images by
   digest through GitHub Actions; no application build runs on Russia.
5. Switch Caddy's transcriber upstream to its local worker listener. On server,
   run only `make forwarder-up`; keep original application/DB containers stopped
   with restart=no. Remove the old coordinator's metrics target; install the
   target on Russia. Verify DB counts, file hashes, public authenticated worker
   polling, client gRPC forwarding and Telegram polling with one active bot.

## Rollback

Before Russia accepts writes, stop its bot/ingress and restore the old settings,
containers, Caddy upstream and restart policies. Remove/stop the forwarder before
reusing the `whisper-ingress` alias. Do not start a second polling bot.

After Russia accepts writes, first stop both applications and snapshot its
**latest** databases and job volume back to server. Restore those together,
then switch Caddy/clients and deployment secrets. Never reactivate the original
stale volumes over newer accepted jobs or bot state. A schema/image rollback
requires compatibility review; preserve migrations and worker credentials.

Private migration backups are stored in
`/home/maxim/backups/transcriber-migration-20261019` on both servers and
`~/.local/state/transcriber-migration-20261019` on the operator's machine.
The original model cache and all original data volumes remain on server.
