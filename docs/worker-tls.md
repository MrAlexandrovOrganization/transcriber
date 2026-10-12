# Worker gRPC over Caddy TLS

## Architecture

Mac/WSL workers open outbound HTTP/2 gRPC connections to the Caddy hostname on
TCP 443. Public CA and hostname verification remain enabled. Caddy forwards only
POST `/whisper.WorkerService/*` via h2c to `transcriber-worker-api:50056` on the
dedicated internal `caddy-transcriber-net`. No worker listener is published on
the host. The submission/status API, PostgreSQL and bot credentials stay private.

`ReportAvailability`, `Claim`, `Download`, `Heartbeat`, `Complete` and `Fail`
keep their existing durable queue/lease behavior. TLS changes transport only;
it does not replicate the coordinator, database or audio. No laptop inbound
port, SSH identity, host-key file or shared tunnel namespace is required.

## Per-worker credentials

Generate a different cryptographically random bearer token (at least 32 random
bytes, encoded as printable ASCII) for each worker ID. Keep the raw token only
in that worker's protected file, outside Git, mode 0600 with a protected parent.
Never put token contents in `.env`, command arguments, URLs, logs or images.

The coordinator's private `WORKER_CREDENTIALS_PATH` contains SHA-256 hashes:

```json
{
  "version": 1,
  "workers": {
    "example-cpu": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "example-gpu": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }
}
```

These hashes are synthetic examples. Hash the exact ASCII token, excluding the
file's trailing newline. This is verification of high-entropy bearer tokens,
not a password KDF. The registry validates format, duplicate fields/hashes and
worker IDs without exposing file contents in errors. It is read at startup.

A token permits only its worker ID in Claim/ReportAvailability, and only that
worker's lease tokens in Download/Heartbeat/Complete/Fail. Lease ownership and
expiry are additionally enforced by PostgreSQL. Workers share no raw secret.
The public listener never falls back to the legacy shared-token listener.

For rotation, securely provision the new token file and update its registry
hash; recreate ingress with `make restart-worker-api` in server mode, then
recreate only the affected worker. Drain active work first. A short authentication
gap is expected with this single-token-per-worker format. To revoke a worker,
remove its registry entry and recreate ingress. Other worker tokens remain valid;
lost leases follow normal expiry/retry rules. Never edit JSON using a tool that
prints its contents, and preserve registry mode 0600.

## Configuration

Coordinator `.env`:

```dotenv
TRANSCRIBER_ROLE=server
WORKER_TLS_ENABLED=1
WORKER_API_ENABLED=0
WORKER_CREDENTIALS_PATH=/home/your-login/.config/transcriber/worker-credentials.json
```

Start Caddy's stack first to create `caddy-transcriber-net`. Set its private
`TRANSCRIBER_WORKERS_DOMAIN` to a DNS name pointing to the server. Caddy handles
certificate issuance/renewal. Application authentication stays in WorkerService.
Its access/error logs must omit request/response objects including Authorization.

Worker `.env`:

```dotenv
TRANSCRIBER_ROLE=workers
WORKER_TRANSPORT=tls
REMOTE_WORKER_ENDPOINT=workers.example.com:443
CPU_WORKER_TOKEN_PATH=/home/your-login/.config/transcriber/cpu.token
GPU_WORKER_TOKEN_PATH=/home/your-login/.config/transcriber/gpu.token
```

Keep existing COMPOSE_PROFILES, models, worker IDs, cache paths, quotas and
autodeploy settings. Only enabled roles need a token file. Run `make up` to
apply transport/configuration changes. Each selected worker has a separate
ordinary Docker network namespace and token mount. No ports are published.

## Migration and rollback

1. Provision the registry and individual token files without logging secrets.
2. Deploy Caddy's route/network through its Git/CI workflow; check TLS.
3. Enable `WORKER_TLS_ENABLED=1` and deploy the coordinator through Git/CI.
   Temporarily retain `WORKER_API_ENABLED=1` for existing SSH workers. The two
   listeners have separate credentials; the legacy token cannot use public RPCs.
4. Check there are no in-flight jobs to interrupt, or drain workers with SIGUSR1
   and wait for `/tmp/worker-drained`. Stop the autodeploy controller during the
   topology change. After draining, explicitly stop the old inference containers
   before running Compose: they still reference the tunnel's network namespace.
   Update the bootstrap checkout and per-device `.env`; `make up` recreates
   workers and removes the orphan tunnel container. Keep the coordinator's
   permanent fallback worker running during this brief per-device transition.
5. Verify TLS registration, identity rejection, readiness and unchanged model
   settings on both devices. Disable `WORKER_API_ENABLED` and recreate ingress
   with `make restart-worker-api`; confirm port 50055 is no longer published.

Protect backups of `.env` and credentials outside Git. Do not delete model
caches, updater state, database volumes or unrelated SSH keys during migration.
For explicit rollback, enable the legacy loopback overlay and set worker
`WORKER_TRANSPORT=ssh`, `REMOTE_WORKER_ENDPOINT=127.0.0.1:50055`, the old token
paths and SSH settings, then `make up`. Normal TLS deployments do not use that
compatibility overlay. Changing Compose topology is a bootstrap operation;
subsequent inference-code releases still use the existing automatic image rollout.

## Checks

`make check` includes scoped credential validation and synthetic Compose checks.
`make test-integration` verifies identity/lease isolation using a disposable
PostgreSQL schema. `make test-worker-tls-docker` exercises real Caddy TLS/HTTP2,
authentication, audio download, heartbeat, completion and rejection of untrusted
certificates with an isolated synthetic store; it does not run inference or
contact the production queue. CI runs these checks before release promotion.
