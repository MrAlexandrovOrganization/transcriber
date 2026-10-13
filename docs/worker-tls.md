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
The listener accepts only individual credentials; there is no shared-token listener.

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
WORKER_CREDENTIALS_PATH=/home/your-login/.config/transcriber/worker-credentials.json
```

Start Caddy's stack first to create `caddy-transcriber-net`. Set its private
`TRANSCRIBER_WORKERS_DOMAIN` to a DNS name pointing to the server. Caddy handles
certificate issuance/renewal. Application authentication stays in WorkerService.
Its access/error logs must omit request/response objects including Authorization.

Worker `.env`:

```dotenv
TRANSCRIBER_ROLE=workers
REMOTE_WORKER_ENDPOINT=workers.example.com:443
CPU_WORKER_TOKEN_PATH=/home/your-login/.config/transcriber/cpu.token
GPU_WORKER_TOKEN_PATH=/home/your-login/.config/transcriber/gpu.token
```

Keep existing COMPOSE_PROFILES, models, worker IDs, cache paths, quotas and
autodeploy settings. Only enabled roles need a token file. Run `make up` to
apply transport/configuration changes. Each selected worker has a separate
ordinary Docker network namespace and token mount. No ports are published.

## Upgrade and rollback

Server deployments use `compose/server.yml` plus `compose/worker-tls.yml` when
`WORKER_TLS_ENABLED=1`. Mac/WSL use `compose/workers.yml`, plus
`compose/workers.autodeploy.yml` when image updates are enabled. Run the unchanged
Make commands from the project root. Keep project names, volumes, cache paths,
worker IDs and individual credential files unchanged.

The retired SSH overlay, shared-token loopback listener and native machine
launchers have been removed. Remove obsolete `WORKER_API_ENABLED`,
`WORKER_TRANSPORT` and `SSH_*` selectors from machine `.env` files.
`WORKER_GRPC_ADDR`/`WORKER_TOKEN_FILE` on ingress now fail startup rather than
silently accepting obsolete authentication. Workers reject plaintext connections
outside loopback, even if an old insecure override is set. Loopback plaintext
with scoped credentials remains available only for isolated tests/pilots.

Topology and updater bootstrap changes require updating the checkout and
running `make up` on each device. Stop the updater and drain inference workers
with `SIGUSR1`; wait for `/tmp/worker-drained` before replacing containers.
The worker finishes its current task and renews its lease while draining.
Never force a busy replacement; a disconnected attempt otherwise waits for
lease expiry and consumes a retry. Resume a retained drained worker with
`SIGUSR2` if the upgrade is aborted.

Rollback uses a known good Git revision and approved image digests **with TLS
and individual credentials retained**. The image updater already restores the
previous container if a new image fails readiness. Do not restore the retired
SSH topology. Protect backups of configuration/credentials outside Git and
preserve database, job, model and updater-state volumes.

## Checks

`make check` includes scoped credential validation and synthetic Compose checks.
`make test-integration` verifies identity/lease isolation using a disposable
PostgreSQL schema. `make test-worker-tls-docker` exercises real Caddy TLS/HTTP2,
authentication, audio download, heartbeat, completion and rejection of untrusted
certificates with an isolated synthetic store; it does not run inference or
contact the production queue. CI runs these checks before release promotion.
