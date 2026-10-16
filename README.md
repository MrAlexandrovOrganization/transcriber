# Transcriber backend

## Build versions

`Makefiles/versions.mk` owns Python, Poetry and PostgreSQL versions. The exact
grpcio-tools version is read from `poetry.lock`, also used for Python dependency
installation. Run `make versions` to inspect values; install that Poetry version
locally. Use `make build`/`make up`: Make exports versions to Compose build args.

Durable asynchronous Whisper transcription service.

## Compose configuration layout

All Compose files live in `compose/`. Use the root `Makefile` as the entry point;
the machine's `.env` selects the required files automatically.

| File | Purpose |
| --- | --- |
| `compose/server.yml` | PostgreSQL, ingress and server fallback worker |
| `compose/worker-tls.yml` | Server listener for Caddy and per-worker credentials |
| `compose/workers.yml` | CPU/GPU workers selected by profiles |
| `compose/workers.autodeploy.yml` | Published inference images and updater |

Make passes `--project-directory` pointing to the checkout root. Build contexts,
relative bind/secret paths, `.env` and the default Compose project name therefore
keep their existing meaning. Keep `COMPOSE_PROJECT_NAME` unchanged to reuse
existing volumes. Direct Compose invocations also need `--project-directory .`
from the root, explicit `-f compose/...` files and the required exported versions.
Prefer `make config-check`, `make build`, `make up`, `make ps` and `make logs`.
The directory move does not require editing machine `.env` files.

## Architecture

- [`whisper-ingress`](compose/server.yml) accepts gRPC uploads and returns a `job_id` quickly.
- Uploaded files are persisted to [`/data/jobs`](compose/server.yml) on a Docker volume.
- Job metadata, status, retries, and worker leases are stored in Postgres.
- [`whisper-worker`](compose/server.yml) claims jobs via Postgres using `FOR UPDATE SKIP LOCKED`.
- A worker stores the final result before marking the job `done`; durable clients
  can separately acknowledge receipt with `Ack`.
- If a worker dies before lease renewal, the job becomes claimable again and is retried.
- Mac/WSL remote workers are preferred; the permanent server worker claims only
  while all preferred workers are unavailable. See [availability and priority](docs/worker-priority.md).

## Local run

Choose the machine's configuration first: `.env.server.example`,
`.env.mac.example`, or `.env.wsl.example`. Copy the appropriate example to the
ignored `.env` and provision its paths/credentials. The same `make up`, `down`,
`restart`, `logs` and `ps` commands manage the selected stack on each machine.
See [container workers and machine configuration](docs/container-workers.md) and
[automatic worker deployment](docs/worker-autodeploy.md).
The server also provisions [transcription dashboards and metrics](docs/monitoring.md)
into the existing Grafana/Prometheus stack.
Without `TRANSCRIBER_ROLE`, the existing server deployment remains the default.

```bash
make proto
make up
```

Ingress endpoint inside Docker network: `whisper-ingress:50053`.

## Durable clients and upload retries

The first `Submit` chunk can set `request_id` and `retain_until_ack=true`.
Persist a unique request ID before upload, for example `notes-bot:<operation UUID>`.
Its allowed characters are `[A-Za-z0-9_.:-]`, with a maximum of 200 characters.
The namespace is global within this backend; each trusted caller owns its prefix.
All retries must identify the same audio bytes, format, options and retention
setting. Options conflicts return `INVALID_ARGUMENT`; the service trusts callers
to preserve audio identity and does not hash a replayed completed upload.

If the accepted response is lost, retrying `Submit` returns the existing `job_id`
without restarting inference. A simultaneous duplicate upload receives `ABORTED`;
retry with bounded backoff and the same ID. An interrupted upload remains
recoverable under the same ID. Each upload holds a PostgreSQL session advisory
lock and writes an isolated temporary file; after fsync, atomic rename publishes
the complete audio before the job becomes queued. Connection/process loss
releases the lock. Retries replace abandoned partial files, while different
request IDs upload independently. The protocol restarts an interrupted upload
from its beginning rather than resuming at a byte offset.

Queued jobs and their audio survive without any available workers. A retained
terminal job, including failure or cancellation, remains available through
`GetStatus` until the client calls `Ack(AckRequest{job_id})`. The client must first
commit the result/error and its delivery context to its own durable store.
`Ack` is idempotent while the job exists; it rejects unfinished jobs with
`FAILED_PRECONDITION`, and unknown or already cleaned jobs with `NOT_FOUND`.
After Ack, `JOB_TTL_S` starts from the acknowledgement time. Without the retention
flag, legacy cleanup continues to use the finish time. Idempotency lasts while
the job record exists, including the period after Ack before TTL cleanup.
Never reuse a request ID for a new operation. Unacknowledged retained results
have no automatic expiry, so clients must complete Ack and disk usage needs
monitoring.

Schema changes live in ordered SQL files under `whisper/sql/migrations`, tracked
by `transcriber_schema_migrations`. `0001_baseline.sql` adopts the existing
schema and preserves jobs/files; `0002_client_delivery.sql` adds only defaulted
columns and a partial unique index. All unapplied migrations and their ledger
records run in one transaction with an advisory lock. Applied files must remain
unchanged. `make migrate` applies them explicitly using the documented database
and job-storage configuration. Startup also applies them to preserve this
project's established deployment behavior; this exception avoids requiring
a simultaneous change to its startup/deployment contract.

Deploy the entire backend, including any permanent local worker, before enabling
retained submissions in clients. Older workers run legacy TTL cleanup and can
delete unacknowledged retained results. Old clients remain compatible with the
new backend. Rollback to an old backend requires disabling retained submissions
and durably receiving/Acking outstanding results first; keep the additive schema
and ledger rather than reversing migrations.

## Main environment variables

- `DATABASE_URL`
- `JOB_STORAGE_DIR`
- `JOB_MAX_ATTEMPTS`
- `JOB_LEASE_SECONDS`
- `JOB_TTL_S`
- `LOG_LEVEL` — `DEBUG|INFO|WARNING|ERROR` (default `INFO`); logs are emitted as one JSON object per line to stdout (compatible with Go `slog` JSONHandler: fields `time`, `level`, `msg`, plus `service=whisper` and `role`)
- `SERVICE_ROLE=ingress|worker`
- `WORKER_POLL_INTERVAL_S`
- `WHISPER_MODEL`
- `WHISPER_CPU_THREADS`
- `PREFERRED_WORKER_IDS`, `WORKER_AVAILABILITY_TTL_S`, `WORKER_FALLBACK`

## Remote workers

The standard Mac/WSL lifecycle is now Docker Compose configured by `.env`.
See [container workers](docs/container-workers.md) for profiles, per-worker
models and limits, prerequisites, readiness, CI and native-to-container migration.
Native machine-specific SSH launchers have been removed. Use the same container
worker configuration on Mac and WSL; CPU/GPU selection and models stay in `.env`.

The existing PostgreSQL queue remains the source of truth. Remote workers use a
separate, authenticated gRPC WorkerService to claim jobs, download audio, renew
leases and return results. They need neither database credentials nor a shared
filesystem. No worker endpoint is enabled by the normal deployment.

Start with a synthetic local experiment, using Python/Poetry at the versions
from `Makefiles/versions.mk`, Buf at `BUF_VERSION`, and Docker with the pinned PostgreSQL
image already present. Install dependencies with `make install`, then:

```bash
make pilot PILOT_AUDIO_FILE=/absolute/path/to/synthetic-speech.wav
```

The pilot starts its own disposable PostgreSQL, ingress and native CPU worker,
submits the supplied synthetic audio before starting the worker, verifies the
result, and cleans up its processes/container. It requires a cached `base`
Whisper model and forbids model downloads. It never connects to Telegram or
an existing queue/database. Missing model/image is a failed prerequisite.

## Remote workers over TLS

Normal container workers connect directly to Caddy over gRPC/HTTP2 with TLS.
Set `REMOTE_WORKER_ENDPOINT=workers.example.com:443` and a separate protected `CPU_WORKER_TOKEN_PATH`/`GPU_WORKER_TOKEN_PATH` for
each enabled role. No SSH key, tunnel or inbound laptop port is required.

On the coordinator, `WORKER_TLS_ENABLED=1` enables `compose/worker-tls.yml` and
`WORKER_CREDENTIALS_PATH` selects a private JSON registry of SHA-256 token hashes
bound to exact worker IDs. Caddy reaches the worker-only listener on the
external internal Docker network `caddy-transcriber-net`; no worker port is
published. The client-facing API stays private. Start Caddy's network first.

See [TLS provisioning and rollout](docs/worker-tls.md),
[per-machine Compose](docs/container-workers.md), and
[automatic image delivery](docs/worker-autodeploy.md).
SSH transport, the shared-token listener and their native launchers have been
removed. Only individual worker credentials are accepted. Remote native CLI
connections default to TLS; plaintext is restricted to loopback for isolated
experiments. See [upgrade and rollback](docs/worker-tls.md#upgrade-and-rollback).

Remote workers accept at most 256 MiB of audio by default. Results are limited
to 4 MiB. Existing model/preset behavior, durable queue and leases are unchanged;
model-aware routing remains a separate future change.

Production notes-bot now persists accepted voice jobs, results and delivery
checkpoints in PostgreSQL and uses idempotent note appends. It can wait for absent
workers across frontend restarts; results are retained until durable receipt.
Its implementation and guarantees are documented in that independent repository,
`docs/durable-voice.md`. [The initial runtime audit](docs/distributed-workers.md)
records the earlier deployment and is historical context.

## Checks

`make format` applies safe Ruff fixes, import ordering, Python and proto
formatting. `make check` regenerates ignored Python stubs, validates Compose and
Buf format/build/lint/breaking compatibility, checks Ruff and runs synthetic
unit tests. Integration tests are skipped unless an explicitly supplied
`TRANSCRIBER_TEST_DATABASE_URL` is set. No test downloads a model by default.

`make test-db-up` / `make test-db-down` manage an isolated temporary PostgreSQL
for tests (loopback port 55439; synthetic credentials; tmpfs). Run:

```bash
make test-db-up
TRANSCRIBER_TEST_DATABASE_URL=postgresql://pilot:synthetic-ci-password@127.0.0.1:55439/transcriber_pilot \
  make test-integration
make test-db-down
```

The suite uses a random schema per test and checks real concurrent claims,
expired leases, cancellation, stale/duplicate results, authentication and retry.
Set `TRANSCRIBER_PILOT_AUDIO_FILE` plus cached-model environment settings to
include real CPU speech inference. Proto generation keeps the established
`grpcio-tools` pipeline pinned by Poetry because it supplies the bundled Python
generator; Buf handles format/build/lint/breaking. `FILE` compatibility protects
existing generated clients; these checks do not prove semantic compatibility.
Locally, the breaking baseline defaults to the previous commit (`HEAD^`); CI
uses the PR base or previous push SHA and fails if that history is unavailable.
CI now checks and builds on PR/main before deployment; no changes are deployed
until committed and pushed through the normal project workflow.

## Container names

Compose uses explicit service names without replica numbers. Set
`CONTAINER_NAME_PREFIX` (for example `dev-`) for an additional local or CI stack
on the same Docker host. Keep Compose project/service labels and volume names
unchanged; clients address services or explicit network aliases.
