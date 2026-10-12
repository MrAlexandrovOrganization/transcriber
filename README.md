# Transcriber backend

## Build versions

`versions.mk` owns Python, Poetry and PostgreSQL versions. The exact
grpcio-tools version is read from `poetry.lock`, also used for Python dependency
installation. Run `make versions` to inspect values; install that Poetry version
locally. Use `make build`/`make up`: Make exports versions to Compose build args.

Durable asynchronous Whisper transcription service.

## Architecture

- [`whisper-ingress`](docker-compose.yml) accepts gRPC uploads and returns a `job_id` quickly.
- Uploaded files are persisted to [`/data/jobs`](docker-compose.yml) on a Docker volume.
- Job metadata, status, retries, and worker leases are stored in Postgres.
- [`whisper-worker`](docker-compose.yml) claims jobs via Postgres using `FOR UPDATE SKIP LOCKED`.
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
The older native launchers below remain available for rollback/experiments;
avoid running both installations simultaneously with the same worker IDs.

For a managed native WSL GPU/CPU installation, see
[WSL worker prerequisites, setup and systemd lifecycle](docs/wsl-worker.md).
It uses the locked project `.venv`, CUDA/cuBLAS supplied by WSL, and the optional
cuDNN package pinned by `CUDNN_VERSION` in `versions.mk`; it does not install
system drivers or change Windows startup/sleep settings.

The existing PostgreSQL queue remains the source of truth. Remote workers use a
separate, authenticated gRPC WorkerService to claim jobs, download audio, renew
leases and return results. They need neither database credentials nor a shared
filesystem. No worker endpoint is enabled by the normal deployment.

Start with a synthetic local experiment, using Python/Poetry at the versions
from `versions.mk`, Buf at `BUF_VERSION`, and Docker with the pinned PostgreSQL
image already present. Install dependencies with `make install`, then:

```bash
make pilot PILOT_AUDIO_FILE=/absolute/path/to/synthetic-speech.wav
```

The pilot starts its own disposable PostgreSQL, ingress and native CPU worker,
submits the supplied synthetic audio before starting the worker, verifies the
result, and cleans up its processes/container. It requires a cached `base`
Whisper model and forbids model downloads. It never connects to Telegram or
an existing queue/database. Missing model/image is a failed prerequisite.

For a remote deployment, provision a random 32–512 character ASCII bearer token
in a protected file on the ingress and worker, outside Git. `make up-worker-api
WORKER_TOKEN_PATH=/protected/token` explicitly enables `compose.worker-api.yml`:
only the worker API is published on host loopback port 50055. The client-facing
API and database are not exposed.

To preserve this opt-in across normal CI deployment (`make up`), put these
non-secret settings in the server checkout's ignored `.env`:

```dotenv
WORKER_API_ENABLED=1
WORKER_TOKEN_PATH=/home/maxim/.config/transcriber/worker.token
```

The enable flag must be an unquoted `0` or `1`. Make reads only that flag;
Compose reads the token file path. Token contents remain exclusively in the
protected file. `make up`, `build`, `down`, `logs` and `restart` then consistently
select the worker API overlay. `make config-check` validates the selected
configuration without printing secrets. Unset/zero keeps the default stack.
After disabling the flag, run `make up` to remove the published listener from
the recreated ingress. The one-time `make up-worker-api` alias does not persist
the setting; use the ignored `.env` for subsequent deployments.

Forward the loopback listener with:

```bash
ssh -N -L 50055:127.0.0.1:50055 server
```

In another terminal on Mac/WSL:

```bash
REMOTE_WORKER_TOKEN_FILE=/protected/token \
WHISPER_MODEL=base WHISPER_LOCAL_FILES_ONLY=1 \
WHISPER_DEVICE=cpu WHISPER_COMPUTE_TYPE=int8 WHISPER_CPU_THREADS=2 \
WORKER_POLL_INTERVAL_S=60 make run-remote-worker
```

Ctrl+C stops the worker; active inference is interrupted cooperatively and its
lease eventually expires. Compose uses a 120-second job lease. Each
process claims at most one job at a time. WSL CUDA uses the same worker with
`WHISPER_DEVICE=cuda`; verify CUDA/cuBLAS/cuDNN and supported compute types first.
Mac uses CPU in this pilot; Apple GPU requires a separate inference engine.
No availability probe from the bot is needed: workers independently report
readiness while idle or busy, then poll for work. See
[Mac setup](docs/mac-worker.md) and [WSL setup](docs/wsl-worker.md).

Worker transport rejects non-loopback plaintext unless explicitly allowed with
`REMOTE_WORKER_ALLOW_INSECURE=1` for a trusted VPN. `REMOTE_WORKER_TLS=1` and
optional `REMOTE_WORKER_TLS_CA_FILE` support an externally configured TLS proxy;
the built-in listener itself is plaintext. Never publish it directly to the
Internet. Remote workers accept at most 256 MiB of audio by default
(`REMOTE_WORKER_MAX_INPUT_BYTES`); `MAX_AUDIO_BYTES` optionally limits total
uploads on ingress. Existing ingress upload behavior is unchanged unless it is
set. Serialized results are limited to 4 MiB. Only enable a remote pool for jobs
compatible with its model and limits: capability-based routing is a later step.

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
