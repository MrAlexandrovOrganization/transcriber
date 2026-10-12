# Worker automatic deployment

## Release flow

Push to `main` starts `.github/workflows/ci.yaml`:

1. `make check`, isolated PostgreSQL/RPC integration tests and image builds.
2. Native amd64 and arm64 runners call `make publish-workers`. CPU supports
   both architectures; CUDA supports amd64. Images carry the tested Git SHA.
3. The VM deploys that exact SHA with a clean working copy and `make deploy`;
   ingress/PostgreSQL must become healthy. Deployment never fetches an untested
   newer `main` through a plain `git pull`.
4. Only after both publication and VM deployment succeed, `make promote-workers`
   publishes one `transcriber-release:stable` marker containing immutable CPU/GPU
   manifest digests and the approved SHA. A failed pipeline cannot promote it.
   Workflow concurrency serializes releases.
5. Each enabled device's `worker-updater` container checks this marker every
   minute. An offline device catches up to the latest approved release after
   Docker starts again. No inbound SSH or GitHub runner is needed on the device.

Public images live under `ghcr.io/mralexandrovorganization/`:
`transcriber-worker-cpu`, `transcriber-worker-gpu`, `transcriber-release`.
GitHub creates new packages private even for a public repository: an organization
owner must make all three packages public once in GitHub Packages settings.
Anonymous pulls are then sufficient; no registry tokens are stored on devices.
Source code, dependencies and OCI revision are public. Models, audio, `.env`,
credentials and local caches are excluded from published images.

## Bootstrap and normal commands

Requires Docker Compose >=2.24.4 (`!reset` support), Docker API >=1.45,
Python 3 on the host for `make worker-release`, prepared local model cache,
and the existing `.env` paths to token and SSH credentials.

In each Mac/WSL `.env` enable:

```dotenv
TRANSCRIBER_ROLE=workers
WORKER_AUTODEPLOY=1
# Keep this machine's existing COMPOSE_PROFILES, model names and limits.
```

Run `make up` once from the current checkout. It resolves the approved release
into ignored `.worker-release.env`, pulls ready-made CPU/GPU images and builds
only the small tunnel/updater bootstrap images. The SSH tunnel remains local
because its user must match the machine's credential-file owner.

Afterward normal worker-code releases need no command on the device.
`make ps` shows the updater and workers; `make logs` includes update decisions.
`make down` first stops the updater, then stops the selected stack without deleting
volumes. `make up` resolves the newest release again; it cannot accidentally use
an old `.worker-release.env` to downgrade an automatically updated worker.

Profiles, worker IDs, models, cache paths and CPU/RAM limits remain local `.env`
configuration and are copied to replacements unchanged. Editing these settings
requires `make up` on that device. Tunnel/updater code, Compose topology and
protocol changes require updating the bootstrap checkout and `make up` as well;
this first version automatically replaces **CPU/GPU inference containers**.
The server's permanent worker is deployed by the existing VM workflow.

## Tasks, failures and recovery

The updater pulls the image before asking a worker to drain (`SIGUSR1`). The
worker completes its current inference, continues renewing its lease and sends
Complete/Fail; then it stops claiming, withdraws availability and creates an idle
marker. Only that marker permits replacement. `SIGTERM` keeps its existing
bounded stop semantics for normal operator shutdown; a lost lease is recoverable
from the durable queue.

Updates happen one worker at a time. A busy worker may drain for up to one hour;
if it is still busy, drain is cancelled (`SIGUSR2`) and the update retries later.
An updated worker must pass its normal model/coordinator readiness check within
240 seconds. Otherwise its stopped predecessor is restored. The failed digest
is quarantined for that worker until a different release arrives. The updater's
named state volume contains an atomic recovery journal; an interrupted rollout
is completed if the replacement is healthy, or rolled back on updater restart.
Do not remove the updater state volume or rollback container during a rollout.
Old image layers are retained for rollback; periodically inspect Docker disk
usage and remove unused images while no rollout is active.

A registry outage leaves existing workers running. Loss of the coordinator can
prevent a new version becoming healthy and cause rollback; the durable queue
still owns jobs/results. Health/readiness success does not assert identical
transcription quality between model versions. Backward-compatible RPC/schema
changes remain required during mixed-version operation.

The updater mounts the Docker socket, which grants control over the Docker
engine. Its code only accepts labeled `worker-cpu`/`worker-gpu` containers from
its exact Compose project, and release digests from the configured GHCR prefix;
this allowlist is application protection, not a Docker privilege boundary.
It exposes no network listener and never logs container configuration or API
error bodies. Do not mount credentials or unrelated host paths into it.

## Checks

`make check` validates both Compose modes and fake-Docker recovery tests.
Drain tests use fake RPC/inference and synthetic data. CI additionally checks
real PostgreSQL and RPC behavior against an isolated temporary database.
A real Docker update/rollback smoke must use a distinct synthetic project and
must never inject jobs into the production queue.
