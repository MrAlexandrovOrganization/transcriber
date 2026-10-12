# Container workers and per-machine configuration

The application and checkout are the same on every machine. The ignored `.env`
selects the role, worker profiles, models, credentials and resource limits.
There is no native transcription process or SSH tunnel in the normal container
installation: `make ps` and `make logs` show both workers and their SSH sidecar.
Docker Engine/Colima/Docker Desktop still needs to be running.

## Choose a machine configuration

| Machine | Example | Role | Profiles | Processes |
| --- | --- | --- | --- | --- |
| Mac | `.env.mac.example` | `workers` | `cpu` | One CPU worker and an SSH tunnel container |
| WSL | `.env.wsl.example` | `workers` | `cpu,gpu` | Independent CPU and CUDA workers and one SSH tunnel container |
| Server | `.env.server.example` | `server` | unset | Existing PostgreSQL, ingress and fallback worker |

Copy the relevant example to `.env` once, then edit its machine-specific values.
Do not overwrite an existing `.env`. `TRANSCRIBER_ROLE=server|workers` and the
server's `WORKER_API_ENABLED=0|1` must be unquoted assignments at the beginning of
their lines. Make reads only these selectors, never sources the file as shell
code. Compose reads the remaining values. `ENV_FILE=/path/to/file` can select an
alternative file explicitly. Environment/Make overrides take precedence.

For example, the WSL configuration includes:

```dotenv
TRANSCRIBER_ROLE=workers
COMPOSE_PROJECT_NAME=transcriber-workers
COMPOSE_PROFILES=cpu,gpu
CPU_WORKER_ID=wsl-cpu
CPU_MODEL=large-v3
CPU_COMPUTE_TYPE=int8
CPU_THREADS=4
CPU_LIMIT=4
CPU_MEMORY_LIMIT=5500m
GPU_WORKER_ID=wsl-gpu
GPU_MODEL=small
GPU_COMPUTE_TYPE=int8_float16
GPU_THREADS=2
GPU_CPU_LIMIT=1.5
GPU_MEMORY_LIMIT=3g
```

`COMPOSE_PROFILES=cpu`, `gpu`, or `cpu,gpu` selects the worker containers.
At least one worker profile is required. Each worker processes one task at a
time. Thread counts and CPU quotas are separate settings. Limits on container
RAM do not limit VRAM. After changing models, limits or profiles, run `make up`;
`make restart` only restarts existing containers with their existing configuration.
Successful `make up` also removes any CPU/GPU container disabled by the new
profiles. It waits up to 180 seconds for readiness after building/starting.

The Mac example uses `CPU_MODEL=large-v3`, two threads and a two-CPU quota.
It is Linux/ARM64 CPU inference; Metal/MLX is not enabled. The WSL GPU example
keeps `small` as the existing production default. To select turbo, prepare its
cache and set `GPU_MODEL=large-v3-turbo` explicitly. `large` is currently a
faster-whisper alias for `large-v3`; explicit names make the choice clearer.

Worker IDs must match the coordinator's `PREFERRED_WORKER_IDS` if they should
block server fallback. Mac, WSL CPU and WSL GPU have equal priority. Selection
does not currently route jobs by model or guarantee equal recognition quality.

## Runtime prerequisites and private files

- Docker Engine with Compose v2 and BuildKit. On Mac, a Linux/ARM64 engine such
  as Colima or Docker Desktop; allocate enough VM RAM for model + other services.
- For WSL CUDA, a working NVIDIA WSL driver and GPU-enabled Docker runtime:
  Docker Desktop WSL integration, or Docker Engine with NVIDIA Container Toolkit.
  A native `nvidia-smi` check alone does not validate container GPU access.
- `WORKER_UID` and `WORKER_GID` match the host user (`id -u`, `id -g`). They
  allow non-root workers and the SSH sidecar to read protected bind mounts.
- `MODEL_CACHE_PATH` points to the Hugging Face cache root, e.g.
  `/home/your-login/.cache/huggingface`, containing `hub/models--...`. It is
  mounted read-only at `/models`. Workers use offline mode and do not download.
  Prepare each selected model beforehand, with its `refs/main`, or set the
  model variable to its snapshot directory inside `/models/hub/.../snapshots/SHA`.
- `WORKER_TOKEN_PATH` is a protected bearer-token file matching the coordinator.
- `SSH_IDENTITY_PATH` is a protected SSH key; prefer a dedicated key restricted
  to forwarding `127.0.0.1:50055` on the coordinator.
- `SSH_KNOWN_HOSTS_PATH` contains the server host key verified through an
  already trusted channel. The sidecar requires strict host-key checking.
- `SSH_HOST`, `SSH_USER`, `SSH_PORT` choose the actual SSH endpoint. The
  sidecar deliberately does not depend on the host's SSH alias/config/agent.

Use absolute host paths. Private files remain outside Git, owned by the selected
UID and mode `0600`, with protected parent directories. `.env` stores paths,
not key/token contents. Missing bind sources fail rather than becoming empty
directories. The tunnel forwards only inside its container network namespace;
workers share that namespace and use `127.0.0.1:50055`. No worker API port is
published on the device and no insecure remote gRPC exception is needed.

CPU and CUDA targets share the same Python application and Poetry lockfile.
The CUDA target adds cuBLAS/cuDNN wheels pinned only in `versions.mk`. The SSH
target contains OpenSSH and the small forwarding entry point. Containers are
non-root with dropped capabilities, read-only root filesystems and bounded
temporary storage/logs. Model cache is persistent and read-only; temporary audio
is disposable. The coordinator remains responsible for durable jobs/results.

## Lifecycle and readiness

```sh
make config-check
make up
make ps
make logs
make restart
make down
```

`make up` builds the selected images and recreates containers as needed. Repeating
it is safe. `make down` stops the selected project including disabled worker
profiles, without deleting model caches or server volumes. Worker examples use
`COMPOSE_PROJECT_NAME=transcriber-workers`, separate from an existing local
`transcriber` server stack; keep project names stable across updates.
Local Make builds disable default timestamped provenance attestations so a
cached rebuild does not change image IDs and restart unchanged workers. This
can be overridden with `BUILDX_NO_DEFAULT_ATTESTATIONS=0` when publishing images
with provenance; it is unrelated to application readiness.

Docker status `running` means the process is alive. A worker becomes `healthy`
after loading its model and successfully registering with the coordinator.
Availability reports refresh an atomic local readiness timestamp; the healthcheck
requires a successful report within 45 seconds. Failed reports/shutdown remove
it. Readiness remains valid while inference is busy. The tunnel healthcheck
only checks its local listener; worker readiness checks the full gRPC connection.
An unhealthy container is not automatically restarted by Docker's restart policy;
the worker already retries coordinator connectivity. Registry TTL is 90 seconds.

The `unless-stopped` policy restarts crashed workers while the engine is up.
Sleep/offline devices stop reporting; pending work stays in the durable queue.
Interrupted attempts recover after their lease expires. Containers cannot keep
an asleep/offline device processing. See [priority and leases](worker-priority.md).

## Server configuration and CI

The existing server role is selected by default for backward compatibility.
`WORKER_API_ENABLED=1` keeps the loopback worker API overlay enabled on normal
`make up`. `SERVER_WORKER_MODEL`, `SERVER_WORKER_COMPUTE_TYPE`,
`SERVER_WORKER_CPU_THREADS`, `SERVER_WORKER_CPU_LIMIT` and
`SERVER_WORKER_MEMORY_LIMIT` configure fallback inference independently.
Existing volumes and network aliases are preserved.

CI invokes `make check`, isolated PostgreSQL integration tests, `make build` and
`make build-workers`. Worker builds use synthetic example paths and do not load
models, connect to production or require a GPU. A CUDA image build does not
confirm CUDA inference; validate that on the target WSL runtime separately.
The deployment job continues to call `make up` on the configured VM after Git
update. Each target's ignored `.env` controls what that same command starts.
CI does not yet deploy to sleeping personal devices or publish images to a
registry. Those delivery policies can be added independently of this interface.

## Migrate an existing native worker

1. Provision `.env` and files, build images, and validate model/inference with
   synthetic audio outside the real queue. Keep the native worker running until
   the container prerequisites succeed.
2. Check for in-flight work. Stop the reviewed native installation using
   `scripts/mac_worker.py stop` or `scripts/wsl_worker.py stop`.
3. On Mac, archive the two reviewed LaunchAgent plist files outside
   `~/Library/LaunchAgents` so they cannot return at next login. WSL's stop
   disables its target; retain its files for rollback.
4. Run `make up`, inspect `make ps`/logs and confirm fresh availability for the
   expected worker IDs in the coordinator database. Avoid duplicate native and
   container workers with the same configured ID.
5. If startup fails, stop the container project and restore/start the archived
   native installation. Keep tokens, model caches and durable server data.

Official references: [Compose GPU access](https://docs.docker.com/compose/how-tos/gpu-support/),
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).
