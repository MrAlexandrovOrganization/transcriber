# Container workers and per-machine configuration

The application and checkout are the same on every machine. The ignored `.env`
selects the role, worker profiles, models, credentials and resource limits.
Workers connect directly to Caddy with gRPC/TLS; no SSH sidecar or native
transcription process is required. `make ps` and `make logs` show workers and
the optional autodeploy controller.
Docker Engine/Colima/Docker Desktop still needs to be running.

## Choose a machine configuration

| Machine | Example | Role | Profiles | Processes |
| --- | --- | --- | --- | --- |
| Mac | `.env.mac.example` | `workers` | `cpu` | One CPU worker |
| WSL | `.env.wsl.example` | `workers` | `cpu,gpu` | Independent CPU and CUDA workers |
| Server | `.env.server.example` | `server` | unset | PostgreSQL and ingress; opt-in fallback profile |

Copy the relevant example to `.env` once, then edit its machine-specific values.
Do not overwrite an existing `.env`. `TRANSCRIBER_ROLE=server|workers` and the
server's `WORKER_TLS_ENABLED=0|1` must be unquoted assignments at the beginning of
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
CPU_THREADS=2
CPU_CONCURRENCY=2
CPU_LIMIT=4
CPU_MEMORY_LIMIT=5500m
GPU_WORKER_ID=wsl-gpu
GPU_MODEL=large-v3-turbo
GPU_COMPUTE_TYPE=int8_float16
GPU_THREADS=2
GPU_CPU_LIMIT=1.5
GPU_MEMORY_LIMIT=3g
REMOTE_WORKER_ENDPOINT=workers.example.com:443
CPU_WORKER_TOKEN_PATH=/home/your-login/.config/transcriber/cpu.token
GPU_WORKER_TOKEN_PATH=/home/your-login/.config/transcriber/gpu.token
```

`COMPOSE_PROFILES=cpu`, `gpu`, or `cpu,gpu` selects the worker containers.
At least one worker profile is required. Each container loads one model and runs
`CPU_CONCURRENCY` / `GPU_CONCURRENCY` execution slots (default 1, range 1–16).
Each slot processes one task at a time. CPU threads are per slot; CPU quotas
and RAM limits cover the whole container. Limits on container
RAM do not limit VRAM. After changing models, limits or profiles, run `make up`;
`make restart` only restarts existing containers with their existing configuration.
Successful `make up` also removes any CPU/GPU container disabled by the new
profiles. It waits up to 180 seconds for readiness after building/starting.

The Mac example uses `CPU_MODEL=large-v3`, two slots of two threads and a four-CPU quota.
It is Linux/ARM64 CPU inference; Metal/MLX is not enabled. The WSL GPU example
uses `large-v3-turbo`; prepare its cache before starting. `large` is currently a
faster-whisper alias for `large-v3`; explicit names make the choice clearer.

Worker IDs must appear in coordinator `WORKER_POLICIES` when priorities are
enabled. The server example puts WSL CPU before Mac CPU, WSL GPU and server
fallback. Configure pool caps there as well as actual slot counts on devices;
the effective capacity cannot exceed either. An empty policy retains legacy
`PREFERRED_WORKER_IDS` behavior. See [priorities and shared-model slots](worker-priority.md).
Selection does not route jobs by model or guarantee equal recognition quality.

## Runtime prerequisites and private files

- Docker Engine with Compose v2 and BuildKit. On Mac, a Linux/ARM64 engine such
  as Colima or Docker Desktop; allocate enough VM RAM for model + other services.
- For WSL CUDA, a working NVIDIA WSL driver and GPU-enabled Docker runtime:
  Docker Desktop WSL integration, or Docker Engine with NVIDIA Container Toolkit.
  A native `nvidia-smi` check alone does not validate container GPU access.
- `WORKER_UID` and `WORKER_GID` match the host user (`id -u`, `id -g`). They
  allow non-root workers to read their protected model/token mounts.
- `MODEL_CACHE_PATH` points to the Hugging Face cache root, e.g.
  `/home/your-login/.cache/huggingface`, containing `hub/models--...`. It is
  mounted read-only at `/models`. Workers use offline mode and do not download.
  Prepare each selected model beforehand, with its `refs/main`, or set the
  model variable to its snapshot directory inside `/models/hub/.../snapshots/SHA`.
- `CPU_WORKER_TOKEN_PATH` and `GPU_WORKER_TOKEN_PATH` select separate protected
  bearer-token files for enabled roles. Their hashes map to exact worker IDs in
  the coordinator's private credential registry. Disabled roles need no token.
- `REMOTE_WORKER_ENDPOINT` is the Caddy worker hostname and port (normally 443).
  TLS and certificate/hostname verification are enabled in normal Compose mode.
  Each worker uses its own ordinary Docker network namespace, with outbound
  connectivity and no published ports. Credentials never grant SSH access.

Use absolute host paths. Private files remain outside Git, owned by the selected
UID and mode `0600`, with protected parent directories. `.env` stores paths,
not key/token contents. Missing bind sources fail rather than becoming empty
directories. Bind propagation is explicitly `rprivate`, including for credential
files, for compatibility with the tested Docker Desktop WSL integration.
See [TLS and per-worker credentials](worker-tls.md) for provisioning, rotation,
revocation and upgrades. TLS is the only supported remote transport.

CPU and CUDA targets share the same Python application and Poetry lockfile.
The CUDA target adds cuBLAS/cuDNN wheels pinned only in `makefiles/versions.mk`. Containers are
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

`make up` builds local images or resolves approved images in autodeploy mode,
and recreates containers as needed. Repeating
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
it. Readiness remains valid while inference is busy. Worker readiness checks the
full gRPC connection through Caddy.
An unhealthy container is not automatically restarted by Docker's restart policy;
the worker already retries coordinator connectivity. Registry TTL is 90 seconds.

The `unless-stopped` policy restarts crashed workers while the engine is up.
Sleep/offline devices stop reporting; pending work stays in the durable queue.
Interrupted attempts recover after their lease expires. Containers cannot keep
an asleep/offline device processing. See [priority and leases](worker-priority.md).

## Server configuration and CI

The existing server role is selected by default for backward compatibility.
`WORKER_TLS_ENABLED=1` enables the Caddy network and scoped Worker API.
`SERVER_WORKER_MODEL`, `SERVER_WORKER_COMPUTE_TYPE`,
`SERVER_WORKER_CPU_THREADS`, `SERVER_WORKER_CPU_LIMIT` and
`SERVER_WORKER_MEMORY_LIMIT` configure fallback inference independently.
Existing volumes and network aliases are preserved.

CI invokes `make check`, isolated PostgreSQL integration tests, `make build` and
`make build-workers`. Worker builds use synthetic example paths and do not load
models, connect to production or require a GPU. A CUDA image build does not
confirm CUDA inference; validate that on the target WSL runtime separately.
The deployment job deploys the exact checked SHA with `make deploy` on the VM.
Each target's ignored `.env` controls what the standard commands start.
With `WORKER_AUTODEPLOY=1`, Mac/WSL inference containers follow approved public
GHCR releases through a local updater container, including after being offline.
See [automatic deployment, drain and rollback](worker-autodeploy.md).

## Upgrading existing deployments

Update the bootstrap checkout to the approved revision and use `make up`.
Keep `COMPOSE_PROJECT_NAME`, models, token paths and cache paths unchanged.
Compose files now live under `compose/`; Make anchors all relative paths to the
checkout root. Remove obsolete `WORKER_API_ENABLED`, `WORKER_TRANSPORT` and
`SSH_*` settings from the machine's `.env`. Remove `WORKER_GRPC_ADDR` and
`WORKER_TOKEN_FILE` from custom ingress environments: startup rejects those
retired settings. Do not remove active per-worker token files.

Old SSH launchers/Compose overlays are no longer shipped. Existing native
LaunchAgents/systemd units must already be disabled before this upgrade; container
and native installations must never run with the same worker ID. The current
supported lifecycle is Docker Compose on every device.

Official references: [Compose GPU access](https://docs.docker.com/compose/how-tos/gpu-support/),
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).
