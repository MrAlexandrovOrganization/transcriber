# RabbitMQ delivery

This change replaces SQL job selection with RabbitMQ delivery. The public
TranscriptionService (`Submit`, `Transcribe`, `GetStatus`, `Cancel`, `Ack`) and
all existing fields retain their semantics. Audio remains in the existing
`whisper-job-data` volume; jobs, results, client request IDs and retention stay
in the existing PostgreSQL. No data volume, service identity or worker ID changes.

## Responsibilities and guarantees

An upload is fsynced and atomically renamed before its queued transition.
That transition and a new outbox record commit in the same database transaction.
The ingress's bounded background publisher sends a persistent envelope containing
only version, job ID and message ID, with mandatory routing and publisher confirms.
Only then does it commit `published_at`. Broker outages leave accepted work in
the outbox; a lost database commit may duplicate a publication.

Each loaded model retains its existing execution slots. Every slot obtains
an expiring admission from the coordinator and subscribes directly to the common
quorum jobs queue, with prefetch one. The old lower-is-better policy maps to
RabbitMQ's higher-is-better priority as `10000 - priority`: WSL CPU 10 precedes
Mac 20, which precedes GPU 30. Equal priorities compete at the same tier.
Already executing jobs are never preempted to prefer another device.

Admission counts all admitted ready slots and valid orphaned execution leases
across processes using the same worker ID. BeginAttempt also atomically checks
the pool cap and whether the individual slot is busy. Starting extra containers
cannot increase the coordinator's configured execution limit. Configure priority
only through WORKER_POLICIES; deployed worker code passes the granted priority to
RabbitMQ. Broker consumer arguments assume trusted worker software; broker ACLs
do not independently force a client's priority argument.

WorkerService adds RegisterConsumer and BeginAttempt. BeginAttempt accepts only
the delivered job/message pair, never scans for a queued job. A fresh delivery
UUID makes a retry of a lost BeginAttempt response idempotent. An unexpired
execution owned by another delivery returns busy; the consumer confirms a copy
to the five-second retry queue before acknowledging the original. The retry
queue uses quorum storage and an at-least-once dead-letter policy.

The existing 120-second job lease and progress heartbeat fence writes through
the API. They are not the broker's connection heartbeat. A disconnected worker
may continue computing briefly; expired or replaced attempt tokens cannot
complete, fail or renew another attempt. A duplicate in-flight message waits
for the live attempt; a terminal, cancelled, unknown or stale message is acked
without inference. Poison envelopes are dead-lettered.

After Complete or Fail commits successfully, the worker acknowledges the broker
delivery. Retryable Fail atomically creates the next outbox generation with
bounded exponential delay (2–256 seconds). Attempts count actual started
executions, not deliveries, capacity denials or duplicates. Existing max-attempt
and terminal cleanup rules remain. Failed RPCs or ambiguous completion leave
the message unacknowledged. Redelivery checks durable state before computing.
Delivery is at least once, with idempotent result acceptance; inference itself
can repeat after a crash. Client Ack remains separate from RabbitMQ ack.

The broker networking loop is independent of blocking gRPC and inference.
AMQP heartbeat is 20 seconds. REMOTE_WORKER_MAX_TASK_S defaults to six hours;
the provisioned broker ack timeout is seven hours. Inference must cooperate
with cancellation; a permanently hung native call may require process recovery.
SIGUSR1 drains all slots, finishes completion/failure and broker ack, closes
subscriptions, withdraws admission, then writes the existing drained marker.
SIGUSR2 resumes. SIGTERM retains bounded shutdown and unacknowledged recovery.

## Network and credentials

On Hostkey Russia, run the broker alongside ingress, on the existing whisper-net.
Expose AMQPS 5671 only on the Headscale address. Workers open outgoing TLS
connections to that address and retain their existing authenticated gRPC
connection through Caddy. No inbound worker port, database password, shared
filesystem or per-device broker is needed.

The root Makefile selects `compose/rabbitmq.yml` for server mode and
`compose/workers.rabbitmq.yml` for workers when QUEUE_BACKEND=rabbitmq.
Existing SERVER_OVERLAY and image-by-digest delivery remain in use.
The existing WORKER_GPU_RUNTIME=wsl-direct selector is retained, including the
WSL driver mounts and CUDA library wrapper across automatic image replacement.
The optional fallback also runs the remote consumer protocol; it requires its
own worker API token, RabbitMQ credentials and SERVER_WORKER_ENDPOINT.
Do not enable the stopped fallback or ASUS as part of this migration.

Provision a fresh private bundle on the coordinator, with system OpenSSL:

```sh
python3 scripts/provision_rabbitmq.py \
  --output /home/your-login/.config/transcriber/rabbitmq \
  --host 100.64.0.5 \
  --worker-id wsl-cpu --worker-id mac-m3-pro --worker-id wsl-gpu
```

The command refuses an existing destination. It creates a one-year private CA
and server certificate, hashed broker account definitions, a publisher directory
and individual worker directories. Keep the CA key on the coordinator, record
certificate expiry, and renew/distribute certificates before expiry. Never copy
the entire bundle to a worker. Transfer only that worker's connection.json and
ca.crt through the existing authenticated administration channel.

The bundle parent must remain mode 0700. The broker's server subdirectory is
0755 with readable files so the container's RabbitMQ UID can read the mount;
the private parent protects them on the host. Worker directories are 0700 and
connection files 0600; own them by the configured WORKER_UID/WORKER_GID.
The publisher can declare/bind transcriber resources and publish tasks. Workers
can read only the jobs queue and publish only to the deferred-delivery exchange;
they cannot publish arbitrary new jobs or modify topology.

Coordinator private `.env` additions:

```dotenv
QUEUE_BACKEND=rabbitmq
RABBITMQ_BIND_ADDRESS=100.64.0.5
RABBITMQ_CONFIG_DIR=/home/your-login/.config/transcriber/rabbitmq/server
RABBITMQ_PUBLISHER_DIR=/home/your-login/.config/transcriber/rabbitmq/publisher
```

Worker private `.env` additions (keep existing profiles, models, limits,
API credentials, updater configuration and concurrency unchanged):

```dotenv
QUEUE_BACKEND=rabbitmq
CPU_RABBITMQ_DIR=/home/your-login/.config/transcriber/rabbitmq/wsl-cpu
GPU_RABBITMQ_DIR=/home/your-login/.config/transcriber/rabbitmq/wsl-gpu
```

On Mac, CPU_RABBITMQ_DIR points to its own mac-m3-pro directory; GPU settings
are unnecessary unless that role is enabled. Connection files never appear
in container environment values or Compose output. TLS verifies CA and hostname.
Plain AMQP is accepted only on loopback for isolated test fixtures.

One broker preserves work across container restart but does not provide host
HA. Replicated availability requires quorum members on permanent server nodes;
worker devices and sleeping laptops should not be broker cluster members.
The RabbitMQ version is pinned in makefiles/versions.mk. Preserve
whisper-rabbitmq-data and hostname when restarting or upgrading the broker.

## Cutover and rollback

1. Back up PostgreSQL, audio and private configuration using the existing
   operational process. Inspect the real WORKER_POLICIES and device settings.
   Preserve CPU capacity 3 / Mac 2 / GPU 1 if these are the current live values;
   sample files are not authoritative. Check RAM/disk headroom for the bounded
   512 MiB broker and check VPN reachability of AMQPS.
2. Deliver this revision through Git/CI with QUEUE_BACKEND still postgres.
   Migration 0006 is additive. Both old and new single-/multi-slot workers can
   use the unchanged legacy Claim path during this phase.
3. Provision the broker bundle and distribute individual worker credentials.
   Drain current workers through the existing updater/SIGUSR1 protocol and
   verify every slot has finished. Keep receiving new uploads durably.
4. Apply the coordinator's RabbitMQ selector and recreate it from the already
   published digest using make image-deploy IMAGE_MANIFEST=.images/current.json.
   Startup adopts pending SQL jobs that lack a dispatch identity. The outbox
   publishes existing jobs and future submissions. SQL Claim now rejects new
   claims, so forgotten old workers cannot race RabbitMQ consumers.
5. Apply each worker's RabbitMQ selector and credentials, then use its ordinary
   make up with the approved worker image. This step is required once: an image
   updater alone cannot add new credential mounts to existing containers.
   Registered consumer priorities and slots come from the coordinator.
6. Verify active image revisions, TLS, admitted/connected slots, an end-to-end
   synthetic Submit/GetStatus/Ack, no duplicate execution on replay, and queue
   recovery. Expected devices may be offline; do not enable extra ones.

For rollback, drain RabbitMQ consumers, switch coordinator and workers to
QUEUE_BACKEND=postgres, and activate compatible previous images through normal
image delivery. Keep the additive schema, audio, broker volume and credentials.
Pending jobs remain discoverable by SQL claiming; retained results keep their
existing Ack contract. Before a later return to RabbitMQ, stop SQL claimers and
run make rabbit-reconcile after enabling the new coordinator. It creates fresh
outbox identities for queued/expired work without disturbing valid live attempts.
The same explicit command recovers pending work after broker storage restoration.
Never use queue purge or delete data volumes as a migration step.

## Verification and diagnostics

make check covers format/static checks, protobuf compatibility, Compose examples
in both modes and synthetic unit tests. make test-db-up and make test-rabbit-up
start only dedicated disposable fixtures, with synthetic credentials. Then run:

```sh
TRANSCRIBER_TEST_DATABASE_URL=postgresql://pilot:synthetic-ci-password@127.0.0.1:55439/transcriber_pilot \
TRANSCRIBER_TEST_RABBITMQ_FILE=.test-rabbitmq.json make test-broker
make test-broker-tls-docker
make test-rabbit-down test-db-down
```

The suite uses random schemas and broker namespaces, exercising outbox rollback,
ambiguous publication, direct subscriptions, priority order, shared-model pool
capacity, attempt fencing, redelivery, cancellation and the unchanged durable
client API. The TLS smoke uses a separate broker with generated test credentials,
tests certificate/hostname verification, restricted worker permissions and
confirmed deferred delivery. CI executes these Make targets before publication.

Existing job/worker metrics remain. transcriber_outbox_pending and
transcriber_outbox_oldest_seconds expose publication lag; queued age also includes
time without any workers. Ready/admitted slots are not proof of inference
progress. Container readiness additionally requires the slot's broker connection.

### Docker Desktop and private Headscale access

A native host VPN is not automatically routed into Docker Desktop's Linux VM.
The migration verified that WSL itself can reach 100.64.0.5 while ordinary and
host-network containers cannot. Use the optional, per-device `worker-tailnet`
service instead of publishing AMQPS to the internet or depending on another
project's VPN container:

```dotenv
WORKER_TAILNET=1
WORKER_TAILNET_HOSTNAME=transcriber-workers-wsl
HEADSCALE_LOGIN_SERVER=https://headscale.example.com
HEADSCALE_AUTHKEY_PATH=/absolute/private/path/to/headscale.key
```

Provision a single-use, expiring Headscale preauth key for the existing owner,
write it to a 0600 file without printing it, and mount only that file. The
registration key is not an AMQP or worker API credential. Use distinct hostnames
and registration keys for WSL and Mac. The sidecar retains its node identity in
`worker-tailnet-state`; keep that volume. `TS_AUTH_ONCE=true` allows restart with
an already-used/expired bootstrap key. A device that has never registered needs
a fresh unexpired key before its first launch.

The dedicated VPN container uses TUN and network capabilities, plus DAC_OVERRIDE
for its host-owned credential file. Inference containers retain their non-root
UID, read-only filesystem and dropped capabilities. They share the VPN network
namespace and keep the same CPU/GPU models, limits and shared-model slots. DNS,
subnet routes, exit-node advertisements and incoming connections are not enabled.
The existing native host VPN and other projects are unchanged.

The image is pinned by TAILSCALE_VERSION. The updater keeps the sidecar's network
namespace when replacing inference images. After recreating the sidecar itself,
use `make up` to recreate/reconnect dependent workers; service_healthy alone is
not a guarantee of recovery after every future network failure. Without the
sidecar, workers require a directly reachable private broker endpoint.
