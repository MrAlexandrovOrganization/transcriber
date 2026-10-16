# Pool priorities and shared-model workers

`worker_id` identifies a pool, such as `wsl-cpu`. Each execution slot has a fresh
UUID in `instance_id`, its own job, lease token, heartbeat and temporary audio.
Several slots can share one process and one loaded model. A process restart
creates new slot UUIDs; a late withdrawal never removes a different slot.

## Coordinator policy

Set `WORKER_POLICIES` in the coordinator's private `.env`. Compose passes the
same policy to ingress and the permanent local worker. This is an operator-owned
JSON object; workers cannot submit a priority or increase their pool limit:

```dotenv
WORKER_POLICIES={"wsl-cpu":{"priority":10,"max_concurrency":2},"mac-m3-pro":{"priority":20,"max_concurrency":2},"wsl-gpu":{"priority":30,"max_concurrency":1},"server-fallback":{"priority":100,"max_concurrency":1}}
WORKER_DEMAND_TTL_S=10
```

Lower priority numbers win. Equal numbers compete at the same tier. IDs absent
from the policy cannot claim; `max_concurrency=0` disables new claims for a pool.
Already leased jobs continue. The limit applies across every process/container
using that worker ID, including slots whose availability report has expired but
whose job lease is still valid. Priority is independent of model choice; it does
not validate recognition quality or route by requested model.

With two WSL and two Mac slots, the first two jobs go to WSL, the next two to Mac,
then to the next ready free tier. A newly freed WSL slot takes the next queued
job. Running work is never preempted. Local fallback uses exactly the same
scheduler; `WORKER_FALLBACK` only affects legacy mode.

The scheduler takes a short PostgreSQL transaction advisory lock, refreshes the
requesting slot's demand, checks live leases and pool limits, selects the best
free tier, then claims a FIFO job using `FOR UPDATE SKIP LOCKED`. The lock is
released before downloading or inference. Per-attempt lease fencing still
rejects expired owners' heartbeat/result/failure. Rejected priority/capacity
checks do not consume job attempts.
Lease renewals use the same lock and check the clock after acquiring it.
Lock/statement waits and idle transactions have bounded timeouts, so a stalled
coordinator transaction cannot hold scheduler admission indefinitely.

Availability expires on the coordinator clock (default 90 seconds, range
30–600). Each loaded slot reports every 20 seconds independently of inference.
Availability alone does not reserve work indefinitely: an idle slot must also
have requested work within `WORKER_DEMAND_TTL_S` (5–60 seconds, default 10).
A new registration gets one demand window; periodic readiness reports do not
renew it. A reporter whose claim loop is stuck therefore stops blocking lower
tiers after that window. Recovery of a running job still waits for its lease.
Use `WORKER_POLL_INTERVAL_S=2` (the new default), shorter than the demand window.

## Several slots in one process

On each worker machine, configure the existing single CPU/GPU container:

```dotenv
CPU_CONCURRENCY=2
CPU_THREADS=2
CPU_LIMIT=4
CPU_MEMORY_LIMIT=5500m
GPU_CONCURRENCY=1
```

`CPU_CONCURRENCY` / `GPU_CONCURRENCY` map to `WORKER_CONCURRENCY` (1–16) inside
their containers. Native launches use `WORKER_CONCURRENCY` directly. The local
server supports `SERVER_WORKER_CONCURRENCY`, default 1. The model is constructed
once with faster-whisper `num_workers=N`, which configures CTranslate2 parallel
execution workers sharing weights on the same device. Each slot calls this
engine from its own thread; it never leases another job while processing one.
See [CTranslate2 parallelism](https://opennmt.net/CTranslate2/parallel.html).

CPU threads are **per execution worker**; container CPU/RAM limits are for the
whole process. Budget approximately concurrency × threads, plus audio/VAD and
other services. Shared weights save RAM compared with separate processes, but
audio, features and decoder state still grow with concurrent jobs. The examples
start at two CPU slots; measure peak RAM and latency before increasing them.
Remote container workers and ordinary tests do not download models. ASUS remains disabled.

One gRPC channel and credential are shared by the pool. A heartbeat, failure,
cancellation or lost lease affects only its own job. SIGUSR1 stops new claims
and drains all slots: `/tmp/worker-drained` appears only after every job has
finished its Complete/Fail path and every slot has withdrawn readiness. SIGUSR2
resumes them. The existing updater therefore drains the whole process before
replacing it. Container health requires successful reports from all slots;
SIGTERM stops all slots with the bounded existing shutdown grace. Unfinished
leases expire and are recovered. Native local workers also share one model;
they use the server's existing signal/lease lifecycle.

## Rollout, compatibility and checks

An empty/unset `WORKER_POLICIES` deliberately preserves the previous behavior:
`PREFERRED_WORKER_IDS` is an unordered allowlist, all preferred workers race,
and fallback waits while any preferred instance is online, even if busy.
Do not treat that legacy list as an ordered priority configuration.

Migration `0005_worker_slots.sql` is additive and tracked by the existing ledger.
Update ingress and local fallback together through Git/CI/CD, then update remote
workers and set their concurrency. Existing `.env` files are not overwritten by
examples. Activate priorities by setting `WORKER_POLICIES` and recreating both
coordinator services with the same configuration. If an old running attempt has
no slot UUID, its pool is conservatively held until it finishes/expires. No
protobuf fields/RPCs changed, and old remote single-slot processes remain wire
compatible; update their polling interval if it was explicitly 60 seconds.

Rollback: restore one slot per process and the previous coordinator policy (or
empty policies for legacy semantics), then activate a compatible previous image.
Keep the additive schema and migration ledger; do not reverse migrations or
interrupt running jobs just to change priority. During a mixed coordinator
rollout, old direct-DB workers do not enforce new policy, so enable it only after
both coordinator services have been upgraded.

Metrics expose ready execution slots, active jobs, configured priority/capacity
and free slots with fresh demand. Labels use configured pool IDs, never slot
UUIDs. Availability rows older than one day are cleaned during reports.

`make test-integration` exercises real PostgreSQL/gRPC priority ordering,
concurrent pool limits, disabled/unready/stuck slots, cancellation, expired lease
recovery and old-schema adoption. Unit tests cover shared model construction,
parallel request isolation, all-slot drain/resume and independent lease loss.
With a cached model and synthetic English speech containing the word "worker",
set `TRANSCRIBER_PILOT_AUDIO_FILE` to include real shared-model speech inference.
Ordinary tests never download models or access production queues.
