# Preferred workers and server fallback

Compose configures `mac-m3-pro`, `wsl-gpu`, and `wsl-cpu` as preferred workers;
the permanent worker is `server-fallback` with `WORKER_FALLBACK=1`.
`PREFERRED_WORKER_IDS` is a comma-separated coordinator allowlist, identical on
ingress and fallback worker. With no allowlist, storage preserves legacy claims.

After loading its complete local model, each remote process creates a fresh UUID
and calls authenticated `ReportAvailability(worker_id, instance_id, ready=true)`.
It renews availability every 20 seconds independently of job polling, downloads
and inference. PostgreSQL sets expiry using its own clock: default TTL 90 seconds,
configurable with `WORKER_AVAILABILITY_TTL_S` (30–600). Reporting failure pauses
new remote claims until reporting succeeds. Graceful shutdown attempts to withdraw
its own process registration; crash, sleep, or lost connectivity expires by TTL.
Old process shutdown cannot remove another process's registration.

The same SQL statement that claims a job checks availability. The fallback can
claim only if **none** of the configured preferred worker IDs has a live process.
A preferred worker must have a live registration matching its own process UUID.
Idle and busy processes both count as available: if all preferred workers are
busy, jobs wait for them rather than overflowing to the server. Mac and both WSL
workers share the same priority; they compete for jobs. There is no GPU-first
ordering between these three workers.

Availability does not preempt a job already owned by the fallback. After preferred
workers return, that job finishes on the server, and new fallback claims stop.
A claim whose database snapshot predates registration can still succeed.
Concurrent claims retain `FOR UPDATE SKIP LOCKED` and per-attempt lease fencing.

For new queued jobs the fallback becomes eligible after the last preferred
availability expires (up to about 90 seconds, plus its two-second poll). Recovery
of a job already running on a disconnected process also waits for its job lease:
Compose now uses 120 seconds, renewed at most every 30 seconds. No job can be
reclaimed while its original lease remains valid. Existing attempt limits still
apply to repeatedly interrupted or failed inference. Losing the coordinator or
its database prevents every worker from claiming until it returns.

Registry rows older than one day are cleaned during reporting. Observe runtime
readiness by selecting worker ID, `ready_until > now()`, and expiry from
`worker_availability`; never inspect audio, bearer tokens or transcripts for a
readiness check. Job claim/completion logs identify the owning worker/operation.
The registry is not historical monitoring or a proof of inference quality.

Worker IDs are trusted identities within the existing shared bearer-token pool;
a trusted token holder could report another allowed ID. Keep the token protected
and the API on loopback behind SSH. Per-device credentials/revocation are a later
hardening step.

Rollout updates ingress and permanent worker together through Git/CI/CD, then
updates remote worker containers on the same published revision with `make up`.
See [per-machine container configuration](container-workers.md). Migration
`0003_worker_availability.sql` is additive and tracked by the existing ledger.
Old clients remain protocol compatible; an old preferred native worker that
cannot report availability will not claim work until upgraded.

Checks cover real PostgreSQL/gRPC transitions, idle/busy readiness, TTL expiry,
multiple instances, old-instance shutdown, authentication, simultaneous claims,
and expired-owner fencing. Remote unit tests check reporting during inference,
withdrawal at shutdown, and refusal to claim before readiness is accepted.
