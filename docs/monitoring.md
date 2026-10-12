# Transcription dashboards and metrics

The transcriber repository owns two Grafana dashboards:

- `/d/transcriber-overview`: worker availability, queue depth, oldest waiting
  job, valid active leases, completion throughput, failures/retries, latency,
  expired leases, and retained results awaiting client Ack.
- `/d/transcriber-workers`: worker selector, ready/idle/busy timeline,
  availability percentage, last report age, duplicate process instances,
  progress, current attempt age, throughput and successful-attempt P95.

`scripts/render_dashboards.py` is the editable source. `make dashboards`
regenerates `monitoring/grafana/*.json`; do not edit the JSON by hand.
`make monitoring-check` checks the generated files and parses all panel PromQL
with the pinned Prometheus tool from `versions.mk`. CI runs this through
`make check` before deployment.

## Delivery

The existing Grafana file provider watches `${MONITORING_DATA_DIR}/grafana-dashboards`
every 30 seconds, with folders inferred from subdirectories. Prometheus already
watches `${MONITORING_DATA_DIR}/prometheus-targets/*.yml` every 30 seconds.
Server `make up`/CI `make deploy` installs only this project's two dashboards
under `Transcription/` and the `transcriber.yml` discovery file. Installation is
atomic per file and leaves other projects' files alone. No Grafana restart,
new datasource, public metrics route, worker ingress port or notification
configuration is required.

`MONITORING_DATA_DIR` defaults to `$HOME/.monitoring`; override it as a Make
variable/environment setting for a different server. The server must already
have the external `prometheus-net`, owned by the monitoring stack. Ingress binds
`METRICS_ADDRESS=transcriber-metrics:9108` only on that network alias; there is
no host port. `METRICS_ADDRESS` unset disables the endpoint for standalone
Python launches. The datasource UID is the existing `prometheus`.

## Meaning and limitations

- Availability comes from the coordinator's persisted ready reports. Any ready
  process instance makes its worker ID online until the configured TTL expires
  (90 seconds by default). Readiness begins after model load. The permanent
  fallback reports independently during synchronous inference. Its reports do
  not suppress fallback claiming: only `PREFERRED_WORKER_IDS` does that.
- Busy means the worker owns a valid lease, not proof that inference is making
  progress. The lease counter and progress panels help distinguish this.
- Offline Mac/WSL is expected. Missing exporter/Prometheus observations are
  unknown, never rendered as healthy or artificially filled with offline.
  Availability percentage averages successful samples in the selected range;
  exporter outages and time before installation are excluded.
- `transcriber_jobs{status=...}` is current retained database state. Terminal
  counts decrease after TTL cleanup. Throughput uses separate durable counters.
- Migration `0004_observability.sql` observes committed job transitions in the
  same transaction as the job update. Fenced/rejected Complete/Fail calls and
  duplicate submits add no observations. Counters and histogram buckets survive
  ingress restarts and job deletion; they belong to the regular PostgreSQL
  backup. Existing jobs are preserved without synthesizing historical counters.
- First-claim queue wait is creation to first claim, including upload time.
  Processing duration covers only the final successful attempt. Turnaround is
  creation to successful completion, including waits/retries. Histogram P95 is
  an estimate; when no jobs finish, it is undefined. These graphs do not compare
  model speed on identical audio and do not measure real-time factor.
- Retry attempts include reclaiming an expired lease. Terminal errors exclude
  user cancellations. Attempt failures include failed/expired exhausted
  attempts; lease expirations are also counted separately. Retries keep the
  existing queue semantics.
- Results awaiting Ack measure client durable receipt, not Telegram delivery.
  Notifications/outbox and Telegram delivery metrics require instrumentation in
  the consuming bot and are not inferred from transcriber completion.
- Last report age survives per-process availability cleanup. Worker IDs are
  configured preferred IDs plus `FALLBACK_WORKER_ID` (default `server-fallback`).
  Unlisted observation IDs aggregate under `other`; no per-job/user/error-text
  labels, audio, transcription text, tokens or input paths are exported.
- Remote RAM, VRAM, model/revision metadata and host utilization are not
  collected by this endpoint. They need additional worker/host instrumentation.

The endpoint caches snapshots for five seconds, reads with a separate read-only
repeatable-read transaction and bounded connection/query timeouts, and returns
503 when the database snapshot fails. Prometheus `up{service="transcriber"}`
then becomes zero. It does not serve stale successful snapshots after failure.
