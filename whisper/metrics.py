"""Private Prometheus endpoint backed by a bounded read-only database snapshot."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shutil
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from whisper.admission import UploadLimits, storage_bytes
from whisper.storage import JobStore

LOG = logging.getLogger(__name__)
# Must match the immutable histogram boundaries in 0004_observability.sql.
BUCKETS = (1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 10800)
COUNTERS = {
    "submitted": "jobs_submitted",
    "completed": "jobs_completed",
    "failed": "jobs_failed",
    "cancelled": "jobs_cancelled",
    "attempts": "attempts",
    "retries": "retry_attempts",
    "attempt_failures": "attempt_failures",
    "lease_expirations": "lease_expirations",
}
HISTOGRAMS = {
    "queue_wait": "job_queue_wait_seconds",
    "processing": "job_processing_seconds",
    "turnaround": "job_turnaround_seconds",
}


def render_metrics(snapshot: dict, workers: dict[str, str], policies=None) -> bytes:
    """Only configured worker IDs are labels; unlisted IDs collapse to 'other'."""
    lines: list[str] = []

    def definition(name, kind, help_text):
        lines.extend(
            [
                f"# HELP transcriber_{name} {help_text}",
                f"# TYPE transcriber_{name} {kind}",
            ]
        )

    def sample(name, value, **labels):
        suffix = ""
        if labels:
            suffix = (
                "{"
                + ",".join(
                    f"{key}={json.dumps(str(value))}"
                    for key, value in sorted(labels.items())
                )
                + "}"
            )
        number = float(value)
        rendered = "NaN" if math.isnan(number) else str(number)
        lines.append(f"transcriber_{name}{suffix} {rendered}")

    jobs = {row["status"]: row for row in snapshot["jobs"]}
    definition(
        "jobs",
        "gauge",
        "Currently retained jobs, including terminal jobs awaiting cleanup.",
    )
    for status in ("accepted", "downloading", "queued", "running", "done", "failed"):
        sample("jobs", jobs.get(status, {}).get("jobs", 0), status=status)
    for name, value, help_text in (
        (
            "queue_oldest_wait_seconds",
            jobs.get("queued", {}).get("oldest_seconds", 0),
            "Age since submission of the oldest queued job; zero for an empty queue.",
        ),
        (
            "queue_retry_jobs",
            jobs.get("queued", {}).get("retried", 0),
            "Queued jobs that have already been attempted.",
        ),
        (
            "expired_leases",
            jobs.get("running", {}).get("expired", 0),
            "Running rows whose lease has expired and require recovery.",
        ),
        (
            "results_unacknowledged",
            sum(jobs.get(s, {}).get("unacknowledged", 0) for s in ("done", "failed")),
            "Retained terminal results not yet durably acknowledged by the client; not Telegram delivery confirmation.",
        ),
    ):
        definition(name, "gauge", help_text)
        sample(name, value)

    definition(
        "results_oldest_unacknowledged_seconds",
        "gauge",
        "Age since completion of the oldest retained unacknowledged terminal result.",
    )
    sample(
        "results_oldest_unacknowledged_seconds",
        max(
            (
                jobs.get(s, {}).get("oldest_unacknowledged_seconds", 0)
                for s in ("done", "failed")
            ),
            default=0,
        ),
    )
    for key, value in snapshot.get("storage", {}).items():
        definition("storage_" + key, "gauge", "Job volume capacity in bytes.")
        sample("storage_" + key, value)

    availability = {row["worker_id"]: row for row in snapshot["workers"]}
    outbox = (snapshot.get("outbox") or [{}])[0]
    for name, key, help_text in (
        (
            "outbox_pending",
            "pending",
            "Ready outbox messages awaiting confirmed RabbitMQ publication.",
        ),
        (
            "outbox_oldest_seconds",
            "oldest_seconds",
            "Age of the oldest ready unpublished outbox message.",
        ),
    ):
        definition(name, "gauge", help_text)
        sample(name, outbox.get(key, 0))
    running = {row["worker_id"]: row for row in snapshot["running"]}
    worker_metrics = {
        "worker_online": "At least one unexpired ready report exists for this worker ID.",
        "worker_state": "Worker state: 0 offline, 1 ready and idle, 2 ready and busy. Busy means a valid job lease.",
        "worker_instances": "Number of currently ready execution slots; multiple slots may share one process/model.",
        "worker_last_report_age_seconds": "Age of the last availability report, NaN if never observed.",
        "worker_active_jobs": "Jobs currently held by this worker with an unexpired lease.",
        "worker_progress_percent": "Mean reported progress of currently leased jobs, zero when idle.",
        "worker_attempt_age_seconds": "Age of the oldest currently leased attempt, zero when idle.",
    }
    for name, help_text in worker_metrics.items():
        definition(name, "gauge", help_text)
    if policies:
        definition(
            "worker_priority", "gauge", "Configured pool priority; lower numbers win."
        )
        definition(
            "worker_capacity",
            "gauge",
            "Configured maximum concurrent jobs for the pool.",
        )
        definition(
            "worker_free_slots",
            "gauge",
            "Ready idle slots with fresh work requests or RabbitMQ admission, bounded by pool capacity.",
        )
    for worker_id, role in sorted(workers.items()):
        row = availability.get(worker_id, {})
        active = running.get(worker_id, {})
        online = bool(row.get("online", False))
        values = {
            "worker_online": int(online),
            "worker_state": (2 if active.get("active", 0) else 1) if online else 0,
            "worker_instances": row.get("instances", 0),
            "worker_last_report_age_seconds": row.get("last_report_age", float("nan")),
            "worker_active_jobs": active.get("active", 0),
            "worker_progress_percent": active.get("progress", 0),
            "worker_attempt_age_seconds": active.get("attempt_age", 0),
        }
        for name, value in values.items():
            sample(name, value, worker_id=worker_id, worker_role=role)
        if policies and worker_id in policies:
            policy = policies[worker_id]
            for name, value in {
                "worker_priority": policy["priority"],
                "worker_capacity": policy["max_concurrency"],
                "worker_free_slots": max(
                    0,
                    min(
                        row.get("idle_slots", 0),
                        policy["max_concurrency"] - active.get("active", 0),
                    ),
                ),
            }.items():
                sample(name, value, worker_id=worker_id, worker_role=role)

    # Pre-initialize series so idle dashboards show zero, rather than No data.
    totals = {
        (w, event): [0, 0.0, [0] * len(BUCKETS)]
        for w in (*workers, "ingress", "other")
        for event in (*COUNTERS, *HISTOGRAMS)
    }
    completion_ages = []
    for row in snapshot["observations"]:
        worker_id = row["worker_id"]
        if worker_id not in workers and worker_id != "ingress":
            worker_id = "other"
        key = (worker_id, row["event"])
        if key not in totals:
            continue
        values = totals[key]
        values[0] += row["total"]
        values[1] += row["seconds_sum"]
        values[2] = [a + b for a, b in zip(values[2], row["buckets"], strict=True)]
        if row["event"] == "completed":
            completion_ages.append(float(row["last_event_age"]))
    for event, name in COUNTERS.items():
        definition(
            name + "_total",
            "counter",
            "Durable "
            + event.replace("_", " ")
            + " observations since monitoring migration; survives job cleanup.",
        )
        for worker_id in (*workers, "ingress", "other"):
            sample(name + "_total", totals[(worker_id, event)][0], worker_id=worker_id)
    for event, name in HISTOGRAMS.items():
        definition(
            name,
            "histogram",
            "Durable "
            + event
            + " duration in seconds; queue_wait observes first claim, processing and turnaround observe successful jobs.",
        )
        for worker_id in (*workers, "ingress", "other"):
            count, total_seconds, buckets = totals[(worker_id, event)]
            for bound, count_at_bound in zip(BUCKETS, buckets, strict=True):
                sample(name + "_bucket", count_at_bound, worker_id=worker_id, le=bound)
            sample(name + "_bucket", count, worker_id=worker_id, le="+Inf")
            sample(name + "_count", count, worker_id=worker_id)
            sample(name + "_sum", total_seconds, worker_id=worker_id)
    definition(
        "last_completion_age_seconds",
        "gauge",
        "Seconds since last observed successful completion, NaN before the first completion.",
    )
    sample("last_completion_age_seconds", min(completion_ages, default=float("nan")))
    return ("\n".join(lines) + "\n").encode()


class MetricsEndpoint:
    def __init__(self, store: JobStore, address: str):
        preferred = [
            w.strip()
            for w in os.getenv("PREFERRED_WORKER_IDS", "").split(",")
            if w.strip()
        ]
        self.workers = dict.fromkeys(store.worker_policies or preferred, "preferred")
        self.workers[os.getenv("FALLBACK_WORKER_ID", "server-fallback")] = "fallback"
        if len(self.workers) > 129 or any(
            worker in {"ingress", "other"}
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", worker)
            for worker in self.workers
        ):
            raise ValueError("Invalid monitoring worker IDs")
        self.store = store
        self._payload = b""
        self._cached_at = 0.0
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/metrics":
                    self.send_error(404)
                    return
                try:
                    if time.monotonic() - endpoint._cached_at >= 5:
                        snapshot = store.metrics_snapshot()
                        limits = UploadLimits.from_env()
                        snapshot["storage"] = {
                            "used_bytes": storage_bytes(store.storage_root),
                            "free_bytes": shutil.disk_usage(store.storage_root).free,
                            "limit_bytes": limits.storage_bytes,
                            "reserve_bytes": limits.min_free_bytes,
                        }
                        endpoint._payload = render_metrics(
                            snapshot,
                            endpoint.workers,
                            store.worker_policies,
                        )
                        endpoint._cached_at = time.monotonic()
                    self.send_response(200)
                    self.send_header(
                        "Content-Type", "text/plain; version=0.0.4; charset=utf-8"
                    )
                    self.send_header("Content-Length", str(len(endpoint._payload)))
                    self.end_headers()
                    self.wfile.write(endpoint._payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception:
                    LOG.warning("Metrics snapshot unavailable")
                    self.send_error(503, "Metrics unavailable")

            def log_message(self, *_):
                pass

        class Server(HTTPServer):
            def get_request(self):
                client, peer = super().get_request()
                client.settimeout(5)
                return client, peer

        host, port = address.rsplit(":", 1)
        self.server = Server((host, int(port)), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True, name="prometheus-metrics"
        )
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
