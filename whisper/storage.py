"""Persistent storage and durable queue primitives for transcription jobs."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from whisper.scheduling import parse_policies

logger = logging.getLogger(__name__)

UTC = timezone.utc
SQL_DIR = Path(__file__).with_name("sql")


def _sql(name: str) -> str:
    return (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


@dataclasses.dataclass(frozen=True)
class JobRecord:
    job_id: str
    status: str
    format: str
    preset: str
    language_override: str
    initial_prompt_override: str
    input_path: str
    result_path: str
    error: str
    progress_percent: float
    attempts: int
    max_attempts: int
    lease_owner: str
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    text: str
    segments_json: str
    traceparent: str
    tracestate: str
    request_id: str = ""
    retain_until_ack: bool = False
    acknowledged_at: datetime | None = None
    dispatch_id: str = ""
    delivery_id: str = ""

    @property
    def segments(self) -> list[dict[str, float | str]]:
        if not self.segments_json:
            return []
        return json.loads(self.segments_json)


@dataclasses.dataclass(frozen=True)
class ClaimedJob:
    job: JobRecord
    lease_owner: str
    lease_expires_at: datetime


class UploadBusy(Exception):
    """Another stream is uploading the same client request."""


class UploadConflict(Exception):
    """A client request ID was reused with different immutable options."""


@dataclasses.dataclass(frozen=True)
class UploadReservation:
    job: JobRecord
    input_path: Path
    needs_upload: bool


class JobStore:
    def __init__(self) -> None:
        self._dsn = os.environ["DATABASE_URL"]
        self._lease_seconds = int(os.getenv("JOB_LEASE_SECONDS", "900"))
        self._max_attempts = int(os.getenv("JOB_MAX_ATTEMPTS", "3"))
        self.worker_policies = parse_policies(os.getenv("WORKER_POLICIES", ""))
        self.queue_backend = os.getenv("QUEUE_BACKEND", "postgres")
        if self.queue_backend not in {"postgres", "rabbitmq"}:
            raise ValueError("QUEUE_BACKEND must be postgres or rabbitmq")
        if self.queue_backend == "rabbitmq" and not self.worker_policies:
            raise ValueError("RabbitMQ requires explicit WORKER_POLICIES")
        self._demand_ttl_seconds = int(os.getenv("WORKER_DEMAND_TTL_S", "10"))
        if not 5 <= self._demand_ttl_seconds <= 60:
            raise ValueError("WORKER_DEMAND_TTL_S must be within 5..60")
        self._preferred_workers = [
            value.strip()
            for value in os.getenv("PREFERRED_WORKER_IDS", "").split(",")
            if value.strip()
        ]
        self._availability_ttl_seconds = int(
            os.getenv("WORKER_AVAILABILITY_TTL_S", "90")
        )
        if any(
            not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value)
            for value in self._preferred_workers
        ):
            raise ValueError("Invalid preferred worker ID")
        if not 30 <= self._availability_ttl_seconds <= 600:
            raise ValueError("Worker availability TTL must be within 30..600 seconds")
        self._storage_root = Path(os.getenv("JOB_STORAGE_DIR", "/data/jobs"))
        if self._lease_seconds < 3 or self._max_attempts < 1:
            raise ValueError("Lease must be >= 3 seconds and max attempts >= 1")
        self._storage_root.mkdir(parents=True, exist_ok=True)

    @property
    def lease_seconds(self) -> int:
        return self._lease_seconds

    @property
    def availability_ttl_seconds(self) -> int:
        return self._availability_ttl_seconds

    @property
    def storage_root(self) -> Path:
        return self._storage_root

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=3)

    def metrics_snapshot(self) -> dict[str, list[dict]]:
        # Independent, bounded, read-only snapshot: scrapes never hold job locks.
        with psycopg.connect(
            self._dsn, row_factory=dict_row, connect_timeout=3
        ) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            conn.execute("SET LOCAL statement_timeout = '3s'")
            return {
                name: conn.execute(
                    _sql("metrics_" + name),
                    (self.queue_backend == "rabbitmq",) if name == "outbox" else None,
                ).fetchall()
                for name in ("jobs", "workers", "running", "observations", "outbox")
            }

    def init_schema(self) -> None:
        # Keep the established startup migration policy, now with a ledger and
        # one transaction/advisory lock shared by ingress and local workers.
        with self.connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(812409715031)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS transcriber_schema_migrations "
                "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            )
            for migration in sorted((SQL_DIR / "migrations").glob("*.sql")):
                applied = conn.execute(
                    "SELECT version FROM transcriber_schema_migrations WHERE version = %s",
                    (migration.name,),
                ).fetchone()
                if applied is None:
                    conn.execute(migration.read_text(encoding="utf-8"))
                    conn.execute(
                        "INSERT INTO transcriber_schema_migrations (version) VALUES (%s)",
                        (migration.name,),
                    )
            if self.queue_backend == "rabbitmq":
                conn.execute(_sql("broker_seed"))

    def create_job(
        self,
        *,
        fmt: str,
        preset: str,
        language_override: str,
        initial_prompt_override: str,
        traceparent: str = "",
        tracestate: str = "",
        request_id: str = "",
        retain_until_ack: bool = False,
    ) -> tuple[JobRecord, Path]:
        job_id = str(uuid.uuid4())
        job_dir = self._storage_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        input_path = job_dir / f"input.{fmt}"
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO transcription_jobs (
                        job_id,
                        status,
                        format,
                        preset,
                        language_override,
                        initial_prompt_override,
                        input_path,
                        max_attempts,
                        traceparent,
                        tracestate,
                        request_id,
                        retain_until_ack
                    ) VALUES (%s, 'accepted', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        job_id,
                        fmt,
                        preset,
                        language_override,
                        initial_prompt_override,
                        str(input_path),
                        self._max_attempts,
                        traceparent,
                        tracestate,
                        request_id,
                        retain_until_ack,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._row_to_job(row), input_path

    @contextlib.contextmanager
    def reserve_upload(
        self,
        *,
        request_id: str,
        retain_until_ack: bool,
        fmt: str,
        preset: str,
        language_override: str,
        initial_prompt_override: str,
        traceparent: str = "",
        tracestate: str = "",
    ):
        # A session lock spans streaming without keeping a database transaction
        # open. PostgreSQL releases it automatically when the process crashes.
        # A concurrent duplicate fails fast rather than consuming an RPC thread.
        with self.connect() as connection:
            if request_id:
                locked = connection.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS locked",
                    (request_id,),
                ).fetchone()["locked"]
                connection.commit()
                if not locked:
                    raise UploadBusy()
            try:
                row = None
                if request_id:
                    row = connection.execute(
                        _sql("upload_lookup"),
                        (request_id,),
                    ).fetchone()
                    connection.commit()
                if row:
                    job = self._row_to_job(row)
                    identity = (
                        job.format,
                        job.preset,
                        job.language_override,
                        job.initial_prompt_override,
                        job.retain_until_ack,
                    )
                    if identity != (
                        fmt,
                        preset,
                        language_override,
                        initial_prompt_override,
                        retain_until_ack,
                    ):
                        raise UploadConflict()
                    input_path = Path(job.input_path)
                else:
                    job, input_path = self.create_job(
                        fmt=fmt,
                        preset=preset,
                        language_override=language_override,
                        initial_prompt_override=initial_prompt_override,
                        traceparent=traceparent,
                        tracestate=tracestate,
                        request_id=request_id,
                        retain_until_ack=retain_until_ack,
                    )
                yield UploadReservation(
                    job=job,
                    input_path=input_path,
                    needs_upload=job.status in {"accepted", "downloading"},
                )
            finally:
                if request_id:
                    connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                        (request_id,),
                    )
                    connection.commit()

    def queue_position(self, job_id: str) -> int:
        with self.connect() as conn:
            row = conn.execute(_sql("queue_position"), (job_id,)).fetchone()
        return int(row["count"])

    def mark_upload_interrupted(self, job_id: str) -> None:
        with self.connect() as conn:
            conn.execute(_sql("upload_interrupted"), (job_id,))

    def acknowledge_job(self, job_id: str) -> bool:
        with self.connect() as conn:
            return conn.execute(_sql("ack"), (job_id,)).fetchone() is not None

    def mark_downloading(self, job_id: str) -> None:
        self._update_status(job_id, status="downloading")

    def mark_queued(self, job_id: str) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transcription_jobs
                    SET status = 'queued',
                        progress_percent = 0,
                        error = '',
                        updated_at = NOW()
                    WHERE job_id = %s AND status IN ('accepted', 'downloading')
                    RETURNING (
                        SELECT COUNT(*)
                        FROM transcription_jobs q
                        WHERE q.status IN ('queued', 'running')
                          AND q.created_at <= transcription_jobs.created_at
                    ) AS queue_position
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                if row is not None and self.queue_backend == "rabbitmq":
                    self._enqueue(conn, job_id)
            conn.commit()
        if row is None:
            return None
        return int(row["queue_position"])

    def _enqueue(self, conn, job_id: str, delay: int = 0) -> None:
        message_id = str(uuid.uuid4())
        conn.execute(
            _sql("dispatch"),
            (message_id, job_id),
        )
        conn.execute(
            _sql("outbox_insert"),
            (message_id, job_id, delay),
        )

    def publish_next(self, publish) -> bool:
        """Confirm with RabbitMQ before committing. An ambiguous commit may duplicate."""
        with self.connect() as conn:
            conn.execute("SET LOCAL statement_timeout = '15s'")
            row = conn.execute(_sql("outbox_next")).fetchone()
            if row is None:
                return False
            publish(row["job_id"], row["message_id"])
            conn.execute(
                _sql("outbox_published"),
                (row["message_id"],),
            )
        return True

    def register_consumer(self, worker_id: str, instance_id: str) -> int | None:
        policy = self.worker_policies.get(worker_id)
        if (
            self.queue_backend != "rabbitmq"
            or not policy
            or policy["max_concurrency"] == 0
        ):
            return None
        with self.connect() as conn:
            conn.execute(_sql("scheduler_lock"))
            row = conn.execute(
                _sql("broker_admit"),
                (
                    worker_id,
                    worker_id,
                    instance_id,
                    policy["max_concurrency"],
                    worker_id,
                    instance_id,
                ),
            ).fetchone()
        return 10000 - policy["priority"] if row else None

    def begin_attempt(self, worker_id, instance_id, job_id, message_id, delivery_id):
        """Fence one broker delivery; neither chooses a job nor ranks workers."""
        with self.connect() as conn:
            conn.execute(_sql("scheduler_lock"))
            row = conn.execute(
                _sql("broker_job"),
                (job_id,),
            ).fetchone()
            if (
                not row
                or row["dispatch_id"] != message_id
                or row["status"] in {"done", "failed"}
            ):
                return "discard", None
            if row["status"] == "running" and row["live"]:
                if (
                    row["delivery_id"] == delivery_id
                    and row["assigned_worker_id"] == worker_id
                    and row["assigned_instance_id"] == instance_id
                ):
                    return "started", ClaimedJob(
                        self._row_to_job(row),
                        row["lease_owner"],
                        row["lease_expires_at"],
                    )
                return "busy", None
            if row["status"] not in {"queued", "running"}:
                return "discard", None
            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    _sql("broker_exhausted"),
                    (job_id,),
                )
                return "discard", None
            policy = self.worker_policies.get(worker_id)
            if not policy or policy["max_concurrency"] == 0:
                return "busy", None
            capacity = conn.execute(
                _sql("broker_capacity"),
                (worker_id, instance_id, worker_id, instance_id, worker_id),
            ).fetchone()
            if (
                not capacity["ready"]
                or capacity["busy"]
                or capacity["active"] >= policy["max_concurrency"]
            ):
                return "busy", None
            lease_owner = f"{worker_id}/{uuid.uuid4()}"
            row = conn.execute(
                _sql("broker_start"),
                (
                    lease_owner,
                    self._lease_seconds,
                    worker_id,
                    instance_id,
                    delivery_id,
                    job_id,
                ),
            ).fetchone()
            return "started", ClaimedJob(
                self._row_to_job(row), lease_owner, row["lease_expires_at"]
            )

    def get_job(self, job_id: str) -> JobRecord | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM transcription_jobs WHERE job_id = %s", (job_id,)
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def cancel_job(self, job_id: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transcription_jobs
                    SET status = 'failed',
                        error = 'cancelled',
                        progress_percent = 0,
                        finished_at = NOW(),
                        lease_owner = '',
                        lease_expires_at = NULL,
                        updated_at = NOW()
                    WHERE job_id = %s
                      AND status NOT IN ('done', 'failed')
                    RETURNING job_id
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
            conn.commit()
        return row is not None

    def report_availability(
        self, worker_id: str, instance_id: str, ready: bool
    ) -> bool:
        with psycopg.connect(
            self._dsn, connect_timeout=3, row_factory=dict_row
        ) as conn:
            conn.execute("SET LOCAL statement_timeout = '3s'")
            conn.execute(_sql("cleanup_availability"))
            conn.execute(
                _sql("report_availability"),
                (
                    worker_id,
                    instance_id,
                    self._availability_ttl_seconds if ready else 0,
                    self._demand_ttl_seconds if ready else 0,
                ),
            )
        return True

    def claim_next_job(
        self, worker_id: str, *, fallback: bool = False, instance_id: str = ""
    ) -> ClaimedJob | None:
        if self.queue_backend == "rabbitmq":
            raise ValueError("SQL claiming is disabled; use RabbitMQ consumers")
        # Fence each attempt independently, even across worker restarts/PID reuse.
        lease_owner = f"{worker_id}/{uuid.uuid4()}"
        with self.connect() as conn:
            with conn.cursor() as cur:
                if self.worker_policies:
                    # The statement after this lock gets a fresh READ COMMITTED
                    # snapshot, including every previous slot grant.
                    cur.execute(_sql("scheduler_lock"))
                    cur.execute(_sql("reap"))
                    cur.execute(
                        _sql("request_work"),
                        (self._demand_ttl_seconds, worker_id, instance_id),
                    )
                    eligible = cur.execute(
                        _sql("select_worker"),
                        (Json(self.worker_policies), worker_id, instance_id),
                    ).fetchone()
                    row = None
                    if eligible:
                        row = cur.execute(
                            _sql("claim_slot"),
                            (lease_owner, self._lease_seconds, worker_id, instance_id),
                        ).fetchone()
                    return (
                        ClaimedJob(
                            self._row_to_job(row), lease_owner, row["lease_expires_at"]
                        )
                        if row
                        else None
                    )
                cur.execute(_sql("reap"))
                cur.execute(
                    _sql("claim"),
                    (
                        fallback,
                        self._preferred_workers,
                        worker_id,
                        self._preferred_workers,
                        worker_id,
                        instance_id,
                        lease_owner,
                        self._lease_seconds,
                        worker_id,
                        instance_id,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return ClaimedJob(
            job=self._row_to_job(row),
            lease_owner=lease_owner,
            lease_expires_at=row["lease_expires_at"],
        )

    def get_owned_job(self, job_id: str, lease_owner: str) -> JobRecord | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_sql("owned"), (job_id, lease_owner))
                row = cur.fetchone()
        return self._row_to_job(row) if row else None

    def heartbeat(self, job_id: str, worker_id: str, progress_percent: float) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if self.worker_policies:
                    # Lease renewal must serialize with capacity decisions too:
                    # a renewal in flight near expiry cannot resurrect a busy
                    # slot after a concurrent claim has counted it as free.
                    cur.execute(_sql("scheduler_lock"))
                cur.execute(
                    _sql("heartbeat"),
                    (self._lease_seconds, progress_percent, job_id, worker_id),
                )
                accepted = cur.rowcount == 1
            conn.commit()
        return accepted

    def complete_job(
        self,
        *,
        job_id: str,
        worker_id: str,
        text: str,
        segments: list[dict[str, float | str]],
        result_path: str,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _sql("complete"),
                    (text, Json(segments), result_path, job_id, worker_id),
                )
                accepted = cur.rowcount == 1
            conn.commit()
        return accepted

    def fail_job(
        self,
        *,
        job_id: str,
        worker_id: str,
        error: str,
        retryable: bool,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _sql("fail"),
                    (retryable, error, retryable, job_id, worker_id, worker_id),
                )
                accepted = cur.rowcount == 1
                if accepted and self.queue_backend == "rabbitmq":
                    row = conn.execute(
                        _sql("retry_status"),
                        (job_id,),
                    ).fetchone()
                    if row["status"] == "queued":
                        self._enqueue(
                            conn, job_id, min(300, 2 ** min(row["attempts"], 8))
                        )
            conn.commit()
        return accepted

    def cleanup_expired_jobs(self, ttl_seconds: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_sql("reap"))
                cur.execute(
                    _sql("cleanup"),
                    (ttl_seconds, ttl_seconds),
                )
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            self._cleanup_job_files(row["input_path"], row["result_path"])
        return len(rows)

    def _cleanup_job_files(self, input_path: str, result_path: str) -> None:
        paths = [Path(input_path)]
        if result_path:
            paths.append(Path(result_path))
        for path in paths:
            with contextlib.suppress(OSError):
                path.unlink()
        for partial in paths[0].parent.glob("upload-*.part"):
            with contextlib.suppress(OSError):
                partial.unlink()
        if paths:
            job_dir = paths[0].parent
            with contextlib.suppress(OSError):
                job_dir.rmdir()

    def _update_status(self, job_id: str, *, status: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE transcription_jobs SET status = %s, updated_at = NOW() WHERE job_id = %s AND status IN ('accepted', 'downloading')",
                    (status, job_id),
                )
            conn.commit()

    def _row_to_job(self, row: dict) -> JobRecord:
        return JobRecord(
            job_id=row["job_id"],
            status=row["status"],
            format=row["format"],
            preset=row["preset"],
            language_override=row["language_override"],
            initial_prompt_override=row["initial_prompt_override"],
            input_path=row["input_path"],
            result_path=row["result_path"],
            error=row["error"],
            progress_percent=float(row["progress_percent"]),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            text=row["text"],
            segments_json=json.dumps(row["segments_json"]),
            traceparent=row["traceparent"],
            tracestate=row["tracestate"],
            request_id=row["request_id"],
            retain_until_ack=row["retain_until_ack"],
            acknowledged_at=row["acknowledged_at"],
            dispatch_id=row.get("dispatch_id", ""),
            delivery_id=row.get("delivery_id", ""),
        )
