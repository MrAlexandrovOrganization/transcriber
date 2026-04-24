"""Persistent storage and durable queue primitives for transcription jobs."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import os
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

logger = logging.getLogger(__name__)

UTC = timezone.utc


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


class JobStore:
    def __init__(self) -> None:
        self._dsn = os.environ["DATABASE_URL"]
        self._lease_seconds = int(os.getenv("JOB_LEASE_SECONDS", "900"))
        self._max_attempts = int(os.getenv("JOB_MAX_ATTEMPTS", "3"))
        self._storage_root = Path(os.getenv("JOB_STORAGE_DIR", "/data/jobs"))
        self._storage_root.mkdir(parents=True, exist_ok=True)

    @property
    def storage_root(self) -> Path:
        return self._storage_root

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def init_schema(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS transcription_jobs (
                        job_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        format TEXT NOT NULL,
                        preset TEXT NOT NULL,
                        language_override TEXT NOT NULL DEFAULT '',
                        initial_prompt_override TEXT NOT NULL DEFAULT '',
                        input_path TEXT NOT NULL,
                        result_path TEXT NOT NULL DEFAULT '',
                        error TEXT NOT NULL DEFAULT '',
                        progress_percent REAL NOT NULL DEFAULT 0,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL,
                        lease_owner TEXT NOT NULL DEFAULT '',
                        lease_expires_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        started_at TIMESTAMPTZ,
                        finished_at TIMESTAMPTZ,
                        text TEXT NOT NULL DEFAULT '',
                        segments_json JSONB NOT NULL DEFAULT '[]'::jsonb
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_transcription_jobs_claim ON transcription_jobs (status, lease_expires_at, created_at)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_transcription_jobs_updated_at ON transcription_jobs (updated_at)"
                )
            conn.commit()

    def create_job(
        self,
        *,
        fmt: str,
        preset: str,
        language_override: str,
        initial_prompt_override: str,
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
                        max_attempts
                    ) VALUES (%s, 'accepted', %s, %s, %s, %s, %s, %s)
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
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return self._row_to_job(row), input_path

    def mark_downloading(self, job_id: str) -> None:
        self._update_status(job_id, status="downloading")

    def mark_queued(self, job_id: str) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transcription_jobs
                    SET status = 'queued',
                        progress_percent = 0,
                        error = '',
                        updated_at = NOW()
                    WHERE job_id = %s
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
            conn.commit()
        if row is None:
            raise KeyError(job_id)
        return int(row["queue_position"])

    def get_job(self, job_id: str) -> JobRecord | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM transcription_jobs WHERE job_id = %s", (job_id,))
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

    def claim_next_job(self, worker_id: str) -> ClaimedJob | None:
        lease_expires_at = datetime.now(tz=UTC) + timedelta(seconds=self._lease_seconds)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT job_id
                        FROM transcription_jobs
                        WHERE status IN ('queued', 'running')
                          AND attempts < max_attempts
                          AND (
                              status = 'queued'
                              OR lease_expires_at IS NULL
                              OR lease_expires_at < NOW()
                          )
                        ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE transcription_jobs AS jobs
                    SET status = 'running',
                        attempts = jobs.attempts + 1,
                        lease_owner = %s,
                        lease_expires_at = %s,
                        started_at = COALESCE(jobs.started_at, NOW()),
                        updated_at = NOW(),
                        error = CASE WHEN jobs.error = 'cancelled' THEN jobs.error ELSE '' END
                    FROM candidate
                    WHERE jobs.job_id = candidate.job_id
                    RETURNING jobs.*
                    """,
                    (worker_id, lease_expires_at),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return ClaimedJob(
            job=self._row_to_job(row),
            lease_owner=worker_id,
            lease_expires_at=lease_expires_at,
        )

    def heartbeat(self, job_id: str, worker_id: str, progress_percent: float) -> None:
        lease_expires_at = datetime.now(tz=UTC) + timedelta(seconds=self._lease_seconds)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transcription_jobs
                    SET lease_expires_at = %s,
                        progress_percent = %s,
                        updated_at = NOW()
                    WHERE job_id = %s
                      AND lease_owner = %s
                      AND status = 'running'
                    """,
                    (lease_expires_at, progress_percent, job_id, worker_id),
                )
            conn.commit()

    def complete_job(
        self,
        *,
        job_id: str,
        worker_id: str,
        text: str,
        segments: list[dict[str, float | str]],
        result_path: str,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transcription_jobs
                    SET status = 'done',
                        text = %s,
                        segments_json = %s,
                        result_path = %s,
                        progress_percent = 100,
                        error = '',
                        lease_owner = '',
                        lease_expires_at = NULL,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE job_id = %s
                      AND lease_owner = %s
                    """,
                    (text, Json(segments), result_path, job_id, worker_id),
                )
            conn.commit()

    def fail_job(self, *, job_id: str, worker_id: str, error: str, retryable: bool) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT attempts, max_attempts, status, error FROM transcription_jobs WHERE job_id = %s",
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    conn.commit()
                    return
                attempts = int(row["attempts"])
                max_attempts = int(row["max_attempts"])
                next_status = "queued" if retryable and attempts < max_attempts else "failed"
                finished_at_sql = "NOW()" if next_status == "failed" else "NULL"
                cur.execute(
                    f"""
                    UPDATE transcription_jobs
                    SET status = %s,
                        error = %s,
                        progress_percent = 0,
                        lease_owner = '',
                        lease_expires_at = NULL,
                        finished_at = {finished_at_sql},
                        updated_at = NOW()
                    WHERE job_id = %s
                      AND lease_owner = %s
                    """,
                    (next_status, error, job_id, worker_id),
                )
            conn.commit()

    def cleanup_expired_jobs(self, ttl_seconds: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM transcription_jobs
                    WHERE finished_at IS NOT NULL
                      AND finished_at < NOW() - (%s * INTERVAL '1 second')
                    RETURNING input_path, result_path
                    """,
                    (ttl_seconds,),
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
        if paths:
            job_dir = paths[0].parent
            with contextlib.suppress(OSError):
                job_dir.rmdir()

    def _update_status(self, job_id: str, *, status: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE transcription_jobs SET status = %s, updated_at = NOW() WHERE job_id = %s",
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
        )
