"""Authenticated worker-only gRPC endpoint; database/files stay on the ingress."""

from __future__ import annotations

import hmac
import logging
import math
import os
import re
import uuid
from concurrent import futures
from pathlib import Path

import grpc

from proto import whisper_pb2 as pb
from proto import whisper_pb2_grpc as rpc
from whisper.limits import MAX_RESULT_BYTES
from whisper.storage import JobStore

logger = logging.getLogger(__name__)


def read_worker_token(path: str) -> str:
    try:
        token = Path(path).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        raise ValueError("Cannot read worker token file") from None
    if (
        len(token) < 32
        or len(token) > 512
        or not token.isascii()
        or any(c.isspace() for c in token)
    ):
        raise ValueError(
            "Worker token must contain 32..512 non-whitespace ASCII characters"
        )
    return token


class WorkerServicer(rpc.WorkerServiceServicer):
    def __init__(self, store: JobStore, token: str):
        self._store = store
        self._authorization = f"Bearer {token}"

    def _authenticate(self, context: grpc.ServicerContext) -> None:
        values = [
            m.value for m in context.invocation_metadata() if m.key == "authorization"
        ]
        if len(values) != 1 or not hmac.compare_digest(values[0], self._authorization):
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Worker authentication required"
            )

    def _owned(self, request, context):
        self._authenticate(context)
        if not request.job_id or not request.lease_token:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Job and lease token required"
            )
        job = self._store.get_owned_job(request.job_id, request.lease_token)
        if job is None:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION, "Lease is no longer owned"
            )
        return job

    def Claim(self, request, context):
        self._authenticate(context)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", request.worker_id):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid worker ID")
        if request.instance_id:
            try:
                if str(uuid.UUID(request.instance_id)) != request.instance_id:
                    raise ValueError()
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid worker instance ID"
                )
        claimed = self._store.claim_next_job(
            request.worker_id, instance_id=request.instance_id
        )
        if claimed is None:
            return pb.WorkerClaimResponse()
        job = claimed.job
        try:
            size = Path(job.input_path).stat().st_size
        except OSError:
            self._store.fail_job(
                job_id=job.job_id,
                worker_id=claimed.lease_owner,
                error="input_missing",
                retryable=False,
            )
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Input is unavailable")
        logger.info(
            "Worker claimed job: worker=%s job=%s attempt=%d",
            request.worker_id,
            job.job_id,
            job.attempts,
        )
        return pb.WorkerClaimResponse(
            job_id=job.job_id,
            lease_token=claimed.lease_owner,
            lease_seconds=self._store.lease_seconds,
            format=job.format,
            options=pb.TranscriptionOptions(
                preset=job.preset,
                language=job.language_override,
                initial_prompt=job.initial_prompt_override,
            ),
            input_size=size,
            traceparent=job.traceparent,
            tracestate=job.tracestate,
        )

    def ReportAvailability(self, request, context):
        self._authenticate(context)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", request.worker_id):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid worker ID")
        try:
            if str(uuid.UUID(request.instance_id)) != request.instance_id:
                raise ValueError()
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Invalid worker instance ID"
            )
        accepted = self._store.report_availability(
            request.worker_id, request.instance_id, request.ready
        )
        return pb.WorkerAvailabilityResponse(
            accepted=accepted, ttl_seconds=self._store.availability_ttl_seconds
        )

    def Download(self, request, context):
        job = self._owned(request, context)
        try:
            with Path(job.input_path).open("rb") as audio:
                while context.is_active():
                    if (
                        self._store.get_owned_job(request.job_id, request.lease_token)
                        is None
                    ):
                        context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "Lease is no longer owned",
                        )
                    chunk = audio.read(1024 * 1024)
                    if not chunk:
                        return
                    yield pb.WorkerAudioChunk(data=chunk)
        except OSError:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Input is unavailable")

    def Heartbeat(self, request, context):
        self._authenticate(context)
        if (
            not math.isfinite(request.progress_percent)
            or not 0 <= request.progress_percent <= 99
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Progress must be within 0..99"
            )
        accepted = self._store.heartbeat(
            request.job_id, request.lease_token, request.progress_percent
        )
        return pb.WorkerAck(accepted=accepted)

    def Complete(self, request, context):
        self._authenticate(context)
        if request.ByteSize() > MAX_RESULT_BYTES:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Result is too large")
        segments = []
        for segment in request.segments:
            if (
                not math.isfinite(segment.start)
                or not math.isfinite(segment.end)
                or not 0 <= segment.start <= segment.end
            ):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid segment timestamps"
                )
            segments.append(
                {"start": segment.start, "end": segment.end, "text": segment.text}
            )
        # PostgreSQL is the canonical result; no laptop path/shared mount required.
        accepted = self._store.complete_job(
            job_id=request.job_id,
            worker_id=request.lease_token,
            text=request.text,
            segments=segments,
            result_path="",
        )
        if accepted:
            logger.info(
                "Remote job completed: job=%s chars=%d",
                request.job_id,
                len(request.text),
            )
        return pb.WorkerAck(accepted=accepted)

    def Fail(self, request, context):
        self._authenticate(context)
        if request.error_code not in {
            "invalid_input",
            "inference_failed",
            "worker_stopping",
        }:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Unknown worker error code")
        accepted = self._store.fail_job(
            job_id=request.job_id,
            worker_id=request.lease_token,
            error=request.error_code,
            retryable=request.retryable,
        )
        return pb.WorkerAck(accepted=accepted)


def start_worker_api(store: JobStore):
    address = os.getenv("WORKER_GRPC_ADDR", "")
    if not address:
        return None
    token = read_worker_token(os.getenv("WORKER_TOKEN_FILE", ""))
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8),
        options=[
            ("grpc.max_receive_message_length", MAX_RESULT_BYTES),
            ("grpc.max_send_message_length", MAX_RESULT_BYTES),
        ],
    )
    rpc.add_WorkerServiceServicer_to_server(WorkerServicer(store, token), server)
    if not server.add_insecure_port(address):
        raise ValueError("Cannot bind worker endpoint")
    server.start()
    logger.info("Authenticated worker endpoint started")
    return server
