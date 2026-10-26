"""Authenticated worker-only gRPC endpoint; database/files stay on the ingress."""

from __future__ import annotations

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
from whisper.worker_auth import WorkerCredentials

logger = logging.getLogger(__name__)


class WorkerServicer(rpc.WorkerServiceServicer):
    def __init__(self, store: JobStore, credentials: WorkerCredentials):
        self._store = store
        self._credentials = credentials

    def _authenticate(self, context, *, worker_id=None, lease_token=None):
        values = [
            m.value for m in context.invocation_metadata() if m.key == "authorization"
        ]
        identity = (
            self._credentials.authenticate(values[0]) if len(values) == 1 else None
        )
        if identity is None:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Worker authentication required"
            )
        if (worker_id is not None and identity != worker_id) or (
            lease_token is not None and not lease_token.startswith(identity + "/")
        ):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Worker identity mismatch")

    def _owned(self, request, context):
        self._authenticate(context, lease_token=request.lease_token)
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
        self._authenticate(context, worker_id=request.worker_id)
        if self._store.queue_backend == "rabbitmq":
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Use RabbitMQ delivery and BeginAttempt",
            )
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
        return self._claim_response(claimed, context)

    def _claim_response(self, claimed, context):
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
            "Worker began job: worker=%s job=%s attempt=%d",
            job.lease_owner.split("/")[0],
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

    def _consumer_identity(self, request, context):
        self._authenticate(context, worker_id=request.worker_id)
        try:
            if str(uuid.UUID(request.instance_id)) != request.instance_id:
                raise ValueError()
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Invalid worker instance ID"
            )
        if self._store.queue_backend != "rabbitmq":
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION, "RabbitMQ is not enabled"
            )

    def RegisterConsumer(self, request, context):
        self._consumer_identity(request, context)
        priority = self._store.register_consumer(request.worker_id, request.instance_id)
        return pb.WorkerConsumerResponse(
            admitted=priority is not None, priority=priority or 0
        )

    def BeginAttempt(self, request, context):
        self._consumer_identity(request, context)
        for value in (request.job_id, request.message_id, request.delivery_id):
            try:
                if str(uuid.UUID(value)) != value:
                    raise ValueError()
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid delivery identity"
                )
        disposition, claimed = self._store.begin_attempt(
            request.worker_id,
            request.instance_id,
            request.job_id,
            request.message_id,
            request.delivery_id,
        )
        return pb.WorkerBeginResponse(
            disposition=disposition,
            job=self._claim_response(claimed, context) if claimed else None,
        )

    def ReportAvailability(self, request, context):
        self._authenticate(context, worker_id=request.worker_id)
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
        self._authenticate(context, lease_token=request.lease_token)
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
        self._authenticate(context, lease_token=request.lease_token)
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
        self._authenticate(context, lease_token=request.lease_token)
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


def start_worker_apis(store: JobStore):
    """Start the scoped worker endpoint behind Caddy, or a loopback test endpoint."""
    if os.getenv("WORKER_GRPC_ADDR") or os.getenv("WORKER_TOKEN_FILE"):
        raise ValueError(
            "Shared-token worker listener was removed; configure scoped credentials"
        )
    listeners = []
    public_address = os.getenv("WORKER_PUBLIC_GRPC_ADDR", "")
    if public_address:
        listeners.append(
            (
                public_address,
                WorkerCredentials.from_file(os.getenv("WORKER_CREDENTIALS_FILE", "")),
            )
        )
    servers = []
    try:
        for address, credentials in listeners:
            server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=8),
                maximum_concurrent_rpcs=32,
                options=[
                    ("grpc.max_receive_message_length", MAX_RESULT_BYTES),
                    ("grpc.max_send_message_length", MAX_RESULT_BYTES),
                ],
            )
            rpc.add_WorkerServiceServicer_to_server(
                WorkerServicer(store, credentials), server
            )
            if not server.add_insecure_port(address):
                raise ValueError("Cannot bind worker endpoint")
            server.start()
            servers.append(server)
            logger.info("Authenticated worker endpoint started")
        return servers
    except Exception:
        for server in servers:
            server.stop(0)
        raise
