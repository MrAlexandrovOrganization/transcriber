"""gRPC ingress and durable worker for the Whisper transcription service."""

from __future__ import annotations

import contextlib
import logging
import os
import re
import signal
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import grpc
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from proto import whisper_pb2, whisper_pb2_grpc
from whisper.inference import FasterWhisperEngine, InferenceStopped
from whisper.storage import (
    ClaimedJob,
    JobRecord,
    JobStore,
    UploadBusy,
    UploadConflict,
    UploadReservation,
)
from whisper.telemetry import context_from_carrier, current_trace_carrier
from whisper.worker_api import start_worker_api

logger = logging.getLogger(__name__)

_JOB_TTL_S = int(os.getenv("JOB_TTL_S", "7200"))
_WORKER_POLL_INTERVAL_S = float(os.getenv("WORKER_POLL_INTERVAL_S", "2"))
_CLEANUP_INTERVAL_S = int(os.getenv("CLEANUP_INTERVAL_S", "600"))

_STATUS_MAP = {
    "accepted": whisper_pb2.ACCEPTED,
    "downloading": whisper_pb2.DOWNLOADING,
    "queued": whisper_pb2.QUEUED,
    "running": whisper_pb2.RUNNING,
    "done": whisper_pb2.DONE,
    "failed": whisper_pb2.FAILED,
}


class TranscriptionServicer(whisper_pb2_grpc.TranscriptionServiceServicer):
    def __init__(self, store: JobStore):
        self._store = store

    def Transcribe(
        self,
        request_iterator: Iterator[whisper_pb2.TranscribeChunk],
        context: grpc.ServicerContext,
    ) -> whisper_pb2.TranscribeResponse:
        submit_resp = self.Submit(request_iterator, context)
        if not submit_resp.job_id:
            return whisper_pb2.TranscribeResponse()

        job_id = submit_resp.job_id
        while context.is_active():
            time.sleep(1)
            status_resp = self.GetStatus(
                whisper_pb2.StatusRequest(job_id=job_id),
                context,
            )
            if status_resp.status == whisper_pb2.DONE:
                return whisper_pb2.TranscribeResponse(text=status_resp.text)
            if status_resp.status == whisper_pb2.FAILED:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(status_resp.error)
                return whisper_pb2.TranscribeResponse()
        context.abort(grpc.StatusCode.CANCELLED, "Transcription request closed")

    def Submit(
        self,
        request_iterator: Iterator[whisper_pb2.TranscribeChunk],
        context: grpc.ServicerContext,
    ) -> whisper_pb2.SubmitResponse:
        first_chunk = next(request_iterator, None)
        if first_chunk is None or not first_chunk.format:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("First chunk must contain a non-empty format field")
            return whisper_pb2.SubmitResponse()

        fmt = first_chunk.format
        if not re.fullmatch(r"[a-zA-Z0-9]{1,12}", fmt):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid audio format")
        # Existing clients had no total-upload limit. Keep it opt in for the
        # ingress; the isolated pilot sets its own small synthetic input limit.
        max_audio_bytes = int(os.getenv("MAX_AUDIO_BYTES", "0"))
        opts = first_chunk.options if first_chunk.HasField("options") else None
        preset = (opts.preset or "auto") if opts else "auto"
        language_override = opts.language if opts else ""
        initial_prompt_override = opts.initial_prompt if opts else ""

        request_id = first_chunk.request_id
        if request_id and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", request_id):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload request ID")
        trace_carrier = current_trace_carrier()
        try:
            with self._store.reserve_upload(
                request_id=request_id,
                retain_until_ack=first_chunk.retain_until_ack,
                fmt=fmt,
                preset=preset,
                language_override=language_override,
                initial_prompt_override=initial_prompt_override,
                traceparent=trace_carrier.get("traceparent", ""),
                tracestate=trace_carrier.get("tracestate", ""),
            ) as upload:
                return self._submit_reserved(
                    upload,
                    first_chunk,
                    request_iterator,
                    context,
                    max_audio_bytes,
                )
        except UploadBusy:
            context.abort(
                grpc.StatusCode.ABORTED,
                "Upload already in progress; retry the same request ID",
            )
        except UploadConflict:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Request ID was reused with different upload options",
            )

    def _submit_reserved(
        self,
        upload: UploadReservation,
        first_chunk,
        request_iterator,
        context,
        max_audio_bytes,
    ):
        job = upload.job
        if not upload.needs_upload:
            position = (
                self._store.queue_position(job.job_id)
                if job.status in {"queued", "running"}
                else 0
            )
            return whisper_pb2.SubmitResponse(
                job_id=job.job_id, queue_position=position
            )
        self._store.mark_downloading(job.job_id)
        # Only the holder of this request's session lock can touch partials.
        for previous in upload.input_path.parent.glob("upload-*.part"):
            previous.unlink(missing_ok=True)
        temporary = upload.input_path.with_name(f"upload-{uuid.uuid4().hex}.part")
        too_large = False
        try:
            with temporary.open("xb") as audio:
                total = 0
                for chunk in _prepend(first_chunk, request_iterator):
                    total += len(chunk.data)
                    if max_audio_bytes > 0 and total > max_audio_bytes:
                        too_large = True
                        raise ValueError("input_too_large")
                    audio.write(chunk.data)
                # gRPC may end the iterator without raising on cancellation.
                # Never publish a truncated stream as a complete input file.
                if not context.is_active():
                    raise InterruptedError("Upload interrupted")
                audio.flush()
                os.fsync(audio.fileno())
            os.replace(temporary, upload.input_path)
            directory_fd = os.open(upload.input_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except Exception:
            temporary.unlink(missing_ok=True)
            if job.request_id and not too_large:
                self._store.mark_upload_interrupted(job.job_id)
            else:
                self._store.fail_job(
                    job_id=job.job_id,
                    worker_id="",
                    error="input_too_large" if too_large else "upload_failed",
                    retryable=False,
                )
            logger.error("Failed to persist upload for job %s", job.job_id)
            context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED
                if too_large
                else grpc.StatusCode.INTERNAL,
                "Audio upload exceeds the configured limit"
                if too_large
                else "Upload interrupted; retry the same request ID",
            )
        queue_position = self._store.mark_queued(job.job_id)
        if queue_position is None:
            context.abort(grpc.StatusCode.CANCELLED, "Upload was cancelled")
        # Query after the state transition to include the new queued row.
        queue_position = self._store.queue_position(job.job_id)
        logger.info(
            "Job submitted: id=%s queue_position=%d", job.job_id, queue_position
        )
        return whisper_pb2.SubmitResponse(
            job_id=job.job_id, queue_position=queue_position
        )

    def GetStatus(
        self,
        request: whisper_pb2.StatusRequest,
        context: grpc.ServicerContext,
    ) -> whisper_pb2.StatusResponse:
        job = self._store.get_job(request.job_id)
        if job is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job not found: {request.job_id}")
            return whisper_pb2.StatusResponse()

        segments = [
            whisper_pb2.Segment(
                start=float(segment.get("start", 0.0)),
                end=float(segment.get("end", 0.0)),
                text=str(segment.get("text", "")),
            )
            for segment in job.segments
        ]
        return whisper_pb2.StatusResponse(
            job_id=job.job_id,
            status=_STATUS_MAP[job.status],
            stage=job.status,
            text=job.text,
            error=job.error,
            segments=segments,
            progress_percent=job.progress_percent,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
        )

    def Cancel(
        self,
        request: whisper_pb2.CancelRequest,
        context: grpc.ServicerContext,
    ) -> whisper_pb2.CancelResponse:
        cancelled = self._store.cancel_job(request.job_id)
        if cancelled:
            logger.info("Job cancel requested: %s", request.job_id)
        return whisper_pb2.CancelResponse(cancelled=cancelled)

    def Ack(self, request, context):
        if not request.job_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Job ID required")
        if self._store.acknowledge_job(request.job_id):
            return whisper_pb2.AckResponse(acknowledged=True)
        if self._store.get_job(request.job_id) is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Job not found")
        context.abort(
            grpc.StatusCode.FAILED_PRECONDITION,
            "Only terminal jobs can be acknowledged",
        )


def _prepend(first, remaining):
    yield first
    yield from remaining


class Worker:
    def __init__(self, store: JobStore):
        self._store = store
        self._worker_id = os.getenv("WORKER_ID", f"worker-{uuid.uuid4()}")
        fallback = os.getenv("WORKER_FALLBACK", "0")
        if fallback not in {"0", "1"}:
            raise ValueError("WORKER_FALLBACK must be 0 or 1")
        self._fallback = fallback == "1"
        self._engine = FasterWhisperEngine()

    def run_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        while not stop.is_set():
            try:
                claimed = self._store.claim_next_job(
                    self._worker_id, fallback=self._fallback
                )
                if claimed is not None:
                    self._process_claimed_job(claimed, stop)
                    continue
            except Exception:
                # Connection errors can contain a DSN; do not log exception text.
                logger.error("Worker queue operation failed")
            stop.wait(_WORKER_POLL_INTERVAL_S)

    def _process_claimed_job(self, claimed: ClaimedJob, stop: threading.Event) -> None:
        job = claimed.job
        lease_lost = threading.Event()
        finished = threading.Event()
        progress = [0.0]

        def heartbeat() -> None:
            while not finished.is_set():
                try:
                    if not self._store.heartbeat(
                        job.job_id, claimed.lease_owner, progress[0]
                    ):
                        lease_lost.set()
                        return
                except Exception:
                    lease_lost.set()
                    return
                if finished.wait(min(30.0, self._store.lease_seconds / 3)):
                    return

        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()
        parent_context = context_from_carrier(job.traceparent, job.tracestate)
        tracer = trace.get_tracer("transcriber.worker")
        with tracer.start_as_current_span(
            "transcription.process",
            context=parent_context,
            kind=SpanKind.CONSUMER,
            attributes={
                "transcription.job.id": job.job_id,
                "transcription.attempt": job.attempts,
            },
        ) as span:
            try:
                result = self._engine.transcribe(
                    job.input_path,
                    preset=job.preset,
                    language=job.language_override,
                    initial_prompt=job.initial_prompt_override,
                    on_progress=lambda value: progress.__setitem__(0, value),
                    should_stop=_AnyEvent(stop, lease_lost),
                )
                if stop.is_set() or lease_lost.is_set():
                    raise InferenceStopped()
                accepted = self._store.complete_job(
                    job_id=job.job_id,
                    worker_id=claimed.lease_owner,
                    text=result.text,
                    segments=result.segments,
                    result_path="",
                )
                logger.info(
                    "Local job completion: job=%s accepted=%s", job.job_id, accepted
                )
            except InferenceStopped:
                logger.info("Local job stopped: job=%s", job.job_id)
            except Exception:
                span.set_status(Status(StatusCode.ERROR, "transcription_failed"))
                logger.error("Local inference failed: job=%s", job.job_id)
                self._store.fail_job(
                    job_id=job.job_id,
                    worker_id=claimed.lease_owner,
                    error="transcription_failed",
                    retryable=True,
                )
            finally:
                finished.set()
                heartbeat_thread.join(timeout=5)


class _AnyEvent:
    def __init__(self, *events: threading.Event):
        self._events = events

    def is_set(self) -> bool:
        return any(event.is_set() for event in self._events)


class CleanupWorker:
    def __init__(self, store: JobStore):
        self._store = store

    def run_forever(self) -> None:
        while True:
            time.sleep(_CLEANUP_INTERVAL_S)
            try:
                deleted = self._store.cleanup_expired_jobs(_JOB_TTL_S)
                if deleted:
                    logger.debug("Cleaned up %d expired jobs", deleted)
            except Exception:
                logger.error("Queue cleanup failed")


def run_ingress() -> None:
    from concurrent import futures

    store = JobStore()
    store.init_schema()

    port = os.getenv("GRPC_PORT", "50053")
    grpc_workers = int(os.getenv("GRPC_WORKERS", "4"))
    max_message_size = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(50 * 1024 * 1024)))
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=grpc_workers),
        options=[
            ("grpc.max_receive_message_length", max_message_size),
            ("grpc.max_send_message_length", max_message_size),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    whisper_pb2_grpc.add_TranscriptionServiceServicer_to_server(
        TranscriptionServicer(store),
        server,
    )
    if not server.add_insecure_port(f"{os.getenv('GRPC_BIND_HOST', '[::]')}:{port}"):
        raise ValueError("Cannot bind transcription endpoint")
    worker_server = start_worker_api(store)
    threading.Thread(
        target=CleanupWorker(store).run_forever, daemon=True, name="cleanup-worker"
    ).start()
    server.start()

    def stop_servers(*_):
        if worker_server is not None:
            worker_server.stop(5)
        server.stop(5)

    signal.signal(signal.SIGTERM, stop_servers)
    signal.signal(signal.SIGINT, stop_servers)
    logger.info("Whisper ingress gRPC server started on port %s", port)
    server.wait_for_termination()


def run_worker() -> None:
    store = JobStore()
    store.init_schema()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    Worker(store).run_forever(stop)


def safe_remove_job_dir(job: JobRecord) -> None:
    job_dir = Path(job.input_path).parent
    with contextlib.suppress(OSError):
        job_dir.rmdir()
