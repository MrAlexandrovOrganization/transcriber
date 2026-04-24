"""gRPC ingress and durable worker for the Whisper transcription service."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import os
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import grpc

from proto import whisper_pb2, whisper_pb2_grpc
from whisper.storage import ClaimedJob, JobRecord, JobStore

logger = logging.getLogger(__name__)

_JOB_TTL_S = int(os.getenv("JOB_TTL_S", "7200"))
_WORKER_POLL_INTERVAL_S = float(os.getenv("WORKER_POLL_INTERVAL_S", "2"))
_CLEANUP_INTERVAL_S = int(os.getenv("CLEANUP_INTERVAL_S", "600"))

# ---------------------------------------------------------------------------
# Transcription presets
# ---------------------------------------------------------------------------

_VOICE_PARAMS: dict = {
    "language": "ru",
    "beam_size": 5,
    "temperature": 0.0,
    "vad_filter": True,
    "vad_parameters": {"min_silence_duration_ms": 500, "speech_pad_ms": 200},
    "condition_on_previous_text": True,
    "initial_prompt": "Разговорная речь на русском языке. Текст с правильными знаками препинания.",
    "repetition_penalty": 1.05,
    "no_speech_threshold": 0.5,
}

_LECTURE_PARAMS: dict = {
    "language": "ru",
    "beam_size": 7,
    "temperature": 0.0,
    "vad_filter": True,
    "vad_parameters": {"min_silence_duration_ms": 500, "speech_pad_ms": 200},
    "condition_on_previous_text": True,
    "initial_prompt": (
        "Академическая лекция на русском языке. "
        "Текст с правильными знаками препинания и заглавными буквами."
    ),
    "repetition_penalty": 1.1,
    "hallucination_silence_threshold": 3.0,
    "no_speech_threshold": 0.5,
}

_MEETING_PARAMS: dict = {
    "language": "ru",
    "beam_size": 5,
    "temperature": 0.0,
    "vad_filter": True,
    "vad_parameters": {"min_silence_duration_ms": 300, "speech_pad_ms": 150},
    "condition_on_previous_text": True,
    "initial_prompt": "Деловая встреча или разговор на русском языке. Текст с правильными знаками препинания.",
    "repetition_penalty": 1.05,
    "no_speech_threshold": 0.4,
}

PRESETS: dict[str, dict] = {
    "auto": _VOICE_PARAMS,
    "voice": _VOICE_PARAMS,
    "lecture": _LECTURE_PARAMS,
    "meeting": _MEETING_PARAMS,
}

_STATUS_MAP = {
    "accepted": whisper_pb2.ACCEPTED,
    "downloading": whisper_pb2.DOWNLOADING,
    "queued": whisper_pb2.QUEUED,
    "running": whisper_pb2.RUNNING,
    "done": whisper_pb2.DONE,
    "failed": whisper_pb2.FAILED,
}


class _CancelledError(Exception):
    """Raised inside the worker when a job is cancelled mid-transcription."""


@dataclasses.dataclass(frozen=True)
class _TranscriptionResult:
    text: str
    segments: list[dict[str, float | str]]


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
        while True:
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
        opts = first_chunk.options if first_chunk.HasField("options") else None
        preset = (opts.preset or "auto") if opts else "auto"
        language_override = opts.language if opts else ""
        initial_prompt_override = opts.initial_prompt if opts else ""

        job, input_path = self._store.create_job(
            fmt=fmt,
            preset=preset,
            language_override=language_override,
            initial_prompt_override=initial_prompt_override,
        )
        self._store.mark_downloading(job.job_id)

        try:
            with input_path.open("wb") as fh:
                fh.write(first_chunk.data)
                for chunk in request_iterator:
                    fh.write(chunk.data)
        except Exception as exc:
            logger.exception("Failed to persist upload for job %s", job.job_id)
            self._store.fail_job(
                job_id=job.job_id,
                worker_id="",
                error=f"upload failed: {exc}",
                retryable=False,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to persist uploaded file")
            return whisper_pb2.SubmitResponse()

        queue_position = self._store.mark_queued(job.job_id)
        logger.info(
            "Job submitted: id=%s preset=%s queue_position=%d",
            job.job_id,
            preset,
            queue_position,
        )
        return whisper_pb2.SubmitResponse(job_id=job.job_id, queue_position=queue_position)

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


class Worker:
    def __init__(self, store: JobStore):
        from faster_whisper import WhisperModel

        self._store = store
        self._worker_id = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
        model_size = os.getenv("WHISPER_MODEL", "small")
        cpu_threads = int(os.getenv("WHISPER_CPU_THREADS", "2"))
        logger.info(
            "Loading Whisper model '%s' for worker %s (cpu_threads=%d)...",
            model_size,
            self._worker_id,
            cpu_threads,
        )
        self._model = WhisperModel(
            model_size,
            device="cpu",
            compute_type="int8",
            cpu_threads=cpu_threads,
        )
        logger.info("Whisper model loaded for worker %s.", self._worker_id)

    def run_forever(self) -> None:
        while True:
            claimed = self._store.claim_next_job(self._worker_id)
            if claimed is None:
                time.sleep(_WORKER_POLL_INTERVAL_S)
                continue
            self._process_claimed_job(claimed)

    def _process_claimed_job(self, claimed: ClaimedJob) -> None:
        job = claimed.job
        logger.info(
            "Processing job: id=%s preset=%s attempt=%d/%d",
            job.job_id,
            job.preset,
            job.attempts,
            job.max_attempts,
        )
        try:
            result = self._transcribe_file(job)
            result_path = self._write_result(job, result)
            self._store.complete_job(
                job_id=job.job_id,
                worker_id=self._worker_id,
                text=result.text,
                segments=result.segments,
                result_path=str(result_path),
            )
            logger.info(
                "Job done: id=%s chars=%d segments=%d",
                job.job_id,
                len(result.text),
                len(result.segments),
            )
        except _CancelledError:
            logger.info("Job cancelled mid-transcription: %s", job.job_id)
            self._store.fail_job(
                job_id=job.job_id,
                worker_id=self._worker_id,
                error="cancelled",
                retryable=False,
            )
        except Exception as exc:
            logger.exception("Job failed: id=%s", job.job_id)
            self._store.fail_job(
                job_id=job.job_id,
                worker_id=self._worker_id,
                error=str(exc),
                retryable=True,
            )

    def _transcribe_file(self, job: JobRecord) -> _TranscriptionResult:
        params = dict(PRESETS.get(job.preset, PRESETS["voice"]))
        if job.language_override:
            params["language"] = job.language_override
        if job.initial_prompt_override:
            params["initial_prompt"] = job.initial_prompt_override

        include_segments = job.preset == "lecture"
        segments_iter, info = self._model.transcribe(job.input_path, **params)
        total_duration = max(float(getattr(info, "duration", 0.0) or 0.0), 0.0)
        parts: list[str] = []
        segments: list[dict[str, float | str]] = []
        for seg in segments_iter:
            current = self._store.get_job(job.job_id)
            if current is None or current.status == "failed" and current.error == "cancelled":
                raise _CancelledError()
            text = seg.text.strip()
            parts.append(text)
            if include_segments:
                segments.append({"start": float(seg.start), "end": float(seg.end), "text": text})
            if total_duration > 0:
                progress = min(max((float(seg.end) / total_duration) * 100.0, 0.0), 99.0)
                self._store.heartbeat(job.job_id, self._worker_id, progress)

        return _TranscriptionResult(text=" ".join(parts).strip(), segments=segments)

    def _write_result(self, job: JobRecord, result: _TranscriptionResult) -> Path:
        result_path = Path(job.input_path).with_name("result.json")
        payload = {
            "job_id": job.job_id,
            "text": result.text,
            "segments": result.segments,
        }
        result_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return result_path


class CleanupWorker:
    def __init__(self, store: JobStore):
        self._store = store

    def run_forever(self) -> None:
        while True:
            time.sleep(_CLEANUP_INTERVAL_S)
            deleted = self._store.cleanup_expired_jobs(_JOB_TTL_S)
            if deleted:
                logger.info("Cleaned up %d expired jobs", deleted)


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
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("Whisper ingress gRPC server started on port %s", port)
    server.wait_for_termination()


def run_worker() -> None:
    store = JobStore()
    store.init_schema()
    cleanup = CleanupWorker(store)
    threading.Thread(target=cleanup.run_forever, daemon=True, name="cleanup-worker").start()
    Worker(store).run_forever()


def safe_remove_job_dir(job: JobRecord) -> None:
    job_dir = Path(job.input_path).parent
    with contextlib.suppress(OSError):
        job_dir.rmdir()
