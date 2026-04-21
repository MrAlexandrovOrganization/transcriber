"""gRPC server for the Whisper transcription service."""

import dataclasses
import logging
import os
import queue
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator

import grpc

from proto import whisper_pb2, whisper_pb2_grpc

logger = logging.getLogger(__name__)

# Completed jobs are kept in memory for this long before cleanup.
_JOB_TTL_S = int(os.getenv("JOB_TTL_S", "7200"))  # 2 hours

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


class _CancelledError(Exception):
    """Raised inside the worker when a job is cancelled mid-transcription."""


@dataclasses.dataclass
class _Job:
    job_id: str
    tmp_path: str
    fmt: str
    preset: str = "auto"
    language_override: str = ""
    initial_prompt_override: str = ""
    status: str = "pending"  # pending | running | done | failed
    text: str = ""
    error: str = ""
    segments: list[tuple[float, float, str]] = dataclasses.field(default_factory=list)
    progress_percent: float = 0.0
    finished_at: float = 0.0  # time.monotonic() when done/failed, 0 while pending
    cancelled: bool = False


class TranscriptionServicer(whisper_pb2_grpc.TranscriptionServiceServicer):
    def __init__(self):
        from faster_whisper import WhisperModel

        model_size = os.getenv("WHISPER_MODEL", "small")
        cpu_threads = int(os.getenv("WHISPER_CPU_THREADS", "2"))
        logger.info(
            "Loading Whisper model '%s' (cpu_threads=%d)...", model_size, cpu_threads
        )
        self._model = WhisperModel(
            model_size, device="cpu", compute_type="int8", cpu_threads=cpu_threads
        )
        logger.info("Whisper model loaded.")

        self._jobs: dict[str, _Job] = {}
        self._jobs_lock = threading.Lock()
        self._job_queue: queue.Queue[_Job] = queue.Queue()

        threading.Thread(
            target=self._worker, daemon=True, name="whisper-worker"
        ).start()
        threading.Thread(
            target=self._cleanup, daemon=True, name="whisper-cleanup"
        ).start()

    # ------------------------------------------------------------------
    # Synchronous RPC (legacy) — routes through the same queue, blocks.
    # ------------------------------------------------------------------

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
                whisper_pb2.StatusRequest(job_id=job_id), context
            )
            if status_resp.status == whisper_pb2.DONE:
                return whisper_pb2.TranscribeResponse(text=status_resp.text)
            if status_resp.status == whisper_pb2.FAILED:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(status_resp.error)
                return whisper_pb2.TranscribeResponse()

    # ------------------------------------------------------------------
    # Async RPC — Submit
    # ------------------------------------------------------------------

    def Submit(
        self,
        request_iterator: Iterator[whisper_pb2.TranscribeChunk],
        context: grpc.ServicerContext,
    ) -> whisper_pb2.SubmitResponse:
        tmp_path, fmt, opts = self._receive_file(request_iterator, context)
        if tmp_path is None:
            return whisper_pb2.SubmitResponse()

        preset = (opts.preset or "auto") if opts else "auto"
        language_override = opts.language if opts else ""
        initial_prompt_override = opts.initial_prompt if opts else ""

        job = _Job(
            job_id=str(uuid.uuid4()),
            tmp_path=tmp_path,
            fmt=fmt,
            preset=preset,
            language_override=language_override,
            initial_prompt_override=initial_prompt_override,
        )
        with self._jobs_lock:
            self._jobs[job.job_id] = job
            self._job_queue.put(job)
            queue_position = self._job_queue.qsize()
        logger.info(
            "Job submitted: id=%s preset=%s position=%d",
            job.job_id,
            job.preset,
            queue_position,
        )
        return whisper_pb2.SubmitResponse(
            job_id=job.job_id, queue_position=queue_position
        )

    # ------------------------------------------------------------------
    # Async RPC — GetStatus
    # ------------------------------------------------------------------

    def GetStatus(
        self,
        request: whisper_pb2.StatusRequest,
        context: grpc.ServicerContext,
    ) -> whisper_pb2.StatusResponse:
        with self._jobs_lock:
            job = self._jobs.get(request.job_id)

        if job is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job not found: {request.job_id}")
            return whisper_pb2.StatusResponse()

        _status_map = {
            "pending": whisper_pb2.PENDING,
            "running": whisper_pb2.RUNNING,
            "done": whisper_pb2.DONE,
            "failed": whisper_pb2.FAILED,
        }
        segments = [
            whisper_pb2.Segment(start=s, end=e, text=t) for s, e, t in job.segments
        ]
        return whisper_pb2.StatusResponse(
            job_id=job.job_id,
            status=_status_map[job.status],
            text=job.text,
            error=job.error,
            segments=segments,
            progress_percent=job.progress_percent,
        )

    # ------------------------------------------------------------------
    # Async RPC — Cancel
    # ------------------------------------------------------------------

    def Cancel(
        self,
        request: whisper_pb2.CancelRequest,
        context: grpc.ServicerContext,
    ) -> whisper_pb2.CancelResponse:
        with self._jobs_lock:
            job = self._jobs.get(request.job_id)
            if job is None or job.status in ("done", "failed"):
                return whisper_pb2.CancelResponse(cancelled=False)
            job.cancelled = True

        logger.info("Job cancel requested: %s (status=%s)", job.job_id, job.status)
        return whisper_pb2.CancelResponse(cancelled=True)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _receive_file(
        self,
        request_iterator: Iterator[whisper_pb2.TranscribeChunk],
        context: grpc.ServicerContext,
    ) -> tuple[str | None, str, whisper_pb2.TranscriptionOptions | None]:
        first_chunk = next(request_iterator, None)
        if first_chunk is None or not first_chunk.format:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("First chunk must contain a non-empty format field")
            return None, "", None

        fmt = first_chunk.format
        opts = first_chunk.options if first_chunk.HasField("options") else None
        with tempfile.NamedTemporaryFile(suffix=f".{fmt}", delete=False) as tmp:
            tmp_path = tmp.name
            tmp.write(first_chunk.data)
            for chunk in request_iterator:
                tmp.write(chunk.data)

        logger.info("File received: path=%s format=%s", tmp_path, fmt)
        return tmp_path, fmt, opts

    def _transcribe_file(self, job: _Job) -> tuple[str, list[tuple[float, float, str]]]:
        params = dict(PRESETS.get(job.preset, PRESETS["voice"]))
        if job.language_override:
            params["language"] = job.language_override
        if job.initial_prompt_override:
            params["initial_prompt"] = job.initial_prompt_override

        include_segments = job.preset == "lecture"

        segments_iter, info = self._model.transcribe(job.tmp_path, **params)
        total_duration = max(float(getattr(info, "duration", 0.0) or 0.0), 0.0)
        parts: list[str] = []
        segs: list[tuple[float, float, str]] = []
        for seg in segments_iter:
            with self._jobs_lock:
                if job.cancelled:
                    raise _CancelledError()
            text = seg.text.strip()
            parts.append(text)
            if include_segments:
                segs.append((seg.start, seg.end, text))
            if total_duration > 0:
                progress = min(max((float(seg.end) / total_duration) * 100.0, 0.0), 99.0)
                with self._jobs_lock:
                    job.progress_percent = progress

        return " ".join(parts).strip(), segs

    def _worker(self) -> None:
        """Single background thread — one transcription at a time."""
        while True:
            job = self._job_queue.get()
            with self._jobs_lock:
                if job.cancelled:
                    logger.info("Job skipped (cancelled before start): %s", job.job_id)
                    job.status = "failed"
                    job.error = "cancelled"
                    job.finished_at = time.monotonic()
                else:
                    logger.info(
                        "Processing job: %s (preset=%s)", job.job_id, job.preset
                    )
                    job.status = "running"
                    job.progress_percent = 0.0

            if job.status == "failed":
                _safe_unlink(job.tmp_path)
                self._job_queue.task_done()
                continue

            try:
                text, segs = self._transcribe_file(job)
                logger.info(
                    "Job done: id=%s chars=%d segments=%d",
                    job.job_id,
                    len(text),
                    len(segs),
                )
                with self._jobs_lock:
                    job.status = "done"
                    job.text = text
                    job.segments = segs
                    job.progress_percent = 100.0
            except _CancelledError:
                logger.info("Job cancelled mid-transcription: %s", job.job_id)
                with self._jobs_lock:
                    job.status = "failed"
                    job.error = "cancelled"
                    job.progress_percent = 0.0
            except Exception as e:
                logger.error("Job failed: id=%s error=%s", job.job_id, e)
                with self._jobs_lock:
                    job.status = "failed"
                    job.error = str(e)
                    job.progress_percent = 0.0
            finally:
                job.finished_at = time.monotonic()
                _safe_unlink(job.tmp_path)
                self._job_queue.task_done()

    def _cleanup(self) -> None:
        """Periodically remove finished jobs older than JOB_TTL_S."""
        while True:
            time.sleep(600)
            now = time.monotonic()
            with self._jobs_lock:
                expired = [
                    jid
                    for jid, j in self._jobs.items()
                    if j.finished_at > 0 and (now - j.finished_at) > _JOB_TTL_S
                ]
                for jid in expired:
                    del self._jobs[jid]
            if expired:
                logger.info("Cleaned up %d expired jobs", len(expired))


def _safe_unlink(path: str | None) -> None:
    if path:
        try:
            os.unlink(path)
        except OSError:
            pass
