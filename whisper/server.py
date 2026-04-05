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


@dataclasses.dataclass
class _Job:
    job_id: str
    tmp_path: str
    fmt: str
    status: str = "pending"   # pending | running | done | failed
    text: str = ""
    error: str = ""
    finished_at: float = 0.0  # time.monotonic() when done/failed, 0 while pending


class TranscriptionServicer(whisper_pb2_grpc.TranscriptionServiceServicer):
    def __init__(self):
        from faster_whisper import WhisperModel

        model_size = os.getenv("WHISPER_MODEL", "small")
        logger.info("Loading Whisper model '%s'...", model_size)
        self._model = WhisperModel(model_size, device="cpu", compute_type="int8")
        logger.info("Whisper model loaded.")

        self._jobs: dict[str, _Job] = {}
        self._jobs_lock = threading.Lock()
        self._job_queue: queue.Queue[_Job] = queue.Queue()

        threading.Thread(target=self._worker, daemon=True, name="whisper-worker").start()
        threading.Thread(target=self._cleanup, daemon=True, name="whisper-cleanup").start()

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
        tmp_path, fmt = self._receive_file(request_iterator, context)
        if tmp_path is None:
            return whisper_pb2.SubmitResponse()

        job = _Job(job_id=str(uuid.uuid4()), tmp_path=tmp_path, fmt=fmt)
        with self._jobs_lock:
            self._jobs[job.job_id] = job
        self._job_queue.put(job)

        # +1 because the item just entered the queue (qsize is post-put).
        queue_position = self._job_queue.qsize()
        logger.info("Job submitted: id=%s position=%d", job.job_id, queue_position)
        return whisper_pb2.SubmitResponse(job_id=job.job_id, queue_position=queue_position)

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
            "done":    whisper_pb2.DONE,
            "failed":  whisper_pb2.FAILED,
        }
        return whisper_pb2.StatusResponse(
            job_id=job.job_id,
            status=_status_map[job.status],
            text=job.text,
            error=job.error,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _receive_file(
        self,
        request_iterator: Iterator[whisper_pb2.TranscribeChunk],
        context: grpc.ServicerContext,
    ) -> tuple[str | None, str]:
        first_chunk = next(request_iterator, None)
        if first_chunk is None or not first_chunk.format:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("First chunk must contain a non-empty format field")
            return None, ""

        fmt = first_chunk.format
        with tempfile.NamedTemporaryFile(suffix=f".{fmt}", delete=False) as tmp:
            tmp_path = tmp.name
            tmp.write(first_chunk.data)
            for chunk in request_iterator:
                tmp.write(chunk.data)

        logger.info("File received: path=%s format=%s", tmp_path, fmt)
        return tmp_path, fmt

    def _transcribe_file(self, tmp_path: str) -> str:
        segments, _ = self._model.transcribe(
            tmp_path,
            language="ru",
            beam_size=5,
            vad_filter=True,
            temperature=0.0,
        )
        return " ".join(seg.text.strip() for seg in segments).strip()

    def _worker(self) -> None:
        """Single background thread — one transcription at a time."""
        while True:
            job = self._job_queue.get()
            logger.info("Processing job: %s", job.job_id)
            with self._jobs_lock:
                job.status = "running"
            try:
                text = self._transcribe_file(job.tmp_path)
                logger.info("Job done: id=%s chars=%d", job.job_id, len(text))
                with self._jobs_lock:
                    job.status = "done"
                    job.text = text
            except Exception as e:
                logger.error("Job failed: id=%s error=%s", job.job_id, e)
                with self._jobs_lock:
                    job.status = "failed"
                    job.error = str(e)
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
                    jid for jid, j in self._jobs.items()
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
