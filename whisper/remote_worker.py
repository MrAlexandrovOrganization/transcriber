"""Pull transcription work over a private gRPC connection without DB access."""

from __future__ import annotations

import dataclasses
import ipaddress
import logging
import math
import os
import random
import re
import signal
import socket
import tempfile
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import grpc
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from proto import whisper_pb2 as pb
from proto import whisper_pb2_grpc as rpc
from whisper.inference import FasterWhisperEngine, InferenceStopped
from whisper.jsonlog import setup_logging
from whisper.limits import MAX_RESULT_BYTES
from whisper.telemetry import context_from_carrier, init_telemetry

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Config:
    endpoint: str
    token_file: str
    worker_id: str
    poll_interval: float = 60.0
    rpc_timeout: float = 15.0
    download_timeout: float = 120.0
    max_input_bytes: int = 256 * 1024 * 1024
    allow_insecure: bool = False
    tls: bool = False
    tls_ca_file: str = ""
    shutdown_grace: float = 10.0

    def __post_init__(self):
        target = urlsplit("//" + self.endpoint)
        try:
            valid_port = target.port is not None and target.port > 0
        except ValueError:
            valid_port = False
        if (
            not target.hostname
            or not valid_port
            or target.username
            or target.password
            or target.path
            or target.query
            or target.fragment
        ):
            raise ValueError("REMOTE_WORKER_ENDPOINT must be host:port")
        if not self.token_file:
            raise ValueError("REMOTE_WORKER_TOKEN_FILE is required")
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", self.worker_id):
            raise ValueError("WORKER_ID must match [A-Za-z0-9_.:-]{1,128}")
        if not all(
            math.isfinite(value) and value > 0
            for value in (
                self.poll_interval,
                self.rpc_timeout,
                self.download_timeout,
                self.max_input_bytes,
                self.shutdown_grace,
            )
        ):
            raise ValueError("Worker limits must be finite and positive")
        loopback = target.hostname == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(target.hostname).is_loopback
        except ValueError:
            pass
        if not self.tls and not loopback and not self.allow_insecure:
            raise ValueError(
                "Remote plaintext requires REMOTE_WORKER_ALLOW_INSECURE=1 on a trusted VPN"
            )

    @classmethod
    def from_env(cls):
        return cls(
            endpoint=os.getenv("REMOTE_WORKER_ENDPOINT", "localhost:50055"),
            token_file=os.getenv("REMOTE_WORKER_TOKEN_FILE", ""),
            worker_id=os.getenv(
                "WORKER_ID",
                f"{re.sub(r'[^A-Za-z0-9_.:-]', '_', socket.gethostname())[:90]}-{uuid.uuid4().hex}",
            ),
            poll_interval=float(os.getenv("WORKER_POLL_INTERVAL_S", "60")),
            rpc_timeout=float(os.getenv("REMOTE_WORKER_RPC_TIMEOUT_S", "15")),
            download_timeout=float(
                os.getenv("REMOTE_WORKER_DOWNLOAD_TIMEOUT_S", "120")
            ),
            max_input_bytes=int(
                os.getenv("REMOTE_WORKER_MAX_INPUT_BYTES", str(256 * 1024 * 1024))
            ),
            allow_insecure=os.getenv("REMOTE_WORKER_ALLOW_INSECURE", "0") == "1",
            tls=os.getenv("REMOTE_WORKER_TLS", "0") == "1",
            tls_ca_file=os.getenv("REMOTE_WORKER_TLS_CA_FILE", ""),
            shutdown_grace=float(os.getenv("REMOTE_WORKER_SHUTDOWN_GRACE_S", "10")),
        )


def read_token(path: str) -> str:
    with open(path, "rb") as file:
        value = file.read(4097)
    token = value.strip()
    if (
        not 32 <= len(token) <= 512
        or len(value) > 4096
        or any(c < 33 or c > 126 for c in token)
    ):
        raise ValueError(
            "Worker token file must contain 32..512 printable ASCII characters without whitespace"
        )
    return token.decode("ascii")


def open_channel(config: Config):
    if config.tls:
        roots = Path(config.tls_ca_file).read_bytes() if config.tls_ca_file else None
        return grpc.secure_channel(config.endpoint, grpc.ssl_channel_credentials(roots))
    return grpc.insecure_channel(config.endpoint)


class RemoteWorker:
    def __init__(
        self,
        config: Config,
        stub,
        engine,
        token: str,
        stop: threading.Event | None = None,
    ):
        self.config = config
        self.stub = stub
        self.engine = engine
        self.stop = stop if stop is not None else threading.Event()
        self._metadata = (("authorization", "Bearer " + token),)

    def _call(self, method, request):
        return method(request, timeout=self.config.rpc_timeout, metadata=self._metadata)

    def run_forever(self):
        failures = 0
        while not self.stop.is_set():
            try:
                claim = self._call(
                    self.stub.Claim,
                    pb.WorkerClaimRequest(worker_id=self.config.worker_id),
                )
                failures = 0
                if claim.job_id:
                    self.process(claim)
                else:
                    self.stop.wait(self.config.poll_interval)
            except grpc.RpcError:
                failures += 1
                delay = min(60.0, 2.0 ** min(failures, 6)) * random.uniform(0.8, 1.2)
                logger.warning("Worker coordinator unavailable; retry scheduled")
                self.stop.wait(delay)
            except Exception:
                logger.error("Worker iteration failed; retry scheduled")
                self.stop.wait(self.config.poll_interval)

    def process(self, claim):
        if self.stop.is_set():
            return
        lease_lost = threading.Event()
        finished = threading.Event()
        progress = [0.0]
        request = pb.WorkerJobRequest(
            job_id=claim.job_id, lease_token=claim.lease_token
        )
        if not claim.lease_token or claim.lease_seconds <= 0:
            logger.error("Coordinator returned invalid lease")
            return

        def heartbeat():
            period = min(float(claim.lease_seconds) / 3.0, 30.0)
            next_heartbeat = 0.0
            while not finished.is_set():
                if self.stop.is_set():
                    lease_lost.set()
                    return
                if time.monotonic() >= next_heartbeat:
                    try:
                        ack = self._call(
                            self.stub.Heartbeat,
                            pb.WorkerHeartbeatRequest(
                                job_id=claim.job_id,
                                lease_token=claim.lease_token,
                                progress_percent=progress[0],
                            ),
                        )
                        if not ack.accepted:
                            lease_lost.set()
                            return
                    except Exception:
                        lease_lost.set()
                        return
                    next_heartbeat = time.monotonic() + period
                finished.wait(min(period, 0.25))

        thread = threading.Thread(
            target=heartbeat, name="worker-heartbeat", daemon=True
        )
        thread.start()
        tracer = trace.get_tracer("transcriber.remote-worker")
        try:
            with tracer.start_as_current_span(
                "transcription.process",
                kind=SpanKind.CONSUMER,
                context=context_from_carrier(claim.traceparent, claim.tracestate),
            ):
                self._process(request, claim, lease_lost, progress)
        except InferenceStopped:
            logger.info("Worker task stopped or lease lost")
        except grpc.RpcError:
            logger.warning("Worker task connection interrupted; lease will expire")
        except ValueError:
            self._fail(request, lease_lost, "invalid_input", False)
        except Exception:
            self._fail(request, lease_lost, "inference_failed", True)
        finally:
            finished.set()
            thread.join(timeout=self.config.rpc_timeout + 1.0)

    def _process(self, request, claim, lease_lost, progress):
        if claim.input_size <= 0 or claim.input_size > self.config.max_input_bytes:
            raise ValueError("Invalid input size")
        # Never derive a filesystem path from remote job IDs or untrusted formats.
        with tempfile.TemporaryDirectory(prefix="transcriber-worker-") as temp:
            input_path = Path(temp) / "input.audio"
            stream = self.stub.Download(
                request, timeout=self.config.download_timeout, metadata=self._metadata
            )
            try:
                size = 0
                with input_path.open("wb") as output:
                    for chunk in stream:
                        if lease_lost.is_set() or self.stop.is_set():
                            raise InferenceStopped()
                        size += len(chunk.data)
                        if (
                            size > self.config.max_input_bytes
                            or size > claim.input_size
                        ):
                            raise ValueError("Input too large")
                        output.write(chunk.data)
                if size != claim.input_size:
                    raise ValueError("Incomplete input")
            finally:
                if hasattr(stream, "cancel"):
                    stream.cancel()
            if lease_lost.is_set() or self.stop.is_set():
                raise InferenceStopped()
            result = self.engine.transcribe(
                str(input_path),
                preset=claim.options.preset,
                language=claim.options.language,
                initial_prompt=claim.options.initial_prompt,
                on_progress=lambda value: progress.__setitem__(0, value),
                should_stop=lease_lost,
            )
            if lease_lost.is_set() or self.stop.is_set():
                raise InferenceStopped()
            completion = pb.WorkerCompleteRequest(
                job_id=request.job_id,
                lease_token=request.lease_token,
                text=result.text,
                segments=[pb.Segment(**segment) for segment in result.segments],
            )
            if completion.ByteSize() > MAX_RESULT_BYTES:
                raise ValueError("Transcription result exceeds protocol limit")
            ack = self._call(self.stub.Complete, completion)
            logger.info(
                "Worker result accepted"
                if ack.accepted
                else "Worker result rejected after lease loss"
            )

    def _fail(self, request, lease_lost, code, retryable):
        if lease_lost.is_set() or self.stop.is_set():
            return
        logger.warning("Worker task failed (%s)", code)
        try:
            self._call(
                self.stub.Fail,
                pb.WorkerFailRequest(
                    job_id=request.job_id,
                    lease_token=request.lease_token,
                    error_code=code,
                    retryable=retryable,
                ),
            )
        except grpc.RpcError:
            logger.warning("Worker failure acknowledgement unavailable")


def main():
    setup_logging("remote-worker")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        config = Config.from_env()
        token = read_token(config.token_file)
        # Load dependencies and model before leasing any task.
        engine = FasterWhisperEngine()
        shutdown_telemetry = init_telemetry("whisper-remote-worker")
        with open_channel(config) as channel:
            worker = RemoteWorker(
                config, rpc.WorkerServiceStub(channel), engine, token, stop
            )
            thread = threading.Thread(
                target=worker.run_forever, daemon=True, name="remote-worker"
            )
            thread.start()
            while thread.is_alive() and not stop.wait(0.5):
                pass
            thread.join(timeout=config.shutdown_grace)
        shutdown_telemetry()
        return 0
    except Exception:
        logger.error(
            "Remote worker startup failed; check configuration and local model availability"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
