"""Pull transcription work over a private gRPC connection without DB access."""

from __future__ import annotations

import asyncio
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
from whisper.limits import DEFAULT_MAX_AUDIO_BYTES, MAX_RESULT_BYTES, audio_limit
from whisper.scheduling import worker_concurrency
from whisper.telemetry import context_from_carrier, init_telemetry
from whisper.worker_health import report as report_health

logger = logging.getLogger(__name__)
DRAIN_FILE = Path("/tmp/worker-drained")


@dataclasses.dataclass(frozen=True)
class Config:
    endpoint: str
    token_file: str
    worker_id: str
    poll_interval: float = 2.0
    rpc_timeout: float = 15.0
    download_timeout: float = 120.0
    max_input_bytes: int = DEFAULT_MAX_AUDIO_BYTES
    tls: bool = False
    tls_ca_file: str = ""
    shutdown_grace: float = 10.0
    health_file: str = ""
    concurrency: int = 1
    broker_file: str = ""
    broker_ca_file: str = ""
    max_task_seconds: float = 21600.0

    def __post_init__(self):
        worker_concurrency(str(self.concurrency))
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
                self.max_task_seconds,
            )
        ):
            raise ValueError("Worker limits must be finite and positive")
        loopback = target.hostname == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(target.hostname).is_loopback
        except ValueError:
            pass
        if not self.tls and not loopback:
            raise ValueError("Remote workers require TLS outside loopback")

    @classmethod
    def from_env(cls):
        max_input_bytes = int(
            os.getenv("REMOTE_WORKER_MAX_INPUT_BYTES", str(audio_limit()))
        )
        if max_input_bytes < audio_limit():
            raise ValueError("REMOTE_WORKER_MAX_INPUT_BYTES must be >= MAX_AUDIO_BYTES")
        return cls(
            endpoint=os.getenv("REMOTE_WORKER_ENDPOINT", "localhost:50056"),
            token_file=os.getenv("REMOTE_WORKER_TOKEN_FILE", ""),
            worker_id=os.getenv(
                "WORKER_ID",
                f"{re.sub(r'[^A-Za-z0-9_.:-]', '_', socket.gethostname())[:90]}-{uuid.uuid4().hex}",
            ),
            poll_interval=float(os.getenv("WORKER_POLL_INTERVAL_S", "2")),
            rpc_timeout=float(os.getenv("REMOTE_WORKER_RPC_TIMEOUT_S", "15")),
            download_timeout=float(
                os.getenv("REMOTE_WORKER_DOWNLOAD_TIMEOUT_S", "120")
            ),
            max_input_bytes=max_input_bytes,
            tls=os.getenv("REMOTE_WORKER_TLS", "1") == "1",
            tls_ca_file=os.getenv("REMOTE_WORKER_TLS_CA_FILE", ""),
            shutdown_grace=float(os.getenv("REMOTE_WORKER_SHUTDOWN_GRACE_S", "10")),
            health_file=os.getenv("REMOTE_WORKER_HEALTH_FILE", ""),
            concurrency=worker_concurrency(os.getenv("WORKER_CONCURRENCY", "1")),
            broker_file=os.getenv("RABBITMQ_CONNECTION_FILE", ""),
            broker_ca_file=os.getenv("RABBITMQ_CA_FILE", ""),
            max_task_seconds=float(os.getenv("REMOTE_WORKER_MAX_TASK_S", "21600")),
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
        lifecycle=None,
    ):
        self.config = config
        self.stub = stub
        self.engine = engine
        self.stop = stop if stop is not None else threading.Event()
        self.instance_id = str(uuid.uuid4())
        self._available = threading.Event()
        self.drain = threading.Event()
        self._drained = threading.Event()
        self._metadata = (("authorization", "Bearer " + token),)
        self._lifecycle = lifecycle
        self._availability_lock = threading.Lock()
        self._broker_lost = threading.Event()
        self._broker_connected = threading.Event()
        self._broker_priority = 0
        self._broker_admitted = threading.Event()

    def _health(self, ready):
        if self._lifecycle is None:
            report_health(self.config.health_file, ready)
        else:
            self._lifecycle.health(self.instance_id, ready)

    def _mark_drained(self, drained):
        if self._lifecycle is None:
            if drained:
                DRAIN_FILE.touch()
            else:
                DRAIN_FILE.unlink(missing_ok=True)
        else:
            self._lifecycle.mark_drained(self.instance_id, drained)

    def _call(self, method, request):
        return method(request, timeout=self.config.rpc_timeout, metadata=self._metadata)

    def run_forever(self):
        failures = 0
        reporter = None
        broker = None
        if self.config.broker_file:
            from whisper.broker import BrokerConfig

            broker = BrokerConfig.from_file(
                self.config.broker_file, self.config.broker_ca_file
            )
        try:
            while not self.stop.is_set():
                try:
                    if self.drain.is_set():
                        # The synchronous process() has completed, including Complete/Fail.
                        # Serialize withdrawal with the reporter before acknowledging drain.
                        self._drained.set()
                        self._available.clear()
                        try:
                            self._report_availability(False)
                        except Exception:
                            self._health(False)
                        self._mark_drained(True)
                        while self.drain.is_set() and not self.stop.wait(0.25):
                            pass
                        self._mark_drained(False)
                        self._drained.clear()
                        if self.stop.is_set():
                            break
                        self._report_availability(True)
                        self._available.set()
                    if reporter is None:
                        interval = self._report_availability(True)
                        self._available.set()
                        reporter = threading.Thread(
                            target=self._report_availability_loop,
                            args=(interval,),
                            name="worker-availability",
                            daemon=True,
                        )
                        reporter.start()
                        logger.info(
                            "Worker availability registered: worker=%s slot=%s",
                            self.config.worker_id,
                            self.instance_id,
                        )
                    if not self._available.wait(0.5):
                        continue
                    if self.stop.is_set():
                        break
                    if self.drain.is_set():
                        continue
                    if broker:
                        if not self._broker_admitted.is_set():
                            self.stop.wait(self.config.poll_interval)
                            continue
                        from whisper.broker import consume_slot

                        asyncio.run(consume_slot(self, broker))
                        self.stop.wait(1)
                    else:
                        claim = self._call(
                            self.stub.Claim,
                            pb.WorkerClaimRequest(
                                worker_id=self.config.worker_id,
                                instance_id=self.instance_id,
                            ),
                        )
                        failures = 0
                        if claim.job_id:
                            self.process(claim)
                        else:
                            self.stop.wait(self.config.poll_interval)
                except grpc.RpcError:
                    failures += 1
                    delay = min(60.0, 2.0 ** min(failures, 6)) * random.uniform(
                        0.8, 1.2
                    )
                    logger.warning("Worker coordinator unavailable; retry scheduled")
                    self.stop.wait(delay)
                except Exception:
                    logger.error("Worker iteration failed; retry scheduled")
                    self.stop.wait(self.config.poll_interval)
        finally:
            self.stop.set()
            self._health(False)
            if reporter is not None:
                reporter.join(timeout=4)
                try:
                    self._report_availability(False)
                except Exception:
                    # A disconnected/crashed worker expires on the server clock.
                    pass

    def _report_availability(self, ready):
        with self._availability_lock:
            # A reporter already waiting for this lock cannot re-register a
            # slot after drain/stop has withdrawn it and acknowledged drain.
            ready = ready and not self._drained.is_set() and not self.stop.is_set()
            response = self.stub.ReportAvailability(
                pb.WorkerAvailabilityRequest(
                    worker_id=self.config.worker_id,
                    instance_id=self.instance_id,
                    ready=ready,
                ),
                timeout=min(3.0, self.config.rpc_timeout),
                metadata=self._metadata,
            )
            if not response.accepted or response.ttl_seconds < 30:
                raise ValueError("Coordinator rejected worker availability")
            if ready and self.config.broker_file:
                consumer = self._call(
                    self.stub.RegisterConsumer,
                    pb.WorkerClaimRequest(
                        worker_id=self.config.worker_id,
                        instance_id=self.instance_id,
                    ),
                )
                if consumer.admitted:
                    self._broker_admitted.set()
                else:
                    self._broker_admitted.clear()
                self._broker_priority = consumer.priority
            self._health(
                ready
                and (
                    not self.config.broker_file
                    or (
                        self._broker_connected.is_set()
                        and self._broker_admitted.is_set()
                    )
                )
            )
            return min(20.0, response.ttl_seconds / 3.0)

    def _report_availability_loop(self, interval):
        while not self.stop.wait(interval):
            try:
                if self._drained.is_set():
                    continue
                interval = self._report_availability(True)
                if self._drained.is_set():
                    # A concurrent drain may have withdrawn during this RPC.
                    self._report_availability(False)
                    continue
                self._available.set()
            except Exception:
                self._available.clear()
                self._health(False)
                logger.warning(
                    "Worker availability report unavailable; retry scheduled"
                )

    def process(self, claim):
        if self.stop.is_set():
            return False
        lease_lost = threading.Event()
        finished = threading.Event()
        progress = [0.0]
        deadline = time.monotonic() + self.config.max_task_seconds
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
                if (
                    time.monotonic() >= deadline
                    or self.stop.is_set()
                    or (self.config.broker_file and self._broker_lost.is_set())
                ):
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
                return self._process(request, claim, lease_lost, progress)
        except InferenceStopped:
            logger.info("Worker task stopped or lease lost")
            return False
        except grpc.RpcError:
            logger.warning("Worker task connection interrupted; lease will expire")
            return False
        except ValueError:
            return self._fail(request, lease_lost, "invalid_input", False)
        except Exception:
            return self._fail(request, lease_lost, "inference_failed", True)
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
                "Worker result accepted: worker=%s job=%s"
                if ack.accepted
                else "Worker result rejected after lease loss: worker=%s job=%s",
                self.config.worker_id,
                claim.job_id,
            )
            return ack.accepted

    def _process_delivery(self, task):
        request = pb.WorkerBeginRequest(
            worker_id=self.config.worker_id,
            instance_id=self.instance_id,
            job_id=task["job_id"],
            message_id=task["message_id"],
            delivery_id=str(uuid.uuid4()),
        )
        # Retrying the same delivery identity recovers a lost BeginAttempt reply.
        for attempt in range(2):
            try:
                response = self._call(self.stub.BeginAttempt, request)
                break
            except grpc.RpcError:
                if attempt or self.stop.is_set():
                    return "unsettled"
        if response.disposition == "discard":
            return "settled"
        if response.disposition == "busy":
            return "busy"
        if response.disposition != "started" or not response.job.job_id:
            return "unsettled"
        return "settled" if self.process(response.job) else "unsettled"

    def _fail(self, request, lease_lost, code, retryable):
        if lease_lost.is_set() or self.stop.is_set():
            return False
        logger.warning("Worker task failed (%s)", code)
        try:
            ack = self._call(
                self.stub.Fail,
                pb.WorkerFailRequest(
                    job_id=request.job_id,
                    lease_token=request.lease_token,
                    error_code=code,
                    retryable=retryable,
                ),
            )
            return ack.accepted
        except grpc.RpcError:
            logger.warning("Worker failure acknowledgement unavailable")
            return False


class RemoteWorkerPool:
    """Bounded execution loops sharing one engine, channel and process lifecycle."""

    def __init__(self, config: Config, stub, engine, token: str, stop=None, drain=None):
        self.config = config
        self.stop = stop if stop is not None else threading.Event()
        self.drain = drain if drain is not None else threading.Event()
        self._lock = threading.Lock()
        self._ready: set[str] = set()
        self._drained: set[str] = set()
        self.workers = [
            RemoteWorker(config, stub, engine, token, self.stop, lifecycle=self)
            for _ in range(config.concurrency)
        ]
        for worker in self.workers:
            worker.drain = self.drain

    def health(self, instance_id, ready):
        with self._lock:
            if ready:
                self._ready.add(instance_id)
            else:
                self._ready.discard(instance_id)
            report_health(
                self.config.health_file,
                len(self._ready) == len(self.workers)
                and not self.stop.is_set()
                and not self.drain.is_set(),
            )

    def mark_drained(self, instance_id, drained):
        with self._lock:
            if drained:
                self._drained.add(instance_id)
            else:
                self._drained.discard(instance_id)
            if len(self._drained) == len(self.workers) and self.drain.is_set():
                DRAIN_FILE.touch()
            else:
                DRAIN_FILE.unlink(missing_ok=True)

    def run_forever(self):
        threads = [
            threading.Thread(
                target=worker.run_forever, name=f"worker-slot-{index}", daemon=True
            )
            for index, worker in enumerate(self.workers)
        ]
        try:
            for thread in threads:
                thread.start()
            while not self.stop.wait(0.1):
                if not all(thread.is_alive() for thread in threads):
                    self.stop.set()
        finally:
            self.stop.set()
            deadline = time.monotonic() + self.config.shutdown_grace
            for thread in threads:
                if thread.ident is not None:
                    thread.join(timeout=max(0, deadline - time.monotonic()))
            report_health(self.config.health_file, False)
            DRAIN_FILE.unlink(missing_ok=True)


def main():
    setup_logging("remote-worker")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        config = Config.from_env()
        DRAIN_FILE.unlink(missing_ok=True)
        report_health(config.health_file, False)
        token = read_token(config.token_file)
        drain = threading.Event()
        signal.signal(signal.SIGUSR1, lambda *_: drain.set())
        signal.signal(signal.SIGUSR2, lambda *_: drain.clear())
        # Load dependencies and model before leasing any task.
        engine = FasterWhisperEngine(concurrency=config.concurrency)
        logger.info(
            "Remote worker ready: worker=%s slots=%d",
            config.worker_id,
            config.concurrency,
        )
        shutdown_telemetry = init_telemetry("whisper-remote-worker")
        with open_channel(config) as channel:
            worker = RemoteWorkerPool(
                config, rpc.WorkerServiceStub(channel), engine, token, stop, drain
            )
            thread = threading.Thread(
                target=worker.run_forever, daemon=True, name="remote-worker"
            )
            thread.start()
            while thread.is_alive() and not stop.wait(0.5):
                pass
            thread.join(timeout=config.shutdown_grace + 1)
        shutdown_telemetry()
        return 0
    except Exception:
        logger.error(
            "Remote worker startup failed; check configuration and local model availability"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
