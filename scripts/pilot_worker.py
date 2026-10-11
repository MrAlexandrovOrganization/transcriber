"""Exercise a native pull worker against disposable local PostgreSQL and ingress.

Run through the project's Make target after generating protobuf bindings and
caching the base model. This script never downloads images or model weights.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PilotError(Exception):
    """A safe error message which contains no child output or credentials."""


def command(args: list[str], *, timeout: float = 15) -> str:
    try:
        result = subprocess.run(
            args,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        raise PilotError(f"Local command failed: {args[0]} {args[1]}") from None
    return result.stdout.strip()


def require_local_docker() -> None:
    # A developer's active Docker context can point at a production server.
    endpoint = os.getenv("DOCKER_HOST", "")
    if os.getenv("DOCKER_CONTEXT") or not endpoint:
        endpoint = command(
            [
                "docker",
                "context",
                "inspect",
                "--format",
                '{{(index .Endpoints "docker").Host}}',
            ]
        )
    if not endpoint.startswith(("unix://", "npipe://")):
        raise PilotError("Pilot requires a Docker context connected to a local socket")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def child_environment() -> dict[str, str]:
    env = os.environ.copy()
    prefixes = (
        "OTEL_",
        "WHISPER_",
        "WORKER_",
        "REMOTE_WORKER_",
        "JOB_",
        "GRPC_",
        "TELEGRAM_",
        "BOT_",
    )
    for key in list(env):
        if key.startswith(prefixes) or key in {
            "DATABASE_URL",
            "SERVICE_ROLE",
            "MAX_AUDIO_BYTES",
            "CLEANUP_INTERVAL_S",
        }:
            del env[key]
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "LOG_LEVEL": "WARNING",
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "NO_PROXY": "localhost,127.0.0.1,::1",
            "no_proxy": "localhost,127.0.0.1,::1",
            "no_grpc_proxy": "localhost,127.0.0.1,::1",
        }
    )
    return env


def wait_postgres(dsn: str) -> None:
    import psycopg

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            # The container's initialization socket is available before its
            # final TCP server. Readiness must use the actual client endpoint.
            with psycopg.connect(dsn, connect_timeout=1) as connection:
                connection.execute("SELECT 1").fetchone()
            return
        except psycopg.Error:
            time.sleep(0.25)
    raise PilotError(
        "Disposable PostgreSQL TCP endpoint did not become ready within 30 seconds"
    )


def child_error_category(log_path: Path) -> str:
    """Return only a traceback exception class, never child log content."""
    try:
        log = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "no diagnostic category"
    categories = re.findall(
        r"^([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception)):", log, re.MULTILINE
    )
    return categories[-1][:100] if categories else "no diagnostic category"


def stop_child(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_pilot(audio: Path, timeout: float, expect: str) -> dict:
    version = os.getenv("POSTGRES_VERSION", "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", version):
        raise PilotError(
            "POSTGRES_VERSION must be supplied by the project's Make target"
        )
    require_local_docker()
    image = f"postgres:{version}"
    command(["docker", "image", "inspect", image])

    # These imports use generated local bindings and the current Python runtime.
    sys.path.insert(0, str(ROOT))
    import grpc

    from proto import whisper_pb2 as pb
    from proto import whisper_pb2_grpc as rpc

    container = f"transcriber-worker-pilot-{uuid.uuid4().hex}"
    children: list[subprocess.Popen] = []
    container_attempted = False
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="transcriber-pilot-") as temp:
        with contextlib.ExitStack() as stack:
            try:
                container_attempted = True
                command(
                    [
                        "docker",
                        "run",
                        "--detach",
                        "--rm",
                        "--pull=never",
                        "--name",
                        container,
                        "--label",
                        "app=transcriber-local-worker-pilot",
                        "--tmpfs",
                        "/var/lib/postgresql/data:rw,nosuid,size=256m",
                        "--memory",
                        "512m",
                        "--publish",
                        "127.0.0.1::5432",
                        "--env",
                        "POSTGRES_USER=pilot",
                        "--env",
                        "POSTGRES_DB=pilot",
                        "--env",
                        "POSTGRES_PASSWORD=synthetic-local-pilot",
                        image,
                    ],
                    timeout=20,
                )
                binding = command(["docker", "port", container, "5432/tcp"])
                match = re.fullmatch(r"127\.0\.0\.1:([0-9]+)", binding)
                if not match:
                    raise PilotError(
                        "Disposable PostgreSQL is not bound exclusively to loopback"
                    )
                db_port = int(match[1])
                dsn = f"postgresql://pilot:synthetic-local-pilot@127.0.0.1:{db_port}/pilot"
                wait_postgres(dsn)
                token_path = Path(temp) / "worker.token"
                token_fd = os.open(
                    token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
                with os.fdopen(token_fd, "w") as file:
                    file.write(secrets.token_urlsafe(48))
                jobs = Path(temp) / "jobs"
                jobs.mkdir(mode=0o700)
                ingress_port = free_port()
                worker_port = free_port()
                while worker_port == ingress_port:
                    worker_port = free_port()
                base_env = child_environment()
                ingress_env = base_env | {
                    "DATABASE_URL": dsn,
                    "SERVICE_ROLE": "ingress",
                    "GRPC_PORT": str(ingress_port),
                    "GRPC_BIND_HOST": "127.0.0.1",
                    "WORKER_GRPC_ADDR": f"127.0.0.1:{worker_port}",
                    "WORKER_TOKEN_FILE": str(token_path),
                    "JOB_STORAGE_DIR": str(jobs),
                    "JOB_LEASE_SECONDS": "15",
                    "JOB_MAX_ATTEMPTS": "3",
                    "JOB_TTL_S": "3600",
                    "CLEANUP_INTERVAL_S": "60",
                    "MAX_AUDIO_BYTES": str(32 * 1024 * 1024),
                }
                ingress_log = stack.enter_context(
                    (Path(temp) / "ingress.log").open("wb")
                )
                ingress = subprocess.Popen(
                    [sys.executable, "-m", "whisper.main"],
                    cwd=ROOT,
                    env=ingress_env,
                    stdout=ingress_log,
                    stderr=subprocess.STDOUT,
                )
                children.append(ingress)
                channel = stack.enter_context(
                    grpc.insecure_channel(f"127.0.0.1:{ingress_port}")
                )
                ready = grpc.channel_ready_future(channel)
                ready_deadline = time.monotonic() + 20
                while True:
                    if ingress.poll() is not None:
                        category = child_error_category(Path(temp) / "ingress.log")
                        raise PilotError(
                            f"Pilot ingress exited during startup ({category})"
                        )
                    try:
                        ready.result(timeout=0.25)
                        break
                    except grpc.FutureTimeoutError:
                        if time.monotonic() >= ready_deadline:
                            raise PilotError(
                                "Pilot ingress did not become ready within 20 seconds"
                            ) from None
                stub = rpc.TranscriptionServiceStub(channel)

                def chunks():
                    with audio.open("rb") as source:
                        first = True
                        while data := source.read(64 * 1024):
                            yield pb.TranscribeChunk(
                                data=data,
                                format=audio.suffix.lstrip(".") if first else "",
                                options=pb.TranscriptionOptions(
                                    preset="voice", language="en"
                                ),
                            )
                            first = False

                submission = stub.Submit(chunks(), timeout=15)
                if not submission.job_id:
                    raise PilotError("Pilot upload returned no job identifier")
                request = pb.StatusRequest(job_id=submission.job_id)
                status = stub.GetStatus(request, timeout=5)
                if status.status != pb.QUEUED or status.attempts != 0:
                    raise PilotError("Job was not queued before any worker started")
                print(
                    json.dumps({"phase": "queued_without_workers", "attempts": 0}),
                    flush=True,
                )
                worker_env = base_env | {
                    "REMOTE_WORKER_ENDPOINT": f"127.0.0.1:{worker_port}",
                    "REMOTE_WORKER_TOKEN_FILE": str(token_path),
                    "WORKER_ID": "local-native-pilot",
                    "WORKER_POLL_INTERVAL_S": "1",
                    "REMOTE_WORKER_RPC_TIMEOUT_S": "5",
                    "REMOTE_WORKER_DOWNLOAD_TIMEOUT_S": "15",
                    "REMOTE_WORKER_SHUTDOWN_GRACE_S": "5",
                    "WHISPER_MODEL": "base",
                    "WHISPER_DEVICE": "cpu",
                    "WHISPER_COMPUTE_TYPE": "int8",
                    "WHISPER_CPU_THREADS": "2",
                    "WHISPER_LOCAL_FILES_ONLY": "1",
                }
                worker_log = stack.enter_context((Path(temp) / "worker.log").open("wb"))
                worker = subprocess.Popen(
                    [sys.executable, "-m", "whisper.remote_worker"],
                    cwd=ROOT,
                    env=worker_env,
                    stdout=worker_log,
                    stderr=subprocess.STDOUT,
                )
                children.append(worker)
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    if ingress.poll() is not None or worker.poll() is not None:
                        role = "ingress" if ingress.poll() is not None else "worker"
                        category = child_error_category(Path(temp) / f"{role}.log")
                        raise PilotError(
                            f"Pilot {role} exited before completion ({category})"
                        )
                    status = stub.GetStatus(request, timeout=5)
                    if status.status == pb.FAILED:
                        raise PilotError("Synthetic transcription reached FAILED")
                    if status.status == pb.DONE:
                        if not status.text.strip():
                            raise PilotError(
                                "Synthetic transcription returned empty text"
                            )
                        if expect and expect.casefold() not in status.text.casefold():
                            raise PilotError(
                                "Synthetic transcription did not contain the expected phrase"
                            )
                        return {
                            "status": "DONE",
                            "queued_before_worker": True,
                            "attempts": status.attempts,
                            "transcript_characters": len(status.text),
                            "elapsed_seconds": round(time.monotonic() - started, 2),
                            "device": "cpu",
                            "model": "base",
                        }
                    time.sleep(0.25)
                raise PilotError("Synthetic transcription exceeded the pilot timeout")
            finally:
                cleanup_failed = False
                for child in reversed(children):
                    try:
                        stop_child(child)
                    except (OSError, subprocess.SubprocessError):
                        cleanup_failed = True
                if container_attempted:
                    try:
                        command(
                            ["docker", "stop", "--time", "3", container], timeout=15
                        )
                    except PilotError:
                        # --rm may already have removed a container that failed startup.
                        try:
                            remaining = command(
                                [
                                    "docker",
                                    "ps",
                                    "--all",
                                    "--quiet",
                                    "--filter",
                                    f"name=^{container}$",
                                ]
                            )
                            cleanup_failed = cleanup_failed or bool(remaining)
                        except PilotError:
                            cleanup_failed = True
                if cleanup_failed:
                    raise PilotError(
                        f"Pilot cleanup could not be confirmed for {container}"
                    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--audio",
        type=Path,
        required=True,
        help="Existing local synthetic audio fixture",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=90,
        help="Maximum worker completion wait, seconds",
    )
    parser.add_argument(
        "--expect",
        default="",
        help="Optional expected phrase in the synthetic transcript",
    )
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")
    if not args.audio.is_file() or not re.fullmatch(
        r"[A-Za-z0-9]{1,12}", args.audio.suffix.lstrip(".")
    ):
        parser.error("--audio must be an existing file with an alphanumeric extension")
    if not 0 < args.audio.stat().st_size <= 32 * 1024 * 1024:
        parser.error("--audio must contain 1 byte to 32 MiB")
    try:
        result = run_pilot(args.audio.resolve(), args.timeout, args.expect)
    except KeyboardInterrupt:
        print("Pilot interrupted; disposable resources cleaned up", file=sys.stderr)
        return 130
    except PilotError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception:
        print(
            "Pilot failed; check local dependencies, generated protobuf bindings, and cached base model",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
