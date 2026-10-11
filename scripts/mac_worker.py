"""Check/install/start user LaunchAgents for a native Mac transcription worker."""

from __future__ import annotations

import argparse
import os
import plistlib
import re
import stat
import subprocess
import sys
from pathlib import Path

LABEL_PREFIX = "net.mralexandrov.transcriber"


def agents(project: Path, home: Path, token: Path) -> dict[str, dict]:
    python = str(project / ".venv/bin/python")
    wrapper = str(project / "scripts/run_logged.py")
    logs = str(home / "Library/Logs/transcriber")
    common = {
        "WorkingDirectory": str(project),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 30,
        "ProcessType": "Background",
        "Nice": 10,
        "LowPriorityIO": True,
        "LowPriorityBackgroundIO": True,
        "ExitTimeOut": 25,
        # Child output is captured and rotated by run_logged, never appended
        # to unbounded launchd files. No sleep/power assertion is acquired.
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
        "EnvironmentVariables": {
            "HOME": str(home),
            "PATH": "/opt/homebrew/bin:/usr/bin:/bin",
            "PYTHONUNBUFFERED": "1",
        },
    }
    tunnel = dict(common)
    tunnel["Label"] = LABEL_PREFIX + ".tunnel"
    tunnel["ProgramArguments"] = [
        python,
        wrapper,
        "--log-dir",
        logs,
        "--name",
        "tunnel",
        "--",
        "/usr/bin/ssh",
        "-N",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "LogLevel=ERROR",
        "-L",
        "127.0.0.1:50055:127.0.0.1:50055",
        "server",
    ]
    worker = dict(common)
    worker["Label"] = LABEL_PREFIX + ".worker"
    worker["ProgramArguments"] = [
        python,
        wrapper,
        "--log-dir",
        logs,
        "--name",
        "worker",
        "--",
        python,
        "-m",
        "whisper.remote_worker",
    ]
    worker["EnvironmentVariables"] = dict(
        common["EnvironmentVariables"],
        **{
            "WORKER_ID": "mac-m3-pro",
            "WHISPER_MODEL": "large",
            "WHISPER_DEVICE": "cpu",
            "WHISPER_COMPUTE_TYPE": "int8",
            "WHISPER_CPU_THREADS": "2",
            "WHISPER_LOCAL_FILES_ONLY": "1",
            "WORKER_POLL_INTERVAL_S": "60",
            "REMOTE_WORKER_ENDPOINT": "127.0.0.1:50055",
            "REMOTE_WORKER_TOKEN_FILE": str(token),
            "REMOTE_WORKER_SHUTDOWN_GRACE_S": "10",
            "OMP_NUM_THREADS": "2",
            "OPENBLAS_NUM_THREADS": "2",
            "VECLIB_MAXIMUM_THREADS": "2",
            "LOG_LEVEL": "INFO",
        },
    )
    return {"tunnel": tunnel, "worker": worker}


def validate_token_path(token: Path, project: Path):
    if token.resolve().is_relative_to(project.resolve()):
        raise ValueError("Worker token must live outside the repository")
    info = token.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        raise ValueError(
            "Worker token must be a regular owned file with mode 0600 or stricter"
        )


def preflight(project: Path, definitions: dict[str, dict], token: Path):
    python = project / ".venv/bin/python"
    if not python.is_file() or not os.access(python, os.X_OK):
        raise ValueError(
            "Project .venv/bin/python is unavailable; install locked dependencies first"
        )
    if not (project / "scripts/run_logged.py").is_file():
        raise ValueError("Logging wrapper is unavailable")
    validate_token_path(token, project)
    # Only local imports/model loading: this never calls Claim or opens gRPC.
    # Full model construction validates cached tensor data before bootstrap.
    env = dict(os.environ, **definitions["worker"]["EnvironmentVariables"])
    code = (
        "from whisper.remote_worker import Config, read_token; "
        "from whisper.inference import FasterWhisperEngine; "
        "from proto.whisper_pb2_grpc import WorkerServiceStub; "
        "read_token(Config.from_env().token_file); FasterWhisperEngine()"
    )
    result = subprocess.run(
        [str(python), "-c", code],
        cwd=project,
        env=env,
        capture_output=True,
        timeout=180,
    )
    if result.returncode:
        raise ValueError(
            "Local token, dependencies or cached large model validation failed"
        )


def check_existing(directory: Path, definitions: dict[str, dict], *, required=False):
    for definition in definitions.values():
        path = directory / (definition["Label"] + ".plist")
        if not path.exists() and not path.is_symlink():
            if required:
                raise ValueError("LaunchAgent is missing; run install before start")
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError("Refusing to replace an unknown LaunchAgent")
        if path.stat().st_uid != os.getuid():
            raise ValueError("Refusing to use a LaunchAgent owned by another user")
        with path.open("rb") as file:
            existing = plistlib.load(file)
        if existing != definition:
            raise ValueError(
                "Existing LaunchAgent differs; review and remove or archive it explicitly"
            )


def install(directory: Path, definitions: dict[str, dict]):
    check_existing(directory, definitions)
    directory.mkdir(parents=True, exist_ok=True)
    for definition in definitions.values():
        path = directory / (definition["Label"] + ".plist")
        if path.exists():
            continue
        # Exclusive creation protects a concurrent/unknown install as well.
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as file:
            plistlib.dump(definition, file, sort_keys=True)
            file.flush()
            os.fsync(file.fileno())


class LaunchctlError(subprocess.SubprocessError):
    """Report a failed operation without exposing commands or environment."""


def safe_launchctl_error(stderr: bytes) -> str:
    # launchctl receives only operation/domain/plist paths, never the token.
    # Omit stdout: `print` can contain the whole job environment. Bound stderr
    # and remove terminal controls plus common credential-shaped diagnostics.
    message = stderr[:2048].decode("utf-8", errors="replace")
    message = re.sub(r"(?i)\bbearer\s+\S+", "Bearer [redacted]", message)
    message = re.sub(
        r"(?i)\b(token|secret|password|authorization)\s*[:=]\s*\S+",
        r"\1=[redacted]",
        message,
    )
    message = "".join(char for char in message if char in "\n\t" or char.isprintable())
    return " ".join(message.split()) or "no diagnostic returned"


def launchctl(*args, check=True):
    result = subprocess.run(["/bin/launchctl", *args], capture_output=True)
    if check and result.returncode:
        raise LaunchctlError(
            f"launchctl {args[0]} failed (exit {result.returncode}): "
            f"{safe_launchctl_error(result.stderr)}"
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("check", "install", "start", "stop", "status"),
        default="check",
        nargs="?",
    )
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("This installer requires macOS")
    project = Path(__file__).resolve().parents[1]
    home = Path.home()
    token = home / ".config/transcriber/worker.token"
    directory = home / "Library/LaunchAgents"
    definitions = agents(project, home, token)
    domain = f"gui/{os.getuid()}"
    operation = "initialize"
    try:
        if args.action in {"check", "install", "start"}:
            operation = "validate local runtime, token and cached model"
            preflight(project, definitions, token)
            operation = "validate installed LaunchAgents"
            check_existing(directory, definitions, required=args.action == "start")
        if args.action == "check":
            print(
                "Ready: native large CPU/int8, two threads, one worker; no agents started"
            )
        elif args.action == "install":
            operation = "install LaunchAgent files"
            install(directory, definitions)
            print(
                "LaunchAgent files installed; use start explicitly when the coordinator is ready"
            )
        elif args.action == "start":
            for definition in definitions.values():
                label = definition["Label"]
                operation = f"bootstrap {label} in {domain}"
                if launchctl("print", f"{domain}/{label}", check=False).returncode:
                    launchctl("bootstrap", domain, str(directory / (label + ".plist")))
            print("Tunnel and worker registered in this user's login session")
        elif args.action == "stop":
            operation = "validate installed LaunchAgents"
            check_existing(directory, definitions, required=True)
            for definition in reversed(list(definitions.values())):
                label = definition["Label"]
                operation = f"bootout {label} in {domain}"
                if not launchctl("print", f"{domain}/{label}", check=False).returncode:
                    launchctl("bootout", f"{domain}/{label}")
            print("Worker and tunnel stopped; files retained for the next start/login")
        else:
            for definition in definitions.values():
                label = definition["Label"]
                operation = f"query {label} in {domain}"
                loaded = (
                    launchctl("print", f"{domain}/{label}", check=False).returncode == 0
                )
                print(label + (": loaded" if loaded else ": not loaded"))
        return 0
    except LaunchctlError as error:
        print(f"Mac worker setup failed during {operation}: {error}", file=sys.stderr)
        return 1
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        # Local validation errors are deliberately authored above. OS/subprocess
        # errors can include captured commands/data; report only their type.
        detail = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(
            f"Mac worker setup failed during {operation}: {detail}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
