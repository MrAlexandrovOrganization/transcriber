"""Install opt-in user systemd services for native WSL GPU and CPU workers."""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

PREFIX = "transcriber-wsl"


def quote(value: str | Path) -> str:
    text = str(value)
    if any(char in text for char in "\n\r\0"):
        raise ValueError("Unit values cannot contain line breaks or NUL")
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"'


def worker_environment(
    role: str, home: Path, token: Path, cudnn: Path
) -> dict[str, str]:
    environment = {
        "HOME": str(home),
        "PATH": "/usr/local/cuda/bin:/usr/lib/wsl/lib:/usr/bin:/bin",
        "PYTHONUNBUFFERED": "1",
        "LOG_LEVEL": "INFO",
    }
    if role == "tunnel":
        return environment
    threads = "2" if role == "gpu" else "4"
    environment.update(
        {
            "WORKER_ID": "wsl-" + role,
            "WHISPER_MODEL": "small" if role == "gpu" else "large",
            "WHISPER_DEVICE": "cuda" if role == "gpu" else "cpu",
            "WHISPER_COMPUTE_TYPE": "int8_float16" if role == "gpu" else "int8",
            "WHISPER_CPU_THREADS": threads,
            "WHISPER_LOCAL_FILES_ONLY": "1",
            "HF_HUB_OFFLINE": "1",
            "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1",
            "WORKER_POLL_INTERVAL_S": "60",
            "REMOTE_WORKER_ENDPOINT": "127.0.0.1:50055",
            "REMOTE_WORKER_TOKEN_FILE": str(token),
            "REMOTE_WORKER_SHUTDOWN_GRACE_S": "10",
            "OMP_NUM_THREADS": threads,
            "OPENBLAS_NUM_THREADS": threads,
        }
    )
    if role == "gpu":
        environment["LD_LIBRARY_PATH"] = ":".join(
            [
                str(cudnn),
                "/usr/local/cuda/targets/x86_64-linux/lib",
                "/usr/lib/wsl/lib",
            ]
        )
    return environment


def units(
    project: Path, home: Path, token: Path, cudnn: Path, connection: dict
) -> dict[str, str]:
    python = project / ".venv/bin/python"
    wrapper = project / "scripts/run_logged.py"
    logs = home / ".local/state/transcriber"
    names = [PREFIX + "-" + role + ".service" for role in ("tunnel", "gpu", "cpu")]
    definitions = {
        PREFIX + ".target": "\n".join(
            [
                "# Managed by transcriber/scripts/wsl_worker.py",
                "[Unit]",
                "Description=Native WSL transcription workers",
                "Wants=" + " ".join(names),
                "[Install]",
                "WantedBy=default.target",
                "",
            ]
        )
    }
    for role, name in zip(("tunnel", "gpu", "cpu"), names, strict=True):
        log_dir = logs / role
        environment = worker_environment(role, home, token, cudnn)
        command = [str(python), str(wrapper), "--log-dir", str(log_dir)]
        if role == "tunnel":
            command += [
                "--name",
                "tunnel",
                "--",
                "/usr/bin/ssh",
                "-N",
                "-T",
                "-o",
                "HostName=" + connection["host"],
                "-o",
                "User=" + connection["user"],
                "-p",
                str(connection["port"]),
                "-i",
                str(home / ".ssh/id_ed25519_transcriber"),
                "-o",
                "UserKnownHostsFile=" + str(home / ".ssh/known_hosts_transcriber"),
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=yes",
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
                "-o",
                "ControlMaster=no",
                "-o",
                "ControlPath=none",
                "-L",
                "127.0.0.1:50055:127.0.0.1:50055",
                "server",
            ]
        else:
            command += [
                "--name",
                "worker",
                "--",
                str(python),
                "-m",
                "whisper.remote_worker",
            ]
        lines = [
            "# Managed by transcriber/scripts/wsl_worker.py",
            "[Unit]",
            "Description=WSL transcriber " + role,
            "PartOf=" + PREFIX + ".target",
            "StartLimitIntervalSec=0",
            "[Service]",
            "Type=simple",
            "WorkingDirectory=" + quote(project),
            "ExecStart=" + " ".join(quote(arg) for arg in command),
            *[
                "Environment=" + quote(key + "=" + value)
                for key, value in environment.items()
            ],
            "Restart=always",
            "RestartSec=15",
            "TimeoutStopSec=30",
            "KillMode=control-group",
            "UMask=0077",
            "Nice=10",
            "CPUWeight=20",
            "IOWeight=20",
            "TasksMax=96",
            "NoNewPrivileges=yes",
            "PrivateTmp=yes",
            "ProtectSystem=strict",
            "ProtectHome=read-only",
            "ReadWritePaths=" + quote(log_dir),
            "StandardOutput=null",
            "StandardError=journal",
            "LogRateLimitIntervalSec=30s",
            "LogRateLimitBurst=10",
        ]
        if role != "tunnel":
            lines += [
                "CPUQuota=" + ("150%" if role == "gpu" else "400%"),
                "MemoryHigh=" + ("2G" if role == "gpu" else "4G"),
                "MemoryMax=" + ("3G" if role == "gpu" else "5500M"),
                "MemorySwapMax=0",
            ]
        definitions[name] = "\n".join(lines) + "\n"
    return definitions


def private_file(path: Path, project: Path):
    if path.resolve().is_relative_to(project.resolve()):
        raise ValueError("Private credentials must live outside the repository")
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        raise ValueError(
            "Credential must be a regular owned file with mode 0600 or stricter"
        )


def read_connection(home: Path, project: Path) -> dict:
    path = home / ".config/transcriber/wsl-ssh.json"
    private_file(path, project)
    connection = json.loads(path.read_text())
    if not isinstance(connection, dict) or set(connection) != {"host", "user", "port"}:
        raise ValueError("WSL SSH config must contain host, user and port")
    for name in ("host", "user"):
        value = connection[name]
        if (
            not isinstance(value, str)
            or not value
            or any(char.isspace() or char in '\0"\\=%' for char in value)
        ):
            raise ValueError("Invalid WSL SSH host/user")
    if type(connection["port"]) is not int or not 1 <= connection["port"] <= 65535:
        raise ValueError("Invalid WSL SSH port")
    return connection


def check_existing(directory: Path, definitions: dict[str, str], *, required=False):
    for name, content in definitions.items():
        path = directory / name
        drop_ins = directory / (name + ".d")
        if drop_ins.exists() and any(drop_ins.iterdir()):
            raise ValueError("Existing systemd drop-ins require explicit review")
        if not path.exists() and not path.is_symlink():
            if required:
                raise ValueError("Unit is missing; install before start")
            continue
        if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.getuid():
            raise ValueError("Refusing an unknown or unowned systemd unit")
        if path.read_text() != content:
            raise ValueError("Existing unit differs; review and archive it explicitly")


def install(directory: Path, definitions: dict[str, str], home: Path):
    check_existing(directory, definitions)
    directory.mkdir(parents=True, exist_ok=True)
    for role in ("tunnel", "gpu", "cpu"):
        (home / ".local/state/transcriber" / role).mkdir(
            mode=0o700, parents=True, exist_ok=True
        )
    for name, content in definitions.items():
        path = directory / name
        if not path.exists():
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as file:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())


def systemctl(*args, check=True):
    result = subprocess.run(["systemctl", "--user", *args], capture_output=True)
    if check and result.returncode:
        raise ValueError(
            f"systemctl {args[0]} failed (exit {result.returncode}); inspect user journal"
        )
    return result


def preflight(project: Path, home: Path, token: Path, cudnn: Path, audio: Path | None):
    private_file(token, project)
    private_file(home / ".ssh/id_ed25519_transcriber", project)
    known = home / ".ssh/known_hosts_transcriber"
    if known.is_symlink() or not known.is_file() or known.stat().st_uid != os.getuid():
        raise ValueError("Dedicated trusted known_hosts is missing or unowned")
    if not (project / "scripts/run_logged.py").is_file():
        raise ValueError("Logging wrapper is missing")
    for role in ("gpu", "cpu"):
        # This imports generated contracts and loads/inferences locally; it never
        # creates a channel, reports availability, or claims production work.
        env = dict(os.environ, **worker_environment(role, home, token, cudnn))
        code = (
            "from whisper.remote_worker import Config, read_token; "
            "from whisper.inference import FasterWhisperEngine; "
            "from proto.whisper_pb2_grpc import WorkerServiceStub; "
            "read_token(Config.from_env().token_file); e=FasterWhisperEngine(); "
        )
        if audio:
            code += "import sys; r=e.transcribe(sys.argv[1],language='en',initial_prompt='Synthetic runtime check.'); assert r.text.strip()"
        result = subprocess.run(
            [
                str(project / ".venv/bin/python"),
                "-c",
                code,
                *([str(audio)] if audio else []),
            ],
            cwd=project,
            env=env,
            capture_output=True,
            timeout=300,
        )
        if result.returncode:
            raise ValueError(
                role
                + " dependency, credential or cached model/inference validation failed"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("check", "install", "start", "stop", "status"),
        nargs="?",
        default="check",
    )
    parser.add_argument(
        "--audio",
        type=Path,
        help="Optional synthetic audio for real CPU/CUDA preflight inference",
    )
    args = parser.parse_args()
    if sys.platform != "linux" or not Path("/dev/dxg").exists():
        parser.error("This installer requires WSL with /dev/dxg")
    project, home = Path(__file__).resolve().parents[1], Path.home()
    token = home / ".config/transcriber/worker.token"
    versions = (project / "versions.mk").read_text().splitlines()
    python_version = next(
        line.split(":=", 1)[1].strip()
        for line in versions
        if line.startswith("PYTHON_VERSION :=")
    )
    cudnn = project / f".venv/lib/python{python_version}/site-packages/nvidia/cudnn/lib"
    directory = home / ".config/systemd/user"
    try:
        definitions = units(project, home, token, cudnn, read_connection(home, project))
        if args.action in {"check", "install", "start"}:
            preflight(project, home, token, cudnn, args.audio)
            check_existing(directory, definitions, required=args.action == "start")
        if args.action == "check":
            print("Local GPU small/CPU large validated; no services started")
        elif args.action == "install":
            install(directory, definitions, home)
            print(
                "Unit files installed, disabled and stopped; start explicitly after backend deploy"
            )
        elif args.action == "start":
            systemctl("daemon-reload")
            systemctl("enable", "--now", PREFIX + ".target")
            print("WSL tunnel, one GPU worker and one CPU worker enabled for this user")
        elif args.action == "stop":
            check_existing(directory, definitions, required=True)
            systemctl("disable", "--now", PREFIX + ".target")
            print("WSL workers and tunnel stopped and disabled")
        else:
            for name in definitions:
                active = (
                    systemctl("is-active", name, check=False).stdout.decode().strip()
                )
                print(name + ": " + (active or "unknown"))
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        detail = str(error) if isinstance(error, ValueError) else type(error).__name__
        print("WSL worker setup failed: " + detail, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
