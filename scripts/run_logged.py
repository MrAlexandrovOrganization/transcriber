"""Run one foreground child with bounded, rotating stdout/stderr logs."""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import signal
import subprocess
import threading
from pathlib import Path

LOG_BYTES = 5 * 1024 * 1024
LOG_BACKUPS = 3


def capture(pipe, path: Path):
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=LOG_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    try:
        while chunk := pipe.readline(64 * 1024):
            message = chunk.decode("utf-8", errors="replace").rstrip("\r\n")
            handler.emit(
                logging.LogRecord("child", logging.INFO, "", 0, message, (), None)
            )
    finally:
        pipe.close()
        handler.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--name", choices=("worker", "tunnel"), required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("child command is required")
    os.umask(0o077)
    args.log_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Stay in launchd's process group: if the supervisor itself crashes,
    # launchd must reap the child before restarting this agent.
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop(signum, _frame):
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            return

        def force_stop():
            try:
                child.kill()
            except ProcessLookupError:
                pass

        timer = threading.Timer(20, force_stop)
        timer.daemon = True
        timer.start()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop)
    threads = []
    for name, pipe in (("stdout", child.stdout), ("stderr", child.stderr)):
        thread = threading.Thread(
            target=capture,
            args=(pipe, args.log_dir / f"{args.name}.{name}.log"),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
    code = child.wait()
    for thread in threads:
        thread.join(timeout=5)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    raise SystemExit(main())
