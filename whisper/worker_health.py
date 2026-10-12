"""Container readiness is based on successful coordinator availability reports."""

import os
import time
from pathlib import Path


def report(path: str, ready: bool):
    if not path:
        return
    target = Path(path)
    if ready:
        temporary = target.with_suffix(".tmp")
        temporary.write_text(str(time.time()))
        temporary.replace(target)
    else:
        target.unlink(missing_ok=True)


def healthy(path: str, max_age: float = 45) -> bool:
    try:
        age = time.time() - float(Path(path).read_text())
        return 0 <= age <= max_age
    except (OSError, ValueError):
        return False


if __name__ == "__main__":
    raise SystemExit(0 if healthy(os.getenv("REMOTE_WORKER_HEALTH_FILE", "")) else 1)
