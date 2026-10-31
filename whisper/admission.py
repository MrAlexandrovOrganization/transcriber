"""Cooperating ingress writers share a bounded local job volume.

A filesystem lock serializes checks and unbuffered chunk writes, including across
processes. No lock is held while waiting for a network chunk. Upload slot locks
are released by the OS on process death; the next admission removes stale tokens.
All ingress processes sharing a volume must use the same limits. Network
filesystems and uncoordinated external writers cannot provide this guarantee.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import os
import shutil
import uuid
from pathlib import Path

from whisper.limits import audio_limit


class CapacityExceeded(Exception):
    """Temporary volume or upload concurrency pressure; retry the same request."""


def positive_limit(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be a positive integer") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclasses.dataclass(frozen=True)
class UploadLimits:
    audio_bytes: int
    storage_bytes: int
    min_free_bytes: int
    concurrent_uploads: int

    @classmethod
    def from_env(cls):
        return cls(
            audio_bytes=audio_limit(),
            storage_bytes=positive_limit("MAX_JOB_STORAGE_BYTES", 8 * 1024**3),
            min_free_bytes=positive_limit("MIN_JOB_STORAGE_FREE_BYTES", 1024**3),
            concurrent_uploads=positive_limit("MAX_CONCURRENT_UPLOADS", 4),
        )


def _scan_error(error):
    raise error


def storage_bytes(root: Path) -> int:
    """Count every job file, including partials and retained terminal inputs.

    Use the greater of logical and allocated size, so sparse files and filesystem
    block rounding cannot evade the quota. Cleanup may remove files concurrently.
    """
    total = 0
    for directory, dirs, files in os.walk(root, onerror=_scan_error):
        if Path(directory) == root:
            dirs[:] = [name for name in dirs if name != ".admission"]
        for name in files:
            try:
                stat = (Path(directory) / name).stat()
            except FileNotFoundError:
                continue
            total += max(stat.st_size, stat.st_blocks * 512)
    return total


class UploadAdmission:
    def __init__(self, root: Path, limits: UploadLimits):
        self.root = root
        self.limits = limits
        self.control = root / ".admission"

    @contextlib.contextmanager
    def locked(self):
        self.control.mkdir(exist_ok=True)
        with (self.control / "lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def _check_capacity(self, extra: int = 0):
        # Round conservatively for a new allocation (even when appending into an
        # existing block). An external writer may still cause ENOSPC after this.
        block = os.statvfs(self.root).f_frsize
        allocation = ((extra + block - 1) // block) * block
        if storage_bytes(self.root) + allocation > self.limits.storage_bytes:
            raise CapacityExceeded("Job storage byte budget exhausted")
        if shutil.disk_usage(self.root).free - allocation < self.limits.min_free_bytes:
            raise CapacityExceeded("Job storage free-space reserve reached")

    def _active_uploads(self):
        active = 0
        for path in self.control.glob("*.slot"):
            with path.open("rb") as token:
                try:
                    fcntl.flock(token, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    active += 1
                else:
                    path.unlink()
        return active

    @contextlib.contextmanager
    def upload(self):
        token_path = self.control / (uuid.uuid4().hex + ".slot")
        with self.locked():
            if self._active_uploads() >= self.limits.concurrent_uploads:
                raise CapacityExceeded("Too many concurrent audio uploads")
            self._check_capacity()
            token = token_path.open("xb")
            fcntl.flock(token, fcntl.LOCK_EX)
        try:
            yield self
        finally:
            with self.locked():
                token_path.unlink(missing_ok=True)
                token.close()

    def write(self, audio, data: bytes):
        if not data:
            return
        with self.locked():
            self._check_capacity(len(data))
            # Caller opens the file unbuffered: the next process must see this
            # write in stat/disk_usage before it is allowed to write its chunk.
            if audio.write(data) != len(data):
                raise OSError("Incomplete audio write")
