"""Real temporary files, process locks and synthetic disk pressure."""

import errno
import multiprocessing
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from whisper.admission import (
    CapacityExceeded,
    UploadAdmission,
    UploadLimits,
    storage_bytes,
)
from whisper.remote_worker import Config


def competing_write(root, barrier, results):
    guard = UploadAdmission(Path(root), UploadLimits(4096, 4096, 1, 2))
    with (
        guard.upload(),
        (Path(root) / str(os.getpid())).open("wb", buffering=0) as audio,
    ):
        barrier.wait(timeout=10)
        try:
            guard.write(audio, b"x" * 4096)
            results.put("written")
        except CapacityExceeded:
            results.put("rejected")


def hold_upload(root, ready):
    guard = UploadAdmission(Path(root), UploadLimits(4096, 8192, 1, 1))
    with guard.upload():
        ready.set()
        # Parent terminates this process to exercise OS lock recovery.
        import threading

        threading.Event().wait()


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.guard = UploadAdmission(self.root, UploadLimits(4096, 8192, 1, 2))

    def test_invalid_configuration_and_shared_worker_limit(self):
        for name in (
            "MAX_AUDIO_BYTES",
            "MAX_JOB_STORAGE_BYTES",
            "MIN_JOB_STORAGE_FREE_BYTES",
            "MAX_CONCURRENT_UPLOADS",
        ):
            for value in ("0", "-1", "garbage"):
                with (
                    self.subTest(name=name, value=value),
                    patch.dict(os.environ, {name: value}),
                ):
                    with self.assertRaisesRegex(ValueError, name):
                        UploadLimits.from_env()
        with patch.dict(
            os.environ,
            {"MAX_AUDIO_BYTES": "100", "REMOTE_WORKER_TOKEN_FILE": "/synthetic/token"},
        ):
            self.assertEqual(Config.from_env().max_input_bytes, 100)
            with patch.dict(os.environ, {"REMOTE_WORKER_MAX_INPUT_BYTES": "99"}):
                with self.assertRaisesRegex(ValueError, "must be >= MAX_AUDIO_BYTES"):
                    Config.from_env()

    def test_retained_and_abandoned_files_count_toward_budget(self):
        for directory, name in [
            ("retained", "input.wav"),
            ("abandoned", "upload-old.part"),
        ]:
            target = self.root / directory
            target.mkdir()
            (target / name).write_bytes(b"x" * 4096)
        with (
            self.guard.upload(),
            (self.root / "new.part").open("wb", buffering=0) as audio,
        ):
            with self.assertRaises(CapacityExceeded):
                self.guard.write(audio, b"x")
        self.assertEqual((self.root / "retained/input.wav").read_bytes(), b"x" * 4096)

    def test_free_space_is_checked_again_during_stream(self):
        usage = shutil.disk_usage(self.root)
        with (
            self.guard.upload(),
            (self.root / "input.part").open("wb", buffering=0) as audio,
        ):
            self.guard.write(audio, b"x")
            with patch(
                "whisper.admission.shutil.disk_usage",
                return_value=usage._replace(free=1),
            ):
                with self.assertRaisesRegex(CapacityExceeded, "free-space"):
                    self.guard.write(audio, b"y")
            self.assertEqual((self.root / "input.part").read_bytes(), b"x")

    def test_full_disk_rejected_before_admission(self):
        usage = shutil.disk_usage(self.root)
        with patch(
            "whisper.admission.shutil.disk_usage", return_value=usage._replace(free=0)
        ):
            with self.assertRaises(CapacityExceeded), self.guard.upload():
                self.fail("admitted full disk")
        self.assertEqual(list(self.guard.control.glob("*.slot")), [])

    def test_slots_released_on_exception(self):
        self.guard.limits = UploadLimits(4096, 8192, 1, 1)
        with self.assertRaises(OSError), self.guard.upload():
            with self.assertRaises(CapacityExceeded), self.guard.upload():
                self.fail("admitted concurrent upload")
            raise OSError(errno.ENOSPC, "synthetic")
        with self.guard.upload():
            pass

    def test_dead_process_does_not_leak_upload_slot(self):
        ctx = multiprocessing.get_context("spawn")
        ready = ctx.Event()
        process = ctx.Process(target=hold_upload, args=(str(self.root), ready))
        process.start()
        try:
            self.assertTrue(ready.wait(15))
            guard = UploadAdmission(self.root, UploadLimits(4096, 8192, 1, 1))
            with self.assertRaises(CapacityExceeded), guard.upload():
                self.fail("admitted while other process owns slot")
        finally:
            process.terminate()
            process.join(10)
        with guard.upload():
            pass
        self.assertEqual(list(guard.control.glob("*.slot")), [])

    def test_parallel_process_writes_cannot_overbook_volume(self):
        ctx = multiprocessing.get_context("spawn")
        results = ctx.Queue()
        barrier = ctx.Barrier(2)
        processes = [
            ctx.Process(target=competing_write, args=(str(self.root), barrier, results))
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        try:
            self.assertEqual(
                sorted(results.get(timeout=15) for _ in processes),
                ["rejected", "written"],
            )
            self.assertEqual(storage_bytes(self.root), 4096)
        finally:
            for process in processes:
                process.join(10)
                if process.is_alive():
                    process.terminate()
                    process.join(5)
            results.close()
        self.assertTrue(all(process.exitcode == 0 for process in processes))

    def test_cleanup_releases_budget_without_deleting_retained_results(self):
        guard = UploadAdmission(self.root, UploadLimits(4096, 4096, 1, 1))
        old = self.root / "expired.wav"
        old.write_bytes(b"x" * 4096)
        with guard.upload(), (self.root / "new.part").open("wb", buffering=0) as audio:
            with self.assertRaises(CapacityExceeded):
                guard.write(audio, b"x")
            old.unlink()  # Simulate existing TTL/Ack cleanup, not admission eviction.
            guard.write(audio, b"x")
