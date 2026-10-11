"""Remote worker tests use synthetic audio and fake RPCs, never production."""

import pathlib
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import grpc

from proto import whisper_pb2 as pb
from whisper.inference import InferenceStopped, TranscriptionResult
from whisper.limits import MAX_RESULT_BYTES
from whisper.remote_worker import Config, RemoteWorker, read_token


class Offline(grpc.RpcError):
    pass


class FakeStub:
    def __init__(self):
        self.heartbeats = []
        self.completed = []
        self.failed = []
        self.accepted = True
        self.audio = b"synthetic"
        self.calls = []
        self.availability = []
        self.heartbeat_seen = threading.Event()

    def _record(self, kwargs):
        assert kwargs["timeout"] > 0
        assert kwargs["metadata"] == (
            ("authorization", "Bearer synthetic-worker-token-at-least-32-characters"),
        )

    def Download(self, request, **kwargs):
        self._record(kwargs)
        return iter([pb.WorkerAudioChunk(data=self.audio)])

    def ReportAvailability(self, request, **kwargs):
        self._record(kwargs)
        self.availability.append(request)
        return pb.WorkerAvailabilityResponse(accepted=True, ttl_seconds=90)

    def Heartbeat(self, request, **kwargs):
        self._record(kwargs)
        self.heartbeats.append(request)
        self.heartbeat_seen.set()
        return pb.WorkerAck(accepted=self.accepted)

    def Complete(self, request, **kwargs):
        self._record(kwargs)
        self.completed.append(request)
        return pb.WorkerAck(accepted=self.accepted)

    def Fail(self, request, **kwargs):
        self._record(kwargs)
        self.failed.append(request)
        return pb.WorkerAck(accepted=True)


class FakeEngine:
    def __init__(self, stub, *, wait=0, error=False):
        self.stub = stub
        self.wait = wait
        self.error = error
        self.path = None
        self.active = threading.Event()

    def transcribe(self, path, *, should_stop, on_progress, **kwargs):
        self.path = pathlib.Path(path)
        assert self.path.read_bytes() == b"synthetic"
        self.active.set()
        self.stub.heartbeat_seen.wait(1)
        until = time.monotonic() + self.wait
        while time.monotonic() < until:
            if should_stop.wait(0.01):
                raise InferenceStopped()
        if self.error:
            raise RuntimeError("sensitive exception must never be sent")
        on_progress(50)
        return TranscriptionResult("synthetic transcript", [])


def config(**kwargs):
    return Config("localhost:50055", "/unused/test-token", "test-worker", **kwargs)


def claim(**kwargs):
    values = dict(
        job_id="synthetic-job",
        lease_token="lease-one",
        lease_seconds=1,
        input_size=9,
        format="../untrusted",
        options=pb.TranscriptionOptions(preset="voice"),
    )
    values.update(kwargs)
    return pb.WorkerClaimResponse(**values)


class RemoteWorkerTests(unittest.TestCase):
    def test_availability_is_reported_while_busy_and_withdrawn_on_stop(self):
        stub = FakeStub()
        engine = FakeEngine(stub, wait=0.15)
        worker = RemoteWorker(
            config(), stub, engine, "synthetic-worker-token-at-least-32-characters"
        )
        claimed = []

        def do_claim(request, **kwargs):
            stub._record(kwargs)
            self.assertTrue(stub.availability[0].ready)
            self.assertEqual(request.instance_id, worker.instance_id)
            claimed.append(request)
            return claim()

        original_complete = stub.Complete

        def complete(request, **kwargs):
            response = original_complete(request, **kwargs)
            worker.stop.set()
            return response

        original_report = worker._report_availability

        def report(ready):
            original_report(ready)
            return 0.02

        stub.Claim = do_claim
        stub.Complete = complete
        with patch.object(worker, "_report_availability", report):
            worker.run_forever()
        self.assertEqual(len(claimed), 1)
        self.assertGreaterEqual(len(stub.availability), 4)
        self.assertTrue(all(r.ready for r in stub.availability[:-1]))
        self.assertFalse(stub.availability[-1].ready)
        self.assertTrue(
            all(r.instance_id == worker.instance_id for r in stub.availability)
        )

    def test_rejected_readiness_does_not_claim_work(self):
        stub = FakeStub()
        worker = RemoteWorker(
            config(), stub, None, "synthetic-worker-token-at-least-32-characters"
        )
        stub.ReportAvailability = lambda *a, **kw: pb.WorkerAvailabilityResponse(
            accepted=False, ttl_seconds=90
        )
        claims = []
        stub.Claim = lambda *a, **kw: claims.append(a)
        with patch.object(worker.stop, "wait", lambda _: worker.stop.set()):
            worker.run_forever()
        self.assertEqual(claims, [])

    def test_heartbeat_runs_while_inference_is_blocked_and_temp_is_removed(self):
        stub = FakeStub()
        engine = FakeEngine(stub, wait=0.6)
        RemoteWorker(
            config(), stub, engine, "synthetic-worker-token-at-least-32-characters"
        ).process(claim())
        self.assertGreaterEqual(len(stub.heartbeats), 2)
        self.assertEqual(len(stub.completed), 1)
        self.assertEqual(stub.completed[0].lease_token, "lease-one")
        self.assertEqual(stub.failed, [])
        self.assertFalse(engine.path.exists())
        self.assertFalse(engine.path.parent.exists())

    def test_lost_lease_prevents_complete_and_failure(self):
        stub = FakeStub()
        stub.accepted = False
        engine = FakeEngine(stub, wait=0.1)
        RemoteWorker(
            config(), stub, engine, "synthetic-worker-token-at-least-32-characters"
        ).process(claim())
        self.assertEqual(stub.completed, [])
        self.assertEqual(stub.failed, [])
        if engine.path:
            self.assertFalse(engine.path.exists())

    def test_lease_reassigned_during_inference_discards_result(self):
        stub = FakeStub()
        original_heartbeat = stub.Heartbeat

        def heartbeat(request, **kwargs):
            if stub.heartbeats:
                stub.accepted = False
            return original_heartbeat(request, **kwargs)

        stub.Heartbeat = heartbeat
        engine = FakeEngine(stub, wait=5)
        RemoteWorker(
            config(), stub, engine, "synthetic-worker-token-at-least-32-characters"
        ).process(claim())
        self.assertGreaterEqual(len(stub.heartbeats), 2)
        self.assertEqual(stub.completed, [])
        self.assertEqual(stub.failed, [])
        self.assertFalse(engine.path.exists())

    def test_failure_is_retryable_without_exception_text(self):
        stub = FakeStub()
        engine = FakeEngine(stub, error=True)
        with self.assertLogs("whisper.remote_worker", level="INFO") as logs:
            RemoteWorker(
                config(), stub, engine, "synthetic-worker-token-at-least-32-characters"
            ).process(claim())
        self.assertEqual(len(stub.failed), 1)
        self.assertTrue(stub.failed[0].retryable)
        self.assertEqual(stub.failed[0].error_code, "inference_failed")
        self.assertNotIn("sensitive", " ".join(logs.output))
        self.assertNotIn(
            "synthetic-worker-token-at-least-32-characters", " ".join(logs.output)
        )
        self.assertFalse(engine.path.exists())

    def test_input_size_limit_and_truncation(self):
        for size in (8, 10, 300 * 1024 * 1024):
            with self.subTest(size=size):
                stub = FakeStub()
                engine = FakeEngine(stub)
                RemoteWorker(
                    config(),
                    stub,
                    engine,
                    "synthetic-worker-token-at-least-32-characters",
                ).process(claim(input_size=size))
                self.assertEqual(stub.completed, [])
                self.assertEqual(stub.failed[0].error_code, "invalid_input")
                self.assertFalse(stub.failed[0].retryable)

    def test_oversized_result_fails_without_sending_complete(self):
        stub = FakeStub()
        engine = FakeEngine(stub)
        with patch.object(
            engine,
            "transcribe",
            return_value=TranscriptionResult("x" * MAX_RESULT_BYTES, []),
        ):
            RemoteWorker(
                config(), stub, engine, "synthetic-worker-token-at-least-32-characters"
            ).process(claim())
        self.assertEqual(stub.completed, [])
        self.assertEqual(stub.failed[0].error_code, "invalid_input")
        self.assertFalse(stub.failed[0].retryable)

    def test_shutdown_during_inference_stops_without_acknowledging(self):
        stub = FakeStub()
        engine = FakeEngine(stub, wait=5)
        stop = threading.Event()
        worker = RemoteWorker(
            config(),
            stub,
            engine,
            "synthetic-worker-token-at-least-32-characters",
            stop,
        )
        thread = threading.Thread(target=worker.process, args=(claim(),))
        thread.start()
        self.assertTrue(engine.active.wait(1))
        stop.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(stub.completed, [])
        self.assertEqual(stub.failed, [])
        self.assertFalse(engine.path.exists())

    def test_offline_coordinator_backoff(self):
        stop = threading.Event()
        stub = FakeStub()
        stub.ReportAvailability = lambda *args, **kwargs: (_ for _ in ()).throw(
            Offline()
        )
        delays = []

        def wait(delay):
            delays.append(delay)
            stop.set()

        with patch.object(stop, "wait", wait):
            RemoteWorker(
                config(),
                stub,
                None,
                "synthetic-worker-token-at-least-32-characters",
                stop,
            ).run_forever()
        self.assertEqual(len(delays), 1)
        self.assertGreaterEqual(delays[0], 1.6)
        self.assertLessEqual(delays[0], 2.4)

    def test_plaintext_only_loopback_without_explicit_override(self):
        for endpoint in ("localhost:50055", "127.0.0.1:50055", "[::1]:50055"):
            Config(endpoint, "/test", "test")
        with self.assertRaises(ValueError):
            Config("100.64.0.1:50055", "/test", "test")
        Config("100.64.0.1:50055", "/test", "test", allow_insecure=True)
        Config("worker.example:443", "/test", "test", tls=True)
        for endpoint in (
            "user:pass@localhost:50055",
            "localhost:50055/path",
            "localhost",
            "localhost:0",
            "localhost:65536",
        ):
            with self.assertRaises(ValueError):
                Config(endpoint, "/test", "test")

    def test_worker_id_and_finite_limits(self):
        for worker_id in ("", "bad worker", "bad/worker", "я-worker", "x" * 129):
            with self.subTest(worker_id=worker_id):
                with self.assertRaises(ValueError):
                    Config("localhost:50055", "/test", worker_id)
        Config("localhost:50055", "/test", "mac.local:cpu-1_uuid")
        for name in (
            "poll_interval",
            "rpc_timeout",
            "download_timeout",
            "shutdown_grace",
            "max_input_bytes",
        ):
            for value in (0, -1, float("nan"), float("inf")):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(ValueError):
                        config(**{name: value})
        with patch.dict(
            "os.environ", {"REMOTE_WORKER_TOKEN_FILE": "/test"}, clear=True
        ):
            with patch(
                "whisper.remote_worker.socket.gethostname",
                return_value="my host/invalid" * 20,
            ):
                self.assertRegex(
                    Config.from_env().worker_id, r"^[A-Za-z0-9_.:-]{1,128}$"
                )

    def test_token_file_validation(self):
        with tempfile.TemporaryDirectory() as temp:
            path = pathlib.Path(temp) / "token"
            for value in (b"", b"bad\nembedded", b"x" * 31, b"x" * 513, b"x" * 4097):
                path.write_bytes(value)
                with self.assertRaises(ValueError):
                    read_token(str(path))
            path.write_bytes(b"synthetic-worker-token-at-least-32-characters\n")
            self.assertEqual(
                read_token(str(path)), "synthetic-worker-token-at-least-32-characters"
            )


if __name__ == "__main__":
    unittest.main()
