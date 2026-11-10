"""Safe worker diagnostics and bounded recovery, without network or inference."""

import io
import json
import logging
import unittest
from unittest.mock import Mock, patch

import grpc

from whisper.jsonlog import JSONFormatter, failure_fields
from whisper.metrics import MetricsEndpoint
from whisper.remote_worker import Config, RemoteWorker


class Offline(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class LogDiagnosticsTests(unittest.TestCase):
    def test_failure_fields_preserve_type_and_rpc_code_without_secret_text(self):
        error = Offline(
            "synthetic-password=secret https://example.invalid/signed-secret"
        )
        fields = failure_fields(error, retry_attempt=2, retry_in_seconds=4)
        record = logging.LogRecord(
            "worker", logging.WARNING, "", 1, "retry scheduled", (), None
        )
        record.__dict__.update(fields)
        payload = json.loads(JSONFormatter().format(record))
        self.assertEqual(payload["rpc_code"], "UNAVAILABLE")
        self.assertEqual(payload["error_type"], "Offline")
        self.assertEqual(payload["retry_attempt"], 2)
        self.assertEqual(payload["retry_in_seconds"], 4)
        self.assertNotIn("secret", json.dumps(payload))

    def test_unexpected_failure_retries_with_backoff_and_stops_on_cancellation(self):
        stub = Mock()
        stub.ReportAvailability.side_effect = RuntimeError("synthetic-secret")
        worker = RemoteWorker(
            Config("localhost:50055", "/unused", "test-worker"),
            stub,
            None,
            "synthetic-token",
        )
        waits = []

        def wait(delay):
            waits.append(delay)
            if len(waits) == 7:
                worker.stop.set()
            return worker.stop.is_set()

        with (
            patch.object(worker.stop, "wait", side_effect=wait),
            patch("whisper.remote_worker.random.uniform", return_value=1),
            self.assertLogs("whisper.remote_worker", level="ERROR") as logs,
        ):
            worker.run_forever()
        self.assertEqual(waits, [2, 4, 8, 16, 32, 60, 60])
        self.assertEqual(logs.records[-1].retry_attempt, 7)
        self.assertEqual(logs.records[-1].error_type, "RuntimeError")
        self.assertNotIn("synthetic-secret", " ".join(logs.output))

    def test_unavailable_snapshot_and_disconnected_503_remain_structured(self):
        class FixtureServer:
            serve_forever = Mock()

            def __init__(self, address, handler):
                self.RequestHandlerClass = handler

        store = Mock(worker_policies={})
        store.metrics_snapshot.side_effect = RuntimeError("synthetic-secret")
        with (
            patch("whisper.metrics.HTTPServer", FixtureServer),
            patch("whisper.metrics.threading.Thread"),
        ):
            endpoint = MetricsEndpoint(store, "127.0.0.1:0")
        for disconnected in (False, True):
            with self.subTest(disconnected=disconnected):
                client = Mock()
                client.makefile.return_value = io.BytesIO(
                    b"GET /metrics HTTP/1.0\r\n\r\n"
                )
                if disconnected:
                    client.sendall.side_effect = BrokenPipeError("synthetic-secret")
                with self.assertLogs("whisper.metrics", level="WARNING") as logs:
                    endpoint.server.RequestHandlerClass(
                        client, ("127.0.0.1", 1234), endpoint.server
                    )
                self.assertEqual(logs.records[0].error_type, "RuntimeError")
                self.assertNotIn("synthetic-secret", " ".join(logs.output))
                if not disconnected:
                    response = b"".join(
                        call.args[0] for call in client.sendall.call_args_list
                    )
                    self.assertIn(b"503 Metrics unavailable", response)
