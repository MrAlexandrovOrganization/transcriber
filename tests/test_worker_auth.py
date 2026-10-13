"""Synthetic per-worker identities, validation and secret-safe failures."""

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import grpc

from proto import whisper_pb2 as pb
from proto import whisper_pb2_grpc as rpc
from whisper.worker_api import start_worker_apis
from whisper.worker_auth import WorkerCredentials

CPU_TOKEN = "synthetic-cpu-token-with-at-least-32-characters"
GPU_TOKEN = "synthetic-gpu-token-with-at-least-32-characters"


def registry():
    return {
        "version": 1,
        "workers": {
            worker: hashlib.sha256(token.encode()).hexdigest()
            for worker, token in (("wsl-cpu", CPU_TOKEN), ("wsl-gpu", GPU_TOKEN))
        },
    }


class WorkerCredentialTests(unittest.TestCase):
    def load(self, document):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "private.json"
            path.write_text(
                document if isinstance(document, str) else json.dumps(document)
            )
            return WorkerCredentials.from_file(path)

    def test_individual_tokens_resolve_to_worker_ids(self):
        credentials = self.load(registry())
        self.assertEqual(credentials.authenticate("Bearer " + CPU_TOKEN), "wsl-cpu")
        self.assertEqual(credentials.authenticate("Bearer " + GPU_TOKEN), "wsl-gpu")
        for value in (
            "",
            "Bearer invalid",
            "Basic " + CPU_TOKEN,
            "Bearer " + "x" * 513,
            "Bearer " + "я" * 32,
        ):
            self.assertIsNone(credentials.authenticate(value))

    def test_revocation_and_rotation_do_not_affect_other_worker(self):
        document = registry()
        del document["workers"]["wsl-cpu"]
        credentials = self.load(document)
        self.assertIsNone(credentials.authenticate("Bearer " + CPU_TOKEN))
        self.assertEqual(credentials.authenticate("Bearer " + GPU_TOKEN), "wsl-gpu")

    def test_invalid_registries_do_not_expose_contents(self):
        documents = [
            None,
            [],
            {},
            {"version": True, "workers": {}},
            {"version": 1, "workers": {}},
            {"version": 1, "workers": {"bad worker": "a" * 64}},
            {"version": 1, "workers": {"cpu": CPU_TOKEN}},
            {"version": 1, "workers": {"cpu": "a" * 64, "gpu": "a" * 64}},
            '{"version":1,"version":1,"workers":{}}',
            "sensitive-registry-content" * 4000,
        ]
        for document in documents:
            with (
                self.subTest(document_type=type(document)),
                self.assertRaisesRegex(
                    ValueError, "^Invalid worker credential registry$"
                ),
            ):
                self.load(document)

    def test_missing_registry_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "^Invalid worker credential registry$"):
            WorkerCredentials.from_file("/missing/sensitive-path")


class WorkerListenerTests(unittest.TestCase):
    def test_listener_only_accepts_individual_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            registry_path = Path(temp) / "registry.json"
            registry_path.write_text(json.dumps(registry()))
            ports = []
            factory = grpc.server

            def create(*args, **kwargs):
                server = factory(*args, **kwargs)
                bind = server.add_insecure_port

                def record(address):
                    port = bind(address)
                    ports.append(port)
                    return port

                server.add_insecure_port = record
                return server

            store = SimpleNamespace(
                report_availability=lambda *_: True, availability_ttl_seconds=90
            )
            with (
                patch.dict(
                    os.environ,
                    {
                        "WORKER_PUBLIC_GRPC_ADDR": "127.0.0.1:0",
                        "WORKER_CREDENTIALS_FILE": str(registry_path),
                    },
                    clear=True,
                ),
                patch("whisper.worker_api.grpc.server", side_effect=create),
            ):
                servers = start_worker_apis(store)
            try:
                self.assertEqual(len(servers), 1)
                with grpc.insecure_channel(f"127.0.0.1:{ports[0]}") as channel:
                    method = rpc.WorkerServiceStub(channel).ReportAvailability
                    request = pb.WorkerAvailabilityRequest(
                        worker_id="wsl-cpu",
                        instance_id="00000000-0000-0000-0000-000000000001",
                        ready=True,
                    )
                    self.assertTrue(
                        method(
                            request,
                            metadata=(("authorization", "Bearer " + CPU_TOKEN),),
                            timeout=5,
                        ).accepted
                    )
                    for token, code in (
                        (GPU_TOKEN, grpc.StatusCode.PERMISSION_DENIED),
                        (
                            "synthetic-retired-shared-token-with-at-least-32-characters",
                            grpc.StatusCode.UNAUTHENTICATED,
                        ),
                    ):
                        with self.assertRaises(grpc.RpcError) as error:
                            method(
                                request,
                                metadata=(("authorization", "Bearer " + token),),
                                timeout=5,
                            )
                        self.assertEqual(error.exception.code(), code)
            finally:
                for server in servers:
                    server.stop(0).wait()

    def test_invalid_registry_fails_before_binding(self):
        with (
            patch.dict(
                os.environ,
                {
                    "WORKER_PUBLIC_GRPC_ADDR": "127.0.0.1:0",
                    "WORKER_CREDENTIALS_FILE": "/missing/synthetic.json",
                },
                clear=True,
            ),
            patch("whisper.worker_api.grpc.server") as factory,
            self.assertRaisesRegex(ValueError, "Invalid worker credential registry"),
        ):
            start_worker_apis(None)
        factory.assert_not_called()

    def test_removed_shared_listener_fails_closed(self):
        for key in ("WORKER_GRPC_ADDR", "WORKER_TOKEN_FILE"):
            with (
                self.subTest(key=key),
                patch.dict(os.environ, {key: "synthetic"}, clear=True),
                self.assertRaisesRegex(
                    ValueError, "Shared-token worker listener was removed"
                ),
            ):
                start_worker_apis(None)

    def test_no_listener_without_explicit_address(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(start_worker_apis(None), [])


if __name__ == "__main__":
    unittest.main()
