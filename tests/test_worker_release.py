"""Release validation and crash recovery use synthetic Docker state only."""

import copy
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.worker_release import LABEL, atomic_json, publish, release
from scripts.worker_updater import OPT_IN, ROLE, APIError, Updater, replacement

REGISTRY = "ghcr.io/example/transcriber"
IMAGE = REGISTRY + "-worker-cpu@sha256:" + "a" * 64


def container(ident="old", name="/workers-worker-cpu-1", healthy=True):
    return {
        "Id": ident,
        "Name": name,
        "Image": "old-image",
        "Config": {
            "Image": "old-ref",
            "Hostname": "derived-inspect-hostname",
            "Domainname": "derived-domain",
            "Env": ["TOKEN_FILE=/secret"],
            "Labels": {
                "com.docker.compose.project": "workers",
                "com.docker.compose.service": "worker-cpu",
                OPT_IN: "true",
                ROLE: "cpu",
                "io.transcriber.drain-protocol": "1",
            },
        },
        "HostConfig": {
            "NetworkMode": "container:tunnel-id",
            "Mounts": [{"Source": "/translated/wsl/path", "Target": "/secret"}],
            "DeviceRequests": [{"Driver": "nvidia"}],
        },
        "State": {
            "Running": True,
            "Health": {"Status": "healthy" if healthy else "unhealthy"},
        },
    }


class FakeDocker:
    def __init__(self, *containers):
        self.containers = {item["Id"]: item for item in containers}
        self.actions = []
        self.ready = True
        self.new_healthy = True

    def inspect(self, name):
        return next(
            (
                item
                for item in self.containers.values()
                if item["Id"] == name or item["Name"].lstrip("/") == name
            ),
            None,
        )

    def action(self, ident, action, **params):
        self.actions.append((ident, action, params))
        item = self.inspect(ident)
        if action in ("stop", "start"):
            item["State"]["Running"] = action == "start"
        if action == "rename":
            item["Name"] = "/" + params["name"]

    def remove(self, name):
        del self.containers[self.inspect(name)["Id"]]

    def pull(self, image):
        return {"Id": "new-image"}

    def drained(self, name):
        return self.ready

    def call(self, method, path, data=None):
        if method == "GET" and path.startswith("/images/"):
            return {"Config": {}}
        assert method == "POST" and path.startswith("/containers/create?")
        item = container("new", healthy=self.new_healthy)
        item["Config"] = data
        item["Image"] = "new-image"
        self.containers["new"] = item
        return {"Id": "new"}


class ReleaseTests(unittest.TestCase):
    def test_publisher_reuses_dependency_cache_and_labels_exported_revision(self):
        with (
            patch.dict(
                os.environ,
                {
                    key: "synthetic"
                    for key in (
                        "PYTHON_VERSION",
                        "POETRY_VERSION",
                        "GRPCIO_TOOLS_VERSION",
                        "CUDNN_VERSION",
                        "CUBLAS_VERSION",
                    )
                },
            ),
            patch("scripts.worker_release.run") as run,
        ):
            publish(REGISTRY, "f" * 40, "amd64")
        self.assertEqual(len(run.call_args_list), 2)
        for call, role in zip(run.call_args_list, ("cpu", "gpu"), strict=True):
            self.assertIn("org.opencontainers.image.revision=" + "f" * 40, call.args)
            self.assertIn(
                f"type=registry,ref={REGISTRY}-worker-{role}:buildcache-amd64,mode=max",
                call.args,
            )
            self.assertFalse(
                any(str(arg).startswith("SOURCE_REVISION=") for arg in call.args)
            )

    def test_only_expected_registry_digest_and_revision_are_accepted(self):
        labels = {
            LABEL + "revision": "f" * 40,
            LABEL + "cpu": IMAGE,
            LABEL + "gpu": REGISTRY + "-worker-gpu@sha256:" + "b" * 64,
        }
        self.assertEqual(release(labels, REGISTRY)[1]["cpu"], IMAGE)
        for key, value in (
            ("revision", "main"),
            ("cpu", "evil/image:latest"),
            ("gpu", REGISTRY + "-worker-gpu:latest"),
        ):
            with self.assertRaises(ValueError):
                release({**labels, LABEL + key: value}, REGISTRY)

    def test_replacement_preserves_configuration_and_desktop_paths(self):
        old = container()
        before = copy.deepcopy(old)
        new = replacement(old, IMAGE, "new-image")
        self.assertEqual(new["Env"], old["Config"]["Env"])
        self.assertEqual(new["HostConfig"], old["HostConfig"])
        self.assertNotIn("Hostname", new)
        self.assertNotIn("Domainname", new)
        self.assertEqual(new["Labels"]["com.docker.compose.image"], "new-image")
        self.assertEqual(old, before)

    def test_wsl_gpu_replacement_keeps_wrapper_and_uses_new_image_libraries(self):
        old = container()
        wrapper = [
            "/bin/sh",
            "-c",
            'export LD_LIBRARY_PATH="/usr/lib/wsl/lib:$LD_LIBRARY_PATH"; exec "$@"',
            "--",
        ]
        old["Config"]["Entrypoint"] = wrapper
        old["Config"]["Cmd"] = ["python", "-m", "whisper.remote_worker"]
        old["Config"]["Env"].append("LD_LIBRARY_PATH=/old-image/cuda")
        old["HostConfig"]["DeviceRequests"] = []
        old["HostConfig"]["Devices"] = [
            {
                "PathOnHost": "/dev/dxg",
                "PathInContainer": "/dev/dxg",
                "CgroupPermissions": "rwm",
            }
        ]
        defaults = {
            "Entrypoint": None,
            "Cmd": old["Config"]["Cmd"],
            "Env": ["LD_LIBRARY_PATH=/old-image/cuda"],
        }
        new = replacement(
            old,
            IMAGE,
            "new-image",
            defaults,
            {"Env": ["LD_LIBRARY_PATH=/new-image/cuda"]},
        )
        self.assertEqual(new["Entrypoint"], wrapper)
        self.assertEqual(new["HostConfig"], old["HostConfig"])
        self.assertNotIn("LD_LIBRARY_PATH=/old-image/cuda", new["Env"])
        self.assertNotIn("Cmd", new)  # Let the replacement image supply its command.

    def test_recovery_rolls_back_unhealthy_replacement_and_quarantines_it(self):
        with tempfile.TemporaryDirectory() as temp:
            old = container(name="/workers-worker-cpu-1-rollback-old")
            new = container("new", healthy=False)
            docker = FakeDocker(old, new)
            updater = Updater(docker, "workers", REGISTRY, temp)
            updater.state["pending"] = {
                "old": "old",
                "name": "workers-worker-cpu-1",
                "service": "cpu",
                "image": IMAGE,
            }
            updater.save()
            # Simulate updater process restart with a durable journal.
            restarted = Updater(docker, "workers", REGISTRY, temp)
            restarted.recover()
            self.assertIs(docker.inspect("workers-worker-cpu-1"), old)
            self.assertEqual(restarted.state["failed"]["cpu"], IMAGE)
            self.assertNotIn("pending", restarted.state)
            self.assertFalse(docker.inspect("new"))
            # No signal may hit the restored process before Python installs handlers.
            self.assertEqual(docker.actions[-1], ("old", "start", {}))

    def test_recovery_commits_a_healthy_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            docker = FakeDocker(container(name="/backup"), container("new"))
            updater = Updater(docker, "workers", REGISTRY, temp)
            updater.state["pending"] = {
                "old": "old",
                "name": "workers-worker-cpu-1",
                "service": "cpu",
                "image": IMAGE,
            }
            updater.recover()
            self.assertIsNone(docker.inspect("old"))
            self.assertIsNotNone(docker.inspect("new"))

    def test_crash_during_drain_resumes_old_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            docker = FakeDocker(container())
            updater = Updater(docker, "workers", REGISTRY, temp)
            updater.state["pending"] = {
                "old": "old",
                "name": "workers-worker-cpu-1",
                "service": "cpu",
                "image": IMAGE,
            }
            updater.recover()
            self.assertIn(("old", "kill", {"signal": "SIGUSR2"}), docker.actions)

    def test_busy_worker_is_not_stopped_and_drain_is_cancelled(self):
        with tempfile.TemporaryDirectory() as temp:
            docker = FakeDocker(container())
            docker.ready = False
            updater = Updater(docker, "workers", REGISTRY, temp, drain_timeout=0)
            with patch("scripts.worker_updater.STOP.wait", return_value=False):
                updater.update(docker.inspect("old"), IMAGE)
            self.assertFalse(any(action == "stop" for _, action, _ in docker.actions))
            self.assertIn(("old", "kill", {"signal": "SIGUSR2"}), docker.actions)
            self.assertNotIn("pending", updater.state)

    def test_successful_update_replaces_only_after_drain(self):
        with tempfile.TemporaryDirectory() as temp:
            docker = FakeDocker(container())
            updater = Updater(docker, "workers", REGISTRY, temp)
            with patch("scripts.worker_updater.STOP.wait", return_value=False):
                updater.update(docker.inspect("old"), IMAGE)
            self.assertIsNone(docker.inspect("old"))
            self.assertEqual(docker.inspect("new")["Config"]["Image"], IMAGE)
            self.assertNotIn("pending", updater.state)

    def test_create_failure_recovers_immediately_without_waiting_for_next_poll(self):
        with tempfile.TemporaryDirectory() as temp:
            docker = FakeDocker(container())
            updater = Updater(docker, "workers", REGISTRY, temp)
            original = docker.call

            def call(method, path, data=None):
                if path.startswith("/containers/create"):
                    raise APIError("Docker POST create failed: HTTP 400")
                return original(method, path, data)

            docker.call = call
            with self.assertRaises(APIError):
                updater.update(docker.inspect("old"), IMAGE)
            self.assertTrue(docker.inspect("workers-worker-cpu-1")["State"]["Running"])
            self.assertNotIn("pending", updater.state)
            self.assertEqual(updater.state["failed"]["cpu"], IMAGE)

    def test_unchanged_image_and_failed_release_do_not_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            docker = FakeDocker(container())
            updater = Updater(docker, "workers", REGISTRY, temp)
            old = docker.inspect("old")
            old["Image"] = "new-image"
            updater.update(old, IMAGE)
            updater.state["failed"]["cpu"] = IMAGE
            updater.update(old, IMAGE)
            self.assertEqual(docker.actions, [])

    def test_recovery_refuses_an_unmanaged_container(self):
        with tempfile.TemporaryDirectory() as temp:
            old = container()
            old["Config"]["Labels"]["com.docker.compose.project"] = "other"
            updater = Updater(FakeDocker(old), "workers", REGISTRY, temp)
            updater.state["pending"] = {
                "old": "old",
                "name": "workers-worker-cpu-1",
                "service": "cpu",
                "image": IMAGE,
            }
            with self.assertRaises(APIError):
                updater.recover()

    def test_atomic_state_is_recoverable(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "nested" / "state.json"
            atomic_json(path, {"failed": {"cpu": IMAGE}})
            self.assertEqual(
                Updater(FakeDocker(), "workers", REGISTRY, path.parent).state["failed"][
                    "cpu"
                ],
                IMAGE,
            )
