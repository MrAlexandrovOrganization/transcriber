"""Container configuration checks use synthetic paths and no running services."""

import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from whisper.ssh_tunnel import arguments
from whisper.worker_health import healthy, report

ROOT = Path(__file__).resolve().parents[1]


class ContainerHealthTests(unittest.TestCase):
    def test_readiness_expires_and_is_withdrawn(self):
        with tempfile.TemporaryDirectory() as temp:
            path = str(Path(temp) / "ready")
            self.assertFalse(healthy(path))
            report(path, True)
            self.assertTrue(healthy(path))
            with patch(
                "whisper.worker_health.time.time", return_value=time.time() + 50
            ):
                self.assertFalse(healthy(path))
            report(path, False)
            self.assertFalse(healthy(path))
            report(path, False)

    def test_invalid_and_future_readiness_are_unhealthy(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "ready"
            for content in ("invalid", str(time.time() + 60)):
                path.write_text(content)
                self.assertFalse(healthy(str(path)))
        report("", True)
        report("", False)


class TunnelTests(unittest.TestCase):
    def test_forwarding_is_loopback_only_and_host_keys_are_required(self):
        args = arguments({"SSH_HOST": "synthetic.example", "SSH_USER": "worker"})
        self.assertIn("127.0.0.1:50055:127.0.0.1:50055", args)
        self.assertIn("StrictHostKeyChecking=yes", args)
        self.assertIn("BatchMode=yes", args)
        self.assertEqual(args[-1], "synthetic.example")

    def test_invalid_values_cannot_inject_ssh_options(self):
        base = {"SSH_HOST": "synthetic.example", "SSH_USER": "worker"}
        for key, values in {
            "SSH_HOST": ["", "-oProxyCommand=bad", "host\nvalue", "host name"],
            "SSH_USER": ["-root", "root\nvalue", "user@host"],
            "SSH_PORT": ["0", "65536", "bad"],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    arguments(dict(base, **{key: value}))


@unittest.skipUnless(shutil.which("docker"), "Docker CLI required for Compose checks")
class ComposeTests(unittest.TestCase):
    def compose(self, profiles, autodeploy=False):
        with tempfile.TemporaryDirectory() as temp:
            env_file = Path(temp) / "synthetic.env"
            env_file.write_text(
                "TRANSCRIBER_ROLE=workers\n"
                f"COMPOSE_PROFILES={profiles}\n"
                "CPU_WORKER_ID=synthetic-cpu\nGPU_WORKER_ID=synthetic-gpu\n"
                "CPU_MODEL=large-v3\nGPU_MODEL=large-v3-turbo\n"
                "MODEL_CACHE_PATH=/synthetic/models\n"
                "WORKER_TOKEN_PATH=/synthetic/token\n"
                "SSH_HOST=synthetic.example\nSSH_USER=synthetic\n"
                "SSH_IDENTITY_PATH=/synthetic/key\n"
                "SSH_KNOWN_HOSTS_PATH=/synthetic/known_hosts\n"
            )
            env = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith(("COMPOSE_", "TRANSCRIBER_", "WORKER_"))
            }
            env.update(
                PYTHON_VERSION="synthetic",
                POETRY_VERSION="synthetic",
                GRPCIO_TOOLS_VERSION="synthetic",
                CUDNN_VERSION="synthetic",
                CUBLAS_VERSION="synthetic",
            )
            if autodeploy:
                env.update(
                    WORKER_CPU_IMAGE="synthetic/cpu@sha256:" + "a" * 64,
                    WORKER_GPU_IMAGE="synthetic/gpu@sha256:" + "b" * 64,
                    COMPOSE_PROJECT_NAME="synthetic-workers",
                    WORKER_REGISTRY="ghcr.io/synthetic/transcriber",
                )
            command = [
                "docker",
                "compose",
                "--env-file",
                str(env_file),
                "-f",
                "compose.workers.yml",
                "config",
                "--format",
                "json",
            ]
            if autodeploy:
                command[6:6] = ["-f", "compose.workers.autodeploy.yml"]
            result = subprocess.run(
                command,
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
            )
            return json.loads(result.stdout)

    def test_profiles_select_independent_workers(self):
        for profiles, workers in (
            ("cpu", {"worker-cpu"}),
            ("gpu", {"worker-gpu"}),
            ("cpu,gpu", {"worker-cpu", "worker-gpu"}),
        ):
            with self.subTest(profiles=profiles):
                services = self.compose(profiles)["services"]
                self.assertEqual(set(services), workers | {"worker-tunnel"})
                for worker in workers:
                    config = services[worker]
                    self.assertEqual(config["network_mode"], "service:worker-tunnel")
                    self.assertNotIn("ports", config)
                    self.assertTrue(config["read_only"])
                    self.assertEqual(config["cap_drop"], ["ALL"])
                for config in services.values():
                    for volume in config["volumes"]:
                        self.assertTrue(volume["read_only"])
                        self.assertFalse(volume["bind"].get("create_host_path", False))
                        self.assertEqual(volume["bind"]["propagation"], "rprivate")

    def test_registry_mode_preserves_profiles_and_limits_without_worker_builds(self):
        services = self.compose("cpu,gpu", autodeploy=True)["services"]
        for role in ("cpu", "gpu"):
            worker = services["worker-" + role]
            self.assertNotIn("build", worker)
            self.assertIn("@sha256:", worker["image"])
            self.assertEqual(worker["labels"]["io.transcriber.worker-role"], role)
        self.assertEqual(
            services["worker-cpu"]["environment"]["WHISPER_MODEL"], "large-v3"
        )
        self.assertNotIn("ports", services["worker-updater"])
        self.assertEqual(
            services["worker-updater"]["tmpfs"], ["/tmp:size=16777216,mode=1777"]
        )
        self.assertEqual(
            services["worker-updater"]["environment"]["WORKER_PROJECT"],
            "synthetic-workers",
        )

    def test_models_limits_and_devices_are_per_worker(self):
        services = self.compose("cpu,gpu")["services"]
        cpu, gpu = services["worker-cpu"], services["worker-gpu"]
        self.assertEqual(cpu["environment"]["WHISPER_MODEL"], "large-v3")
        self.assertEqual(gpu["environment"]["WHISPER_MODEL"], "large-v3-turbo")
        self.assertEqual(cpu["environment"]["WHISPER_DEVICE"], "cpu")
        self.assertEqual(gpu["environment"]["WHISPER_DEVICE"], "cuda")
        self.assertNotIn("deploy", cpu)
        self.assertEqual(
            gpu["deploy"]["resources"]["reservations"]["devices"][0]["capabilities"],
            ["gpu"],
        )
        self.assertNotIn("ports", services["worker-tunnel"])


if __name__ == "__main__":
    unittest.main()
