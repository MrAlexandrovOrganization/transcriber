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


@unittest.skipUnless(shutil.which("docker"), "Docker CLI required for Compose checks")
class ComposeTests(unittest.TestCase):
    def compose(
        self,
        profiles,
        autodeploy=False,
        wsl_direct=False,
        tailnet=False,
        audio_bytes=None,
    ):
        with tempfile.TemporaryDirectory() as temp:
            env_file = Path(temp) / "synthetic.env"
            env_file.write_text(
                "TRANSCRIBER_ROLE=workers\n"
                f"COMPOSE_PROFILES={profiles}\n"
                "CPU_WORKER_ID=synthetic-cpu\nGPU_WORKER_ID=synthetic-gpu\n"
                "CPU_MODEL=large-v3\nGPU_MODEL=large-v3-turbo\n"
                "MODEL_CACHE_PATH=/synthetic/models\n"
                "CPU_WORKER_TOKEN_PATH=/synthetic/cpu.token\n"
                "GPU_WORKER_TOKEN_PATH=/synthetic/gpu.token\n"
                "REMOTE_WORKER_ENDPOINT=workers.example.com:443\n"
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
                TAILSCALE_VERSION="synthetic",
            )
            if audio_bytes is not None:
                env["MAX_AUDIO_BYTES"] = str(audio_bytes)
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
                "compose/workers.yml",
                "--project-directory",
                str(ROOT),
                "config",
                "--format",
                "json",
            ]
            if autodeploy:
                command[6:6] = ["-f", "compose/workers.autodeploy.yml"]
            if wsl_direct:
                command[6:6] = ["-f", "compose/workers.wsl-gpu.yml"]
            if tailnet:
                env.update(
                    WORKER_TAILNET_HOSTNAME="synthetic-workers",
                    HEADSCALE_LOGIN_SERVER="https://headscale.example.com",
                    HEADSCALE_AUTHKEY_PATH="/synthetic/headscale.key",
                )
                command[6:6] = ["-f", "compose/workers.tailnet.yml"]
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
                self.assertEqual(set(services), workers)
                for worker in workers:
                    config = services[worker]
                    self.assertNotIn("network_mode", config)
                    self.assertEqual(config["environment"]["REMOTE_WORKER_TLS"], "1")
                    self.assertEqual(
                        config["environment"]["REMOTE_WORKER_ENDPOINT"],
                        "workers.example.com:443",
                    )
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
        self.assertNotEqual(cpu["volumes"][1]["source"], gpu["volumes"][1]["source"])

    def test_wsl_direct_gpu_preserves_hardening_and_cpu_configuration(self):
        for autodeploy in (False, True):
            with self.subTest(autodeploy=autodeploy):
                normal = self.compose("cpu,gpu", autodeploy=autodeploy)["services"]
                direct = self.compose(
                    "cpu,gpu", autodeploy=autodeploy, wsl_direct=True
                )["services"]
                self.assertEqual(normal["worker-cpu"], direct["worker-cpu"])
                gpu = direct["worker-gpu"]
                reservations = (
                    gpu.get("deploy", {}).get("resources", {}).get("reservations", {})
                )
                self.assertFalse(reservations.get("devices"))
                self.assertEqual(len(gpu["devices"]), 1)
                self.assertEqual(gpu["devices"][0]["source"], "/dev/dxg")
                self.assertEqual(gpu["devices"][0]["target"], "/dev/dxg")
                for key in (
                    "user",
                    "read_only",
                    "cap_drop",
                    "security_opt",
                    "cpus",
                    "mem_limit",
                ):
                    self.assertEqual(gpu[key], normal["worker-gpu"][key])
                mounts = {volume["target"]: volume for volume in gpu["volumes"]}
                for path in ("/usr/lib/wsl/lib", "/usr/lib/wsl/drivers"):
                    self.assertEqual(mounts[path]["source"], path)
                    self.assertTrue(mounts[path]["read_only"])
                    self.assertFalse(
                        mounts[path]["bind"].get("create_host_path", False)
                    )
                    self.assertEqual(mounts[path]["bind"]["propagation"], "rprivate")
                # Serialized config retains escapes for expansion in the image.
                self.assertIn("$$LD_LIBRARY_PATH", gpu["entrypoint"][2])
                self.assertIn('exec "$$@"', gpu["entrypoint"][2])

    def test_worker_audio_limit_uses_shared_selector(self):
        from whisper.limits import DEFAULT_MAX_AUDIO_BYTES

        for value in (DEFAULT_MAX_AUDIO_BYTES, 536870912):
            services = self.compose("cpu,gpu", audio_bytes=value)["services"]
            for name in ("worker-cpu", "worker-gpu"):
                self.assertEqual(
                    int(services[name]["environment"]["MAX_AUDIO_BYTES"]), value
                )

    def test_tailnet_preserves_worker_security_and_resource_configuration(self):
        normal = self.compose("cpu,gpu", autodeploy=True, wsl_direct=True)["services"]
        shared = self.compose(
            "cpu,gpu", autodeploy=True, wsl_direct=True, tailnet=True
        )["services"]
        vpn = shared["worker-tailnet"]
        self.assertNotIn("ports", vpn)
        self.assertNotIn("TS_AUTHKEY", vpn["environment"])
        self.assertIn(
            "--auth-key=file:/run/secrets/", vpn["environment"]["TS_EXTRA_ARGS"]
        )
        for role in ("cpu", "gpu"):
            worker = shared["worker-" + role]
            self.assertEqual(worker["network_mode"], "service:worker-tailnet")
            for key in (
                "user",
                "read_only",
                "cap_drop",
                "environment",
                "volumes",
                "cpus",
                "mem_limit",
            ):
                self.assertEqual(worker[key], normal["worker-" + role][key])


if __name__ == "__main__":
    unittest.main()
