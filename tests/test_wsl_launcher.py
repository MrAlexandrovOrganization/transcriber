"""WSL launcher tests use synthetic files and never start production services."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import wsl_worker


class WSLLauncherTests(unittest.TestCase):
    def definitions(self, home):
        return wsl_worker.units(
            home / "checkout",
            home,
            home / "token",
            home / "cudnn",
            {"host": "192.0.2.10", "user": "synthetic", "port": 22},
        )

    def test_install_is_disabled_idempotent_and_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            definitions = self.definitions(home)
            directory = home / "units"
            with patch.object(wsl_worker.subprocess, "run") as run:
                wsl_worker.install(directory, definitions, home)
                initial = {p.name: p.read_bytes() for p in directory.iterdir()}
                wsl_worker.install(directory, definitions, home)
                self.assertEqual(
                    initial, {p.name: p.read_bytes() for p in directory.iterdir()}
                )
                run.assert_not_called()
            self.assertFalse((directory / "default.target.wants").exists())
            self.assertIn(
                "PartOf=transcriber-wsl.target",
                definitions["transcriber-wsl-gpu.service"],
            )
            for role in ("cpu", "gpu"):
                content = definitions[f"transcriber-wsl-{role}.service"]
                self.assertIn("MemorySwapMax=0", content)
                self.assertIn("KillMode=control-group", content)
                self.assertIn("NoNewPrivileges=yes", content)
                self.assertIn("Restart=always", content)
                self.assertIn("ProtectHome=read-only", content)
                self.assertIn("REMOTE_WORKER_ENDPOINT=127.0.0.1:50055", content)
                self.assertEqual(
                    os.stat(directory / f"transcriber-wsl-{role}.service").st_mode
                    & 0o777,
                    0o600,
                )
            self.assertIn("CPUQuota=150%", definitions["transcriber-wsl-gpu.service"])
            self.assertIn("CPUQuota=400%", definitions["transcriber-wsl-cpu.service"])

    def test_model_device_threads_and_dedicated_tunnel(self):
        home = Path("/synthetic")
        definitions = self.definitions(home)
        gpu = wsl_worker.worker_environment("gpu", home, home / "token", home / "cudnn")
        cpu = wsl_worker.worker_environment("cpu", home, home / "token", home / "cudnn")
        self.assertEqual(
            (
                gpu["WORKER_ID"],
                gpu["WHISPER_MODEL"],
                gpu["WHISPER_DEVICE"],
                gpu["WHISPER_COMPUTE_TYPE"],
                gpu["WHISPER_CPU_THREADS"],
            ),
            ("wsl-gpu", "small", "cuda", "int8_float16", "2"),
        )
        self.assertEqual(
            (
                cpu["WORKER_ID"],
                cpu["WHISPER_MODEL"],
                cpu["WHISPER_DEVICE"],
                cpu["WHISPER_COMPUTE_TYPE"],
                cpu["WHISPER_CPU_THREADS"],
            ),
            ("wsl-cpu", "large", "cpu", "int8", "4"),
        )
        self.assertEqual(gpu["WHISPER_LOCAL_FILES_ONLY"], "1")
        self.assertEqual(cpu["HF_HUB_OFFLINE"], "1")
        tunnel = definitions["transcriber-wsl-tunnel.service"]
        for option in (
            "127.0.0.1:50055:127.0.0.1:50055",
            "StrictHostKeyChecking=yes",
            "IdentitiesOnly=yes",
            "UserKnownHostsFile=/synthetic/.ssh/known_hosts_transcriber",
            "id_ed25519_transcriber",
            "HostName=192.0.2.10",
            "User=synthetic",
        ):
            self.assertIn(option, tunnel)
        self.assertNotIn("StrictHostKeyChecking=no", tunnel)

    def test_unknown_unit_symlink_and_drop_in_are_preserved(self):
        for variant in ("content", "symlink", "drop-in"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as temp:
                home = Path(temp)
                directory = home / "units"
                directory.mkdir()
                definitions = self.definitions(home)
                path = directory / "transcriber-wsl.target"
                if variant == "content":
                    path.write_text("unknown user configuration")
                elif variant == "symlink":
                    path.symlink_to(home / "missing")
                else:
                    drop_in = directory / "transcriber-wsl.target.d"
                    drop_in.mkdir()
                    (drop_in / "override.conf").write_text("unknown override")
                with self.assertRaises(ValueError):
                    wsl_worker.install(directory, definitions, home)
                self.assertEqual(len(list(directory.iterdir())), 1)

    def test_credentials_cannot_be_public_symlinked_or_in_repo(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            project = home / "checkout"
            project.mkdir()
            token = home / "token"
            token.write_text("synthetic-credential-at-least-thirty-two-characters")
            token.chmod(0o644)
            with self.assertRaises(ValueError):
                wsl_worker.private_file(token, project)
            token.chmod(0o600)
            wsl_worker.private_file(token, project)
            link = home / "link"
            link.symlink_to(token)
            with self.assertRaises(ValueError):
                wsl_worker.private_file(link, project)
            with self.assertRaises(ValueError):
                wsl_worker.private_file(project / "secret", project)

    def test_preflight_never_calls_network_or_worker_loop(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            project = home / "checkout"
            (project / "scripts").mkdir(parents=True)
            (project / "scripts/run_logged.py").touch()
            (home / ".ssh").mkdir()
            (home / ".ssh/known_hosts_transcriber").touch()
            response = subprocess.CompletedProcess([], 0, b"", b"")
            with (
                patch.object(wsl_worker, "private_file"),
                patch.object(
                    wsl_worker.subprocess, "run", return_value=response
                ) as run,
            ):
                wsl_worker.preflight(
                    project,
                    home,
                    home / "token",
                    home / "cudnn",
                    home / "synthetic.wav",
                )
            self.assertEqual(run.call_count, 2)
            for call in run.call_args_list:
                code = call.args[0][2]
                self.assertIn("FasterWhisperEngine", code)
                self.assertIn("e.transcribe", code)
                self.assertNotIn("open_channel", code)
                self.assertNotIn("RemoteWorker(", code)
                self.assertNotIn("ReportAvailability", code)

    def test_systemctl_errors_do_not_disclose_captured_output(self):
        response = subprocess.CompletedProcess(
            [], 1, b"synthetic secret stdout", b"synthetic secret stderr"
        )
        with patch.object(wsl_worker.subprocess, "run", return_value=response):
            with self.assertRaises(ValueError) as error:
                wsl_worker.systemctl("enable", "--now", "transcriber-wsl.target")
        self.assertIn("exit 1", str(error.exception))
        self.assertNotIn("secret", str(error.exception))

    def test_unit_quote_rejects_injection_and_escapes_specifiers(self):
        self.assertEqual(
            wsl_worker.quote("/synthetic a/%name"), '"/synthetic a/%%name"'
        )
        for value in ("a\nb", "a\rb", "a\0b"):
            with self.assertRaises(ValueError):
                wsl_worker.quote(value)


if __name__ == "__main__":
    unittest.main()
