"""Launcher checks use temporary directories and never bootstrap launchd."""

import io
import os
import pathlib
import plistlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import mac_worker, run_logged
from scripts.mac_worker import agents, check_existing, install, validate_token_path


class MacLauncherTests(unittest.TestCase):
    def test_failed_launchctl_reports_operation_exit_and_safe_stderr(self):
        response = subprocess.CompletedProcess(
            [],
            5,
            stdout=b"secret environment must not be emitted",
            stderr=b"Bootstrap failed: 5: Input/output error\nBearer synthetic-credential\x00",
        )
        with patch.object(mac_worker.subprocess, "run", return_value=response):
            with self.assertRaises(mac_worker.LaunchctlError) as caught:
                mac_worker.launchctl("bootstrap", "gui/501", "/synthetic/agent.plist")
            text = str(caught.exception)
            self.assertIn("launchctl bootstrap failed (exit 5)", text)
            self.assertIn("Input/output error", text)
            self.assertNotIn("synthetic-credential", text)
            self.assertNotIn("environment", text)
            self.assertNotIn("\x00", text)
            self.assertIs(
                mac_worker.launchctl("print", "gui/501/synthetic", check=False),
                response,
            )

    def test_start_error_identifies_agent_without_bootstrapping(self):
        response = subprocess.CompletedProcess([], 113, b"", b"missing synthetic label")
        output = io.StringIO()
        with (
            patch.object(mac_worker.sys, "argv", ["mac_worker.py", "start"]),
            patch.object(mac_worker.sys, "platform", "darwin"),
            patch.object(mac_worker, "preflight"),
            patch.object(mac_worker, "check_existing"),
            patch.object(mac_worker.sys, "stderr", output),
            patch.object(
                mac_worker,
                "launchctl",
                side_effect=[
                    response,
                    mac_worker.LaunchctlError(
                        "launchctl bootstrap failed (exit 5): Input/output error"
                    ),
                ],
            ),
        ):
            self.assertEqual(mac_worker.main(), 1)
        self.assertIn(
            "bootstrap net.mralexandrov.transcriber.tunnel", output.getvalue()
        )
        self.assertIn("exit 5", output.getvalue())

    def test_agent_settings_and_install_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            home = pathlib.Path(temp)
            project = home / "checkout"
            definitions = agents(
                project, home, home / ".config/transcriber/worker.token"
            )
            tunnel, worker = definitions["tunnel"], definitions["worker"]
            for definition in definitions.values():
                self.assertTrue(definition["KeepAlive"])
                self.assertTrue(definition["RunAtLoad"])
                self.assertEqual(definition["ThrottleInterval"], 30)
                self.assertEqual(definition["ProcessType"], "Background")
                self.assertEqual(definition["Nice"], 10)
                self.assertNotIn("StartCalendarInterval", definition)
            self.assertIn("127.0.0.1:50055:127.0.0.1:50055", tunnel["ProgramArguments"])
            self.assertIn("BatchMode=yes", tunnel["ProgramArguments"])
            environment = worker["EnvironmentVariables"]
            self.assertEqual(environment["WHISPER_MODEL"], "large")
            self.assertEqual(environment["WHISPER_CPU_THREADS"], "2")
            self.assertEqual(environment["WHISPER_LOCAL_FILES_ONLY"], "1")
            directory = home / "LaunchAgents"
            install(directory, definitions)
            initial = {path.name: path.read_bytes() for path in directory.iterdir()}
            install(directory, definitions)
            self.assertEqual(
                initial, {path.name: path.read_bytes() for path in directory.iterdir()}
            )
            check_existing(directory, definitions, required=True)

    def test_unknown_plist_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = pathlib.Path(temp)
            definitions = agents(directory / "checkout", directory, directory / "token")
            path = directory / (definitions["tunnel"]["Label"] + ".plist")
            with path.open("wb") as file:
                plistlib.dump(
                    {"Label": "unknown", "ProgramArguments": ["/usr/bin/false"]}, file
                )
            original = path.read_bytes()
            with self.assertRaises(ValueError):
                install(directory, definitions)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(len(list(directory.iterdir())), 1)

    def test_token_requires_private_regular_owned_file_outside_repo(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            project = root / "project"
            project.mkdir()
            token = root / "token"
            token.write_text("synthetic-token-more-than-thirty-two-characters")
            token.chmod(0o644)
            with self.assertRaises(ValueError):
                validate_token_path(token, project)
            token.chmod(0o600)
            validate_token_path(token, project)
            link = root / "link"
            link.symlink_to(token)
            with self.assertRaises(ValueError):
                validate_token_path(link, project)
            internal = project / "token"
            internal.write_text("synthetic-token-more-than-thirty-two-characters")
            internal.chmod(0o600)
            with self.assertRaises(ValueError):
                validate_token_path(internal, project)

    def test_log_rotation_has_bounded_backups(self):
        with tempfile.TemporaryDirectory() as temp:
            path = pathlib.Path(temp) / "worker.stdout.log"
            with patch.object(run_logged, "LOG_BYTES", 128):
                run_logged.capture(io.BytesIO((b"synthetic log line\n") * 100), path)
            files = list(path.parent.iterdir())
            self.assertEqual(len(files), run_logged.LOG_BACKUPS + 1)
            self.assertTrue(all(file.stat().st_size < 128 for file in files))

    def test_supervisor_captures_both_streams_and_exit_code(self):
        with tempfile.TemporaryDirectory() as temp:
            script = pathlib.Path(run_logged.__file__).resolve()
            result = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--log-dir",
                    temp,
                    "--name",
                    "worker",
                    "--",
                    sys.executable,
                    "-c",
                    "import sys; print('synthetic stdout'); print('synthetic stderr',file=sys.stderr); sys.exit(7)",
                ],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 7)
            self.assertEqual(
                (pathlib.Path(temp) / "worker.stdout.log").read_text(),
                "synthetic stdout\n",
            )
            self.assertEqual(
                (pathlib.Path(temp) / "worker.stderr.log").read_text(),
                "synthetic stderr\n",
            )
            self.assertEqual(
                os.stat(pathlib.Path(temp) / "worker.stdout.log").st_mode & 0o077, 0
            )


if __name__ == "__main__":
    unittest.main()
