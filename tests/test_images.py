"""Check release fencing and failure recovery without Docker or credentials."""

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "server_images", Path(__file__).resolve().parents[1] / "scripts/images.py"
)
images = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(images)


class ImageReleaseTests(unittest.TestCase):
    def manifest(self):
        return {
            "revision": "a" * 40,
            "platform": "linux/amd64",
            "images": dict.fromkeys(
                images.SERVICES,
                "ghcr.io/mralexandrovorganization/transcriber@sha256:" + "b" * 64,
            ),
        }

    def test_rejects_wrong_revision_platform_service_set_and_mutable_tags(self):
        for field, value in [
            ("revision", "c" * 40),
            ("platform", "linux/arm64"),
            ("images", {"whisper-ingress": "repository:latest"}),
        ]:
            manifest = self.manifest()
            manifest[field] = value
            with (
                self.subTest(field=field),
                patch.object(images, "revision", return_value="a" * 40),
                self.assertRaises(ValueError),
            ):
                images.validate(manifest)

    def test_failed_activation_keeps_previous_success_and_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            current = self.manifest()
            current["revision"] = "c" * 40
            images.write(state / "current.json", current)
            images.write(state / "release.json", self.manifest())
            calls = []

            def run(*args, **kwargs):
                calls.append(args)
                if args[0] == "make":
                    raise OSError("synthetic activation failure")

            with (
                patch.object(images, "STATE", state),
                patch.object(images, "revision", return_value="a" * 40),
                patch.object(images, "run", side_effect=run),
                patch.object(images, "compose", return_value=["docker", "compose"]),
                patch.dict(images.os.environ, {}, clear=True),
            ):
                with self.assertRaises(OSError):
                    images.deploy()
            self.assertEqual(calls[0][0:2], ("docker", "pull"))
            self.assertEqual(calls[1][0:2], ("make", "image-activate"))
            self.assertIn(
                '"revision": "' + "c" * 40, (state / "current.json").read_text()
            )
            self.assertTrue((state / "requested.json").exists())
            self.assertFalse((state / "previous.json").exists())
