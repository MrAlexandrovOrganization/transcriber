"""The dedicated deploy key cannot select another project or inject SSH options."""

import io
import json
import unittest

from scripts.coordinator_deploy import endpoint
from scripts.coordinator_receiver import receive


class CoordinatorDeployTests(unittest.TestCase):
    def payload(self):
        return {
            "DEPLOY_SHA": "a" * 40,
            "IMAGE_MANIFEST_B64": "synthetic-manifest",
            "IMAGE_REPOSITORY": "ghcr.io/MrAlexandrovOrganization/transcriber",
            "GHCR_USER": "synthetic-actor",
            "GHCR_TOKEN": "synthetic-registry-token",
        }

    def test_only_expected_registry_and_payload_fields_are_accepted(self):
        value = receive(io.BytesIO(json.dumps(self.payload()).encode()))
        self.assertEqual(
            value["IMAGE_REPOSITORY"], "ghcr.io/mralexandrovorganization/transcriber"
        )
        for field, invalid in (
            ("DEPLOY_SHA", "a; execute"),
            ("IMAGE_REPOSITORY", "ghcr.io/other/project"),
            ("GHCR_USER", "user with spaces"),
            ("GHCR_TOKEN", "synthetic-secret\nextra"),
            ("PROJECT_PATH", "/another/project"),
        ):
            payload = self.payload()
            payload[field] = invalid
            with self.subTest(field=field), self.assertRaises(ValueError) as caught:
                receive(io.BytesIO(json.dumps(payload).encode()))
            self.assertNotIn("synthetic-secret", str(caught.exception))

    def test_payload_size_is_bounded(self):
        with self.assertRaises(ValueError):
            receive(io.BytesIO(b"x" * 65537))

    def test_ssh_endpoint_rejects_options_and_shell_syntax(self):
        good = {"host": "192.0.2.1", "port": 22, "user": "synthetic"}
        self.assertEqual(endpoint(good), good)
        for field, value in (
            ("host", "host\nProxyCommand unwanted"),
            ("host", "$(unwanted)"),
            ("user", "user name"),
            ("port", True),
            ("port", 0),
            ("extra", "unexpected"),
        ):
            bad = dict(good, **{field: value})
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                endpoint(bad)
