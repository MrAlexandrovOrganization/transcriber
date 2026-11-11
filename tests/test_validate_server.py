"""Reject configurations Docker cannot create before touching durable services."""

import unittest

from scripts.validate_server import validate


class ServerConfigurationTests(unittest.TestCase):
    def test_shared_namespace_rejects_incompatible_docker_options(self):
        for key, value in (
            ("hostname", "conflicting"),
            ("ports", ["5671:5671"]),
            ("networks", {"example": {}}),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(
                    {
                        "services": {
                            "backend": {"network_mode": "service:pod", key: value}
                        }
                    }
                )

    def test_tmpfs_requires_an_absolute_target_after_yaml_parsing(self):
        validate({"services": {"router": {"tmpfs": ["/tmp:size=1m,mode=1777"]}}})
        with self.assertRaises(ValueError):
            validate({"services": {"router": {"tmpfs": ["/tmp:size=1m", "mode=1777"]}}})

    def test_broker_cannot_silently_change_its_persisted_node_name(self):
        service = {
            "network_mode": "service:pod",
            "environment": {"RABBITMQ_NODENAME": "rabbit@transcriber-rabbitmq"},
        }
        validate({"services": {"rabbitmq": service}})
        for name in ("", "rabbit@another-host"):
            service["environment"]["RABBITMQ_NODENAME"] = name
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate({"services": {"rabbitmq": service}})
