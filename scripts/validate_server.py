"""Check Docker runtime constraints not rejected by Compose config --quiet."""

import json
import sys


def validate(config):
    for name, service in config["services"].items():
        shared = service.get("network_mode", "").startswith(("service:", "container:"))
        if shared and any(
            service.get(key) for key in ("hostname", "networks", "ports")
        ):
            raise ValueError(f"Shared network namespace conflicts in service {name}")
        for mount in service.get("tmpfs", []):
            target = (
                mount.split(":", 1)[0]
                if isinstance(mount, str)
                else mount.get("target", "")
            )
            if not target.startswith("/"):
                raise ValueError(f"Non-absolute tmpfs target in service {name}")
        if name == "rabbitmq" and shared:
            if (
                service.get("environment", {}).get("RABBITMQ_NODENAME")
                != "rabbit@transcriber-rabbitmq"
            ):
                raise ValueError(
                    "Shared RabbitMQ must preserve its restored node identity"
                )


if __name__ == "__main__":
    try:
        validate(json.load(sys.stdin))
    except (ValueError, KeyError, TypeError):
        raise SystemExit(
            "Invalid server runtime configuration; values suppressed"
        ) from None
