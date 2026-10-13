"""Validate selected worker configuration without printing secrets or paths."""

import json
import sys


def validate(document):
    workers = {
        name: service
        for name, service in document["services"].items()
        if name in {"worker-cpu", "worker-gpu"}
    }
    if not workers:
        raise ValueError("Enable cpu and/or gpu in COMPOSE_PROFILES")
    paths = []
    for name, service in workers.items():
        sources = [
            mount["source"]
            for mount in service["volumes"]
            if mount["target"] == "/run/credentials/token"
        ]
        if len(sources) != 1 or sources[0].startswith("/missing/"):
            raise ValueError("Set the selected worker's token path")
        if service["environment"].get("REMOTE_WORKER_TLS") != "1":
            raise ValueError("Container workers require TLS")
        paths.extend(sources)
    if len(set(paths)) != len(paths):
        raise ValueError("TLS workers require distinct token files")


if __name__ == "__main__":
    try:
        validate(json.load(sys.stdin))
    except (ValueError, KeyError, TypeError) as error:
        print(
            str(error)
            if isinstance(error, ValueError)
            else "Invalid worker configuration",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
