"""Forced SSH command, installed explicitly outside the coordinator checkout."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path


def receive(stream):
    raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError("Deployment payload too large")
    value = json.loads(raw)
    expected = {
        "DEPLOY_SHA",
        "IMAGE_MANIFEST_B64",
        "IMAGE_REPOSITORY",
        "GHCR_USER",
        "GHCR_TOKEN",
    }
    if set(value) != expected or any(not isinstance(v, str) for v in value.values()):
        raise ValueError("Invalid deployment payload")
    if not re.fullmatch(r"[0-9a-f]{40}", value["DEPLOY_SHA"]):
        raise ValueError("Invalid revision")
    if (
        value["IMAGE_REPOSITORY"].lower()
        != "ghcr.io/mralexandrovorganization/transcriber"
    ):
        raise ValueError("Invalid registry")
    value["IMAGE_REPOSITORY"] = value["IMAGE_REPOSITORY"].lower()
    if not re.fullmatch(r"[A-Za-z0-9_\[\]-]{1,100}", value["GHCR_USER"]):
        raise ValueError("Invalid registry user")
    if not value["GHCR_TOKEN"] or any(c in value["GHCR_TOKEN"] for c in "\r\n\0"):
        raise ValueError("Invalid credential")
    return value


def main():
    if os.environ.get("SSH_ORIGINAL_COMMAND") != "transcriber-deploy":
        raise ValueError("Unsupported SSH command")
    value = receive(sys.stdin.buffer)
    root = Path.home() / "projects/backends/transcriber-coordinator"
    if subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], cwd=root, text=True
    ).strip() != str(root):
        raise ValueError("Unexpected repository root")
    subprocess.run(
        ["git", "fetch", "--quiet", "origin", "refs/heads/main"], cwd=root, check=True
    )
    if (
        subprocess.check_output(
            ["git", "rev-parse", "FETCH_HEAD"], cwd=root, text=True
        ).strip()
        != value["DEPLOY_SHA"]
    ):
        raise ValueError("Stale deployment")
    makefile = subprocess.check_output(
        ["git", "show", value["DEPLOY_SHA"] + ":makefiles/ci-deploy.mk"], cwd=root
    )
    env = os.environ.copy()
    env.update(value)
    subprocess.run(
        ["make", "-f", "-", "ci-deploy"],
        cwd=root,
        input=makefile,
        env=env,
        check=True,
        timeout=1700,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        raise SystemExit("Coordinator receiver rejected or failed deployment") from None
