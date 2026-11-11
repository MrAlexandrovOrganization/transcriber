#!/usr/bin/env python3
"""Canonical service observability package contract; other repositories vendor this file.

Python 3.9+, standard library only. Source packages never execute code.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

SCHEMA = 1
MANIFEST = "monitoring/observability.json"
KINDS = {
    "metrics": "prometheus-targets",
    "dashboards": "grafana-dashboards",
    "datasource_templates": "grafana-datasource-templates",
    "alerting": "grafana-alerting",
    "prometheus_rules": "prometheus-rules",
}
LOCATIONS = {"local", "central"}
ID = re.compile(r"[a-z][a-z0-9-]{0,62}\Z")


def encoded(value):
    return (
        json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    ).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def relative(value):
    if not isinstance(value, str) or "\\" in value:
        raise ValueError("Expected a relative POSIX path")
    p = PurePosixPath(value)
    if (
        not value
        or p.is_absolute()
        or any(x in {"", ".", ".."} for x in value.split("/"))
    ):
        raise ValueError("Unsafe relative path: " + value)
    return p


def source_file(root, value):
    p = relative(value)
    if p.parts[0] not in {"monitoring", "dashboards", "provisioning"}:
        raise ValueError(
            "Observability inputs must be in monitoring/, dashboards/ or provisioning/"
        )
    path = root.joinpath(*p.parts)
    if any(
        root.joinpath(*p.parts[:i]).is_symlink() for i in range(1, len(p.parts) + 1)
    ):
        raise ValueError("Symlinks are not package inputs: " + value)
    if not path.is_file() or path.suffix not in {
        ".json",
        ".alloy",
        ".yaml",
        ".yml",
        ".sql",
    }:
        raise ValueError("Missing or unsupported observability input: " + value)
    return path


def validate(root):
    root = Path(root).resolve()
    doc = json.loads(source_file(root, MANIFEST).read_text())
    if doc.get("schema") != SCHEMA or not ID.fullmatch(doc.get("service", "")):
        raise ValueError("Invalid package schema or service identity")
    if not isinstance(doc.get("components"), list) or not doc["components"]:
        raise ValueError("Declare the owned Compose/service identities")
    for name in doc["components"]:
        if not isinstance(name, str) or not re.fullmatch(
            r"[a-zA-Z0-9_.-]+/(?:[a-zA-Z0-9_.-]+|\*)", name
        ):
            raise ValueError("Invalid component identity")
    files = {MANIFEST: source_file(root, MANIFEST).read_bytes()}
    seen = set()
    for stage in doc.get("logs", []):
        if (
            stage.get("phase") not in {"parse", "normalize"}
            or type(stage.get("order", 100)) is not int
        ):
            raise ValueError("Log stage needs parse/normalize phase and integer order")
        path = source_file(root, stage["file"])
        if path.suffix != ".alloy" or not path.read_text().strip().startswith(
            "stage.match {"
        ):
            raise ValueError("A service log fragment must contain stage.match blocks")
        files[stage["file"]] = path.read_bytes()
    dashboard_uids = set()
    for asset in doc.get("assets", []):
        kind = asset.get("kind")
        if kind not in KINDS:
            raise ValueError("Unknown asset kind")
        destination = str(relative(asset["destination"]))
        identity = (kind, destination)
        if identity in seen:
            raise ValueError("Duplicate output in service package: " + destination)
        seen.add(identity)
        if "hosts" in asset or asset.get("location", "local") not in LOCATIONS:
            raise ValueError(
                "Use logical local/central placement, never physical hosts"
            )
        if any(
            not isinstance(h, str) or not re.fullmatch(r"[a-f0-9]{64}", h)
            for h in asset.get("adopt_sha256", [])
        ):
            raise ValueError("Invalid adoption checksum")
        path = source_file(root, asset["file"])
        body = path.read_bytes()
        if kind == "dashboards":
            dashboard = json.loads(body)
            uid = dashboard.get("uid")
            if not isinstance(uid, str) or not uid or uid in dashboard_uids:
                raise ValueError("Missing/duplicate Grafana dashboard UID")
            dashboard_uids.add(uid)
        if kind == "metrics":
            targets = json.loads(body)
            if not isinstance(targets, list) or any(
                not isinstance(t, dict) or not isinstance(t.get("targets"), list)
                for t in targets
            ):
                raise ValueError(
                    "Invalid Prometheus file-SD targets; use JSON (also valid YAML)"
                )
            for group in targets:
                if any(
                    not isinstance(t, str)
                    or not re.fullmatch(r"[a-zA-Z0-9_.:\[\]-]+", t)
                    for t in group["targets"]
                ):
                    raise ValueError(
                        "Targets must be host:port values without credentials"
                    )
        files[asset["file"]] = body
    for item in doc.get("retire", []):
        if (
            item.get("kind") not in KINDS
            or "hosts" in item
            or item.get("location", "central") not in LOCATIONS
        ):
            raise ValueError("Invalid logical retirement placement")
        relative(item["destination"])
        if not item.get("sha256") or any(
            not re.fullmatch(r"[a-f0-9]{64}", h) for h in item["sha256"]
        ):
            raise ValueError("Retirement requires exact previous file hashes")
    return doc, dict(sorted(files.items()))


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def package(root):
    root = Path(root).resolve()
    doc, files = validate(root)
    lock = {
        "schema": SCHEMA,
        "service": doc["service"],
        "repository": git(root, "remote", "get-url", "origin"),
        "revision": git(root, "rev-parse", "HEAD"),
        "files": {name: digest(body) for name, body in files.items()},
    }
    # A worktree snapshot is explicit, content-addressed and reproducible. It
    # must not be represented as files already present in the base Git commit.
    committed = True
    for name, body in files.items():
        original = subprocess.run(
            ["git", "-C", str(root), "show", lock["revision"] + ":" + name],
            capture_output=True,
            check=False,
        )
        if original.returncode or original.stdout != body:
            committed = False
            break
    lock["source_state"] = "commit" if committed else "worktree"
    lock["content_sha256"] = digest(encoded(lock["files"]))
    return lock, files


def atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".observability-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), 0o644)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def export(root, destination):
    lock, files = package(root)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in sorted({**files, "package.json": encoded(lock)}.items()):
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime, info.uid, info.gid = (
                len(data),
                0o644,
                0,
                0,
                0,
            )
            archive.addfile(info, io.BytesIO(data))
    atomic(Path(destination), buffer.getvalue())
    return lock


def default_root():
    for parent in Path(__file__).resolve().parents:
        if (parent / ".git").exists():
            return parent
    raise ValueError("Run from a Git checkout or supply --root")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "export"))
    parser.add_argument("--root", type=Path, default=default_root())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "check":
        doc, _ = validate(args.root)
        pin = args.root / "monitoring/tool.lock.json"
        if pin.exists():
            lock = json.loads(pin.read_text())
            if lock["sha256"] != digest(Path(__file__).read_bytes()):
                raise ValueError("Observability contract copy does not match its lock")
        print("Observability package validated: " + doc["service"])
    else:
        pin = export(args.root, args.output or args.root / ".observability/package.tar")
        print("Exported " + pin["service"] + " " + pin["content_sha256"])


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from None
