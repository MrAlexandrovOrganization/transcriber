"""Per-worker bearer credentials; the coordinator stores only SHA-256 hashes."""

import hashlib
import hmac
import json
import re
from pathlib import Path


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate credential field")
        result[key] = value
    return result


class WorkerCredentials:
    def __init__(self, hashes):
        self._hashes = dict(hashes)

    @classmethod
    def from_file(cls, path):
        try:
            with Path(path).open("rb") as stream:
                raw = stream.read(65537)
            if len(raw) > 65536:
                raise ValueError()
            document = json.loads(raw, object_pairs_hook=_unique_object)
            if (
                not isinstance(document, dict)
                or set(document) != {"version", "workers"}
                or type(document["version"]) is not int
                or document["version"] != 1
            ):
                raise ValueError()
            hashes = document["workers"]
            if not isinstance(hashes, dict) or not 1 <= len(hashes) <= 128:
                raise ValueError()
            for worker_id, digest in hashes.items():
                if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", worker_id):
                    raise ValueError()
                if not isinstance(digest, str) or not re.fullmatch(
                    "[0-9a-f]{64}", digest
                ):
                    raise ValueError()
            if len(set(hashes.values())) != len(hashes):
                raise ValueError()
            return cls(hashes)
        except (OSError, ValueError, TypeError, UnicodeError):
            # Never include paths, registry contents or JSON decoder excerpts.
            raise ValueError("Invalid worker credential registry") from None

    def authenticate(self, authorization):
        if not isinstance(authorization, str) or not authorization.startswith(
            "Bearer "
        ):
            return None
        token = authorization[7:]
        if not 32 <= len(token) <= 512 or any(not 33 <= ord(c) <= 126 for c in token):
            return None
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        identity = None
        for worker_id, expected in self._hashes.items():
            if hmac.compare_digest(digest, expected):
                identity = worker_id
        return identity
