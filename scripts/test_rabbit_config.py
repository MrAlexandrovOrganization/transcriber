"""Write ONLY synthetic, loopback test credentials for make test-rabbit-up."""

import json
from pathlib import Path

path = Path(__file__).resolve().parents[1] / ".test-rabbitmq.json"
path.write_text(
    json.dumps(
        {
            "host": "localhost",
            "port": 56739,
            "virtual_host": "transcriber_pilot",
            "username": "pilot",
            "password": "synthetic-rabbit-password-for-isolated-tests",
            "tls": False,
        }
    )
    + "\n"
)
path.chmod(0o600)
