"""Create a NEW private RabbitMQ configuration bundle; never overwrite credentials."""

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import re
import secrets
import subprocess
from pathlib import Path


def password_hash(password):
    salt = os.urandom(4)
    return base64.b64encode(
        salt + hashlib.sha256(salt + password.encode()).digest()
    ).decode()


def provision(output, host, workers, port=5671):
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,253}", host):
        raise ValueError("Invalid RabbitMQ hostname")
    if (
        not workers
        or len(set(workers)) != len(workers)
        or any(
            not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", worker) for worker in workers
        )
    ):
        raise ValueError("Specify distinct worker IDs")
    output = Path(output)
    output.mkdir(mode=0o700)  # Refuse replacement/rotation of an existing bundle.
    server = output / "server"
    server.mkdir(mode=0o755)
    ca = output / "ca"
    ca.mkdir(mode=0o700)

    def openssl(*args):
        result = subprocess.run(["openssl", *args], capture_output=True, timeout=60)
        if result.returncode:
            raise RuntimeError("RabbitMQ certificate generation failed")

    openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:3072",
        "-nodes",
        "-days",
        "365",
        "-subj",
        "/CN=Transcriber RabbitMQ CA",
        "-keyout",
        str(ca / "ca.key"),
        "-out",
        str(ca / "ca.crt"),
        "-addext",
        "basicConstraints=critical,CA:TRUE",
    )
    openssl(
        "req",
        "-newkey",
        "rsa:3072",
        "-nodes",
        "-subj",
        "/CN=" + host,
        "-keyout",
        str(server / "server.key"),
        "-out",
        str(ca / "server.csr"),
    )
    try:
        ipaddress.ip_address(host)
        external = "IP:" + host
    except ValueError:
        external = "DNS:" + host
    extensions = ca / "extensions.cnf"
    extensions.write_text(
        "subjectAltName="
        + external
        + ",DNS:transcriber-rabbitmq\nextendedKeyUsage=serverAuth\n"
    )
    openssl(
        "x509",
        "-req",
        "-in",
        str(ca / "server.csr"),
        "-CA",
        str(ca / "ca.crt"),
        "-CAkey",
        str(ca / "ca.key"),
        "-CAcreateserial",
        "-days",
        "365",
        "-extfile",
        str(extensions),
        "-out",
        str(server / "server.crt"),
    )
    certificate = (ca / "ca.crt").read_text()
    (server / "ca.crt").write_text(certificate)

    users, permissions = [], []
    accounts = [("publisher", "transcriber-publisher", "transcriber-rabbitmq")]
    accounts.extend((worker, "worker-" + worker, host) for worker in workers)
    for directory, username, hostname in accounts:
        password = secrets.token_urlsafe(48)
        users.append(
            {
                "name": username,
                "password_hash": password_hash(password),
                "hashing_algorithm": "rabbit_password_hashing_sha256",
                "tags": [],
            }
        )
        permissions.append(
            {
                "user": username,
                "vhost": "transcriber",
                "configure": r"^transcriber\." if directory == "publisher" else r"^$",
                "write": r"^transcriber\."
                if directory == "publisher"
                else r"^transcriber\.defer$",
                "read": r"^transcriber\."
                if directory == "publisher"
                else r"^transcriber\.jobs$",
            }
        )
        target = output / directory
        target.mkdir(mode=0o700)
        connection = target / "connection.json"
        connection.write_text(
            json.dumps(
                {
                    "host": hostname,
                    "port": port,
                    "virtual_host": "transcriber",
                    "username": username,
                    "password": password,
                    "tls": True,
                }
            )
            + "\n"
        )
        connection.chmod(0o600)
        (target / "ca.crt").write_text(certificate)
    definitions = {
        "users": users,
        "permissions": permissions,
        "vhosts": [{"name": "transcriber"}],
        "policies": [
            {
                "vhost": "transcriber",
                "name": "safe-deferred-delivery",
                "pattern": r"^transcriber\.retry$",
                "apply-to": "quorum_queues",
                "priority": 1,
                "definition": {
                    "dead-letter-strategy": "at-least-once",
                    "overflow": "reject-publish",
                },
            }
        ],
    }
    (server / "definitions.json").write_text(json.dumps(definitions) + "\n")
    (server / "rabbitmq.conf").write_text("""listeners.tcp = none
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/ca.crt
ssl_options.certfile = /etc/rabbitmq/server.crt
ssl_options.keyfile = /etc/rabbitmq/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
definitions.import_backend = local_filesystem
definitions.local.path = /etc/rabbitmq/definitions.json
vm_memory_high_watermark.absolute = 256MiB
disk_free_limit.absolute = 1GB
consumer_timeout = 25200000
log.console.level = warning
""")
    # Only this subdirectory is mounted into RabbitMQ. Its 0700 parent protects
    # the host's files; the broker UID must be able to read its mounted config.
    for file in server.iterdir():
        file.chmod(0o644)
    (ca / "ca.key").chmod(0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--worker-id", action="append", required=True)
    parser.add_argument("--port", type=int, default=5671)
    args = parser.parse_args()
    provision(args.output, args.host, args.worker_id, args.port)
    print("Created private RabbitMQ bundle; credentials were not printed")


if __name__ == "__main__":
    main()
