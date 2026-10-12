"""Private container-local SSH forwarding; credentials stay in mounted files."""

import os
import re


def arguments(environment):
    host = environment.get("SSH_HOST", "")
    user = environment.get("SSH_USER", "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,252}", host):
        raise ValueError("SSH_HOST must be a hostname or IP address")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,63}", user):
        raise ValueError("SSH_USER is invalid")
    try:
        port = int(environment.get("SSH_PORT", "22"))
    except ValueError:
        raise ValueError("SSH_PORT must be 1..65535") from None
    if not 1 <= port <= 65535:
        raise ValueError("SSH_PORT must be 1..65535")
    return [
        "ssh",
        "-N",
        "-T",
        "-F",
        "/dev/null",
        "-p",
        str(port),
        "-l",
        user,
        "-i",
        "/run/credentials/identity",
        "-o",
        "UserKnownHostsFile=/run/credentials/known_hosts",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "LogLevel=ERROR",
        "-L",
        "127.0.0.1:50055:127.0.0.1:50055",
        host,
    ]


if __name__ == "__main__":
    try:
        args = arguments(os.environ)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    os.execvp(args[0], args)
