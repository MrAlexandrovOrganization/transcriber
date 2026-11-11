"""Opaque streaming, fast outage and recovery using disposable Docker fixtures."""

import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, timeout=120).strip()


def main():
    prefix = "transcriber-router-test-" + uuid.uuid4().hex[:10]
    network, backend, router = prefix + "-net", prefix + "-backend", prefix + "-router"
    created = []
    temporary = tempfile.TemporaryDirectory(prefix="transcriber-router-compose-")
    try:
        # A private fixture bridge also supports published loopback ports on
        # Docker Desktop, which does not publish ports on internal networks.
        docker("network", "create", network)
        created.append(network)
        code = """import socketserver, threading, time
class Echo(socketserver.BaseRequestHandler):
 def handle(self):
  while True:
   data=self.request.recv(65536)
   if not data: return
   self.request.sendall(data)
for port in (50053,50056,5671):
 server=socketserver.ThreadingTCPServer(('0.0.0.0',port),Echo)
 threading.Thread(target=server.serve_forever,daemon=True).start()
while True: time.sleep(1)
"""
        docker(
            "run",
            "-d",
            "--name",
            backend,
            "--network",
            network,
            "--network-alias",
            "coordinator",
            "--memory",
            "128m",
            "--cpus",
            "0.5",
            "python:" + os.environ["PYTHON_VERSION"] + "-slim",
            "python",
            "-u",
            "-c",
            code,
        )
        created.append(backend)
        ports = {}
        for port in (50053, 50056, 5671, 8404):
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                ports[port] = reservation.getsockname()[1]
        root = Path(temporary.name)
        config = root / "router.cfg"
        config.write_bytes(Path("deploy/router.cfg").read_bytes())
        config.chmod(0o644)
        mounts = [
            {
                "type": "bind",
                "source": str(config),
                "target": "/usr/local/etc/haproxy/haproxy.cfg",
                "read_only": True,
                "bind": {"create_host_path": False, "propagation": "rprivate"},
            }
        ]
        published = [f"127.0.0.1:{value}:{key}" for key, value in ports.items()]
        override = root / "compose.yml"
        override.write_text(
            "services:\n  router:\n    container_name: "
            + router
            + "\n    ports: !override "
            + json.dumps(published)
            + "\n    volumes: !override "
            + json.dumps(mounts)
            + "\n    networks: !override [fixture]\nnetworks:\n  fixture:\n    external: true\n    name: "
            + network
            + "\n"
        )
        env = dict(
            os.environ,
            ROUTER_BIND_ADDRESS="127.0.0.1",
            COORDINATOR_ADDRESS="coordinator",
        )
        subprocess.run(
            [
                "docker",
                "compose",
                "--project-directory",
                str(Path.cwd()),
                "--env-file",
                "/dev/null",
                "-p",
                prefix,
                "-f",
                "compose/router.yml",
                "-f",
                str(override),
                "create",
                "--no-build",
                "router",
            ],
            env=env,
            check=True,
            timeout=90,
        )
        created.append(router)
        docker("start", router)

        def address(port):
            return "127.0.0.1", ports[port]

        def connect(port):
            connection = socket.create_connection(address(port), timeout=2)
            connection.settimeout(8)
            connection.sendall(b"synthetic-stream-without-credentials")
            assert connection.recv(4096) == b"synthetic-stream-without-credentials"
            return connection

        for port in (50053, 50056, 5671):
            deadline = time.monotonic() + 20
            while True:
                try:
                    connection = connect(port)
                    connection.close()
                    break
                except (OSError, AssertionError):
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.2)
        existing = connect(50053)
        try:
            started = time.monotonic()
            docker("stop", "-t", "1", backend)
            try:
                assert existing.recv(1) == b""
            except ConnectionResetError:
                pass
            assert time.monotonic() - started < 8
        finally:
            existing.close()
        started = time.monotonic()
        try:
            unexpected = connect(50053)
        except (OSError, AssertionError):
            pass
        else:
            unexpected.close()
            raise AssertionError("Router accepted an unavailable upstream")
        assert time.monotonic() - started < 4
        host, port = address(8404)
        assert (
            urllib.request.urlopen(f"http://{host}:{port}/health", timeout=3).status
            == 200
        )
        docker("start", backend)
        deadline = time.monotonic() + 20
        while True:
            try:
                connection = connect(50053)
                connection.close()
                break
            except (OSError, AssertionError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        print("Router streaming, bounded outages and automatic recovery verified")
    finally:
        for name in reversed(created[1:]):
            docker("rm", "-f", name)
        if created:
            docker("network", "rm", network)
        temporary.cleanup()


if __name__ == "__main__":
    main()
