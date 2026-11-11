"""Opaque streaming, fast outage and recovery using disposable Docker fixtures."""

import io
import os
import socket
import subprocess
import tarfile
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
        args = [
            "create",
            "--name",
            router,
            "--network",
            network,
            "--memory",
            "64m",
            "--cpus",
            "0.5",
            "-e",
            "COORDINATOR_ADDRESS=coordinator",
        ]
        ports = {}
        for port in (50053, 50056, 5671, 8404):
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                ports[port] = reservation.getsockname()[1]
            args += ["-p", f"127.0.0.1:{ports[port]}:{port}"]
        docker(*args, "haproxy:" + os.environ["HAPROXY_VERSION"])
        created.append(router)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            content = Path("deploy/router.cfg").read_bytes()
            member = tarfile.TarInfo("haproxy.cfg")
            member.mode = 0o644
            member.size = len(content)
            tar.addfile(member, io.BytesIO(content))
        subprocess.run(
            ["docker", "cp", "-", router + ":/usr/local/etc/haproxy/"],
            input=archive.getvalue(),
            check=True,
            timeout=30,
        )
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


if __name__ == "__main__":
    main()
