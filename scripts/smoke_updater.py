"""Real Docker API update/rollback check, isolated from production workers/queue."""

import json
import os
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from worker_updater import OPT_IN, ROLE, Docker, UnixConnection, Updater


def cli(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    project = "transcriber-updater-smoke-" + uuid.uuid4().hex[:8]
    endpoint = cli("context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
    if not endpoint.startswith("unix://"):
        raise ValueError("Smoke currently requires a local Unix Docker context")

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(endpoint.removeprefix("unix://"))

    UnixConnection.connect = connect
    docker = Docker()
    # Smoke images are deliberately local; all other calls use the real API.
    docker.pull = lambda image: docker.call("GET", "/images/" + image + "/json")
    names = [project + ":" + version for version in ("old", "new", "bad")]
    try:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for version, tag in zip(("old", "new", "bad"), names, strict=True):
                (root / "worker.py").write_text(
                    "import signal,time\nfrom pathlib import Path\n"
                    "marker=Path('/tmp/worker-drained')\nmarker.unlink(missing_ok=True)\n"
                    "signal.signal(signal.SIGUSR1,lambda *_: marker.touch())\n"
                    "signal.signal(signal.SIGUSR2,lambda *_: marker.unlink(missing_ok=True))\n"
                    "signal.signal(signal.SIGTERM,lambda *_: exit(0))\n"
                    + ("Path('/tmp/healthy').touch()\n" if version != "bad" else "")
                    + "while True: time.sleep(.1)\n"
                )
                (root / "Dockerfile").write_text(
                    f"FROM python:{os.environ['PYTHON_VERSION']}-slim\n"
                    f'LABEL smoke.version="{version}" io.transcriber.drain-protocol="1"\n'
                    'COPY worker.py /worker.py\nCMD ["python", "/worker.py"]\n'
                )
                subprocess.run(["docker", "build", "-q", "-t", tag, temp], check=True)
            tunnel = docker.call(
                "POST",
                "/containers/create?name=" + project + "-tunnel",
                {
                    "Image": names[0],
                    "Cmd": ["python", "-c", "import time; time.sleep(600)"],
                    "Labels": {"com.docker.compose.project": project},
                    "HostConfig": {"Memory": 64 * 1024 * 1024, "CapDrop": ["ALL"]},
                },
            )
            docker.action(tunnel["Id"], "start")
            created = docker.call(
                "POST",
                "/containers/create?name=" + project,
                {
                    "Image": names[0],
                    "Labels": {
                        "com.docker.compose.project": project,
                        "com.docker.compose.service": "worker-cpu",
                        OPT_IN: "true",
                        ROLE: "cpu",
                    },
                    "Healthcheck": {
                        "Test": [
                            "CMD",
                            "python",
                            "-c",
                            "from pathlib import Path; raise SystemExit(0 if Path('/tmp/healthy').exists() else 1)",
                        ],
                        "Interval": 1_000_000_000,
                        "Timeout": 1_000_000_000,
                        "Retries": 1,
                    },
                    "HostConfig": {
                        "NetworkMode": "container:" + tunnel["Id"],
                        "ReadonlyRootfs": True,
                        "Tmpfs": {"/tmp": "size=16777216,mode=1777"},
                        "Memory": 128 * 1024 * 1024,
                        "CapDrop": ["ALL"],
                        "RestartPolicy": {"Name": "unless-stopped"},
                    },
                },
            )
            docker.action(created["Id"], "start")
            for _ in range(40):
                old = docker.inspect(created["Id"])
                if old["State"].get("Health", {}).get("Status") == "healthy":
                    break
                time.sleep(0.25)
            assert old["State"]["Health"]["Status"] == "healthy"
            updater = Updater(
                docker, project, "synthetic", root / "state", health_timeout=8
            )
            updater.update(old, names[1])
            good = docker.inspect(project)
            assert (
                good["Id"] != old["Id"]
                and good["State"]["Health"]["Status"] == "healthy"
            )
            assert good["Config"]["Labels"]["smoke.version"] == "new"
            updater.health_timeout = 3
            updater.update(good, names[2])
            restored = docker.inspect(project)
            assert restored["Id"] == good["Id"] and restored["State"]["Running"]
            assert updater.state["failed"]["cpu"] == names[2]
            time.sleep(2)
            assert docker.inspect(project)["State"]["Health"]["Status"] == "healthy"
            print(
                json.dumps(
                    {
                        "project": project,
                        "healthy_replacement": True,
                        "unhealthy_rollback": True,
                    }
                )
            )
    finally:
        items = cli(
            "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project
        ).splitlines()
        for ident in items:
            docker.remove(ident)
        for image in names:
            subprocess.run(
                ["docker", "image", "rm", image],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


if __name__ == "__main__":
    main()
