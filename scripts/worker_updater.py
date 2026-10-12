"""Small Docker API reconciler for explicitly opted-in remote workers only."""

import http.client
import json
import logging
import os
import signal
import socket
import threading
import time
from pathlib import Path
from urllib.parse import quote, urlencode

try:
    from scripts.worker_release import atomic_json, release
except ModuleNotFoundError:
    from worker_release import atomic_json, release

LOG = logging.getLogger("worker-updater")
OPT_IN = "io.transcriber.autodeploy"
ROLE = "io.transcriber.worker-role"
STOP = threading.Event()


class APIError(RuntimeError):
    pass


class UnixConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


class Docker:
    def call(self, method, path, data=None, *, stream=False, absent=False):
        connection = UnixConnection("localhost", timeout=1800 if stream else 30)
        try:
            body = json.dumps(data).encode() if data is not None else None
            connection.request(
                method, "/v1.45" + path, body, {"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            if absent and response.status == 404:
                return None
            if response.status >= 300:
                # Docker errors may include secret-bearing container configuration.
                operation = path.split("?", 1)[0].rsplit("/", 1)[-1]
                if operation not in (
                    "create",
                    "start",
                    "stop",
                    "rename",
                    "kill",
                    "exec",
                    "json",
                ):
                    operation = "request"
                raise APIError(
                    f"Docker {method} {operation} failed: HTTP {response.status}"
                )
            if stream:
                while line := response.readline(1024 * 1024):
                    if len(line) >= 1024 * 1024:
                        raise APIError("Oversized Docker stream response")
                    if json.loads(line).get("error"):
                        raise APIError("Docker image pull failed")
                return None
            body = response.read(8 * 1024 * 1024 + 1)
            if len(body) > 8 * 1024 * 1024:
                raise APIError("Oversized Docker response")
            return json.loads(body) if body else None
        finally:
            connection.close()

    def inspect(self, name):
        return self.call(
            "GET", "/containers/" + quote(name, safe="") + "/json", absent=True
        )

    def pull(self, image):
        self.call(
            "POST",
            "/images/create?"
            + urlencode(
                {"fromImage": image, "platform": "linux/amd64"}
                if image.endswith("-release:stable")
                else {"fromImage": image}
            ),
            stream=True,
        )
        return self.call("GET", "/images/" + quote(image, safe="") + "/json")

    def action(self, ident, action, **params):
        self.call(
            "POST",
            "/containers/"
            + quote(ident, safe="")
            + "/"
            + action
            + ("?" + urlencode(params) if params else ""),
        )

    def remove(self, name):
        self.call("DELETE", "/containers/" + quote(name, safe="") + "?force=true")

    def drained(self, name):
        command = self.call(
            "POST",
            "/containers/" + name + "/exec",
            {
                "Cmd": [
                    "python",
                    "-c",
                    "from pathlib import Path; raise SystemExit(0 if Path('/tmp/worker-drained').is_file() else 1)",
                ],
                "AttachStdout": False,
                "AttachStderr": False,
            },
        )["Id"]
        self.call("POST", "/exec/" + command + "/start", {"Detach": True})
        for _ in range(20):
            result = self.call("GET", "/exec/" + command + "/json")
            if not result["Running"]:
                return result["ExitCode"] == 0
            time.sleep(0.1)
        return False


def replacement(old, image, image_id, old_defaults=None, new_defaults=None):
    # Inspect.Config is also the create API's container Config. HostConfig contains
    # the daemon's translated Desktop/WSL mounts, GPU requests and resource limits.
    config = dict(old["Config"])
    config["Image"] = image
    old_defaults, new_defaults = old_defaults or {}, new_defaults or {}
    image_labels = old_defaults.get("Labels") or {}
    config["Labels"] = {
        **(new_defaults.get("Labels") or {}),
        **{
            key: value
            for key, value in (config.get("Labels") or {}).items()
            if key not in image_labels or value != image_labels[key]
        },
    }
    # Preserve machine-specific env overrides, let the new image supply its own
    # PATH/CUDA/Python defaults. This also avoids retaining stale OCI labels.
    inherited_env = set(old_defaults.get("Env") or [])
    config["Env"] = [
        value for value in config.get("Env") or [] if value not in inherited_env
    ]
    for key in ("Cmd", "Entrypoint", "WorkingDir"):
        if key in old_defaults and config.get(key) == old_defaults[key]:
            config.pop(key, None)
    config["Labels"]["com.docker.compose.image"] = image_id
    host = dict(old["HostConfig"])
    host.pop("ConsoleSize", None)
    if host.get("NetworkMode", "").startswith("container:"):
        # Inspect fills these even when Compose did not explicitly set them.
        # Docker rejects explicit hostname/domain options with a shared namespace.
        config.pop("Hostname", None)
        config.pop("Domainname", None)
    config["HostConfig"] = host
    return config


class Updater:
    def __init__(
        self,
        docker,
        project,
        registry,
        state_dir,
        *,
        drain_timeout=3600,
        health_timeout=240,
    ):
        if not project or not registry:
            raise ValueError("Updater requires project and registry")
        self.docker, self.project, self.registry = docker, project, registry
        self.path = Path(state_dir) / "state.json"
        self.state = (
            json.loads(self.path.read_text()) if self.path.exists() else {"failed": {}}
        )
        self.drain_timeout, self.health_timeout = drain_timeout, health_timeout

    def save(self):
        atomic_json(self.path, self.state)

    def managed(self, container):
        labels = container["Config"].get("Labels") or {}
        role = labels.get(ROLE)
        return (
            labels.get("com.docker.compose.project") == self.project
            and role in ("cpu", "gpu")
            and labels.get("com.docker.compose.service") == "worker-" + role
            and labels.get(OPT_IN) == "true"
        )

    def recover(self):
        pending = self.state.get("pending")
        if not pending:
            return
        old = self.docker.inspect(pending["old"])
        new = self.docker.inspect(pending["name"])
        if new and new["Id"] == pending["old"]:
            # Crash before rename: leave old container running and cancel its drain.
            if not self.managed(new):
                raise APIError("Recovery refused an unmanaged container")
            if not new["State"]["Running"]:
                self.docker.action(new["Id"], "start")
            elif new["State"].get("StartedAt") == pending.get("started"):
                self.docker.action(new["Id"], "kill", signal="SIGUSR2")
        elif (
            new
            and self.managed(new)
            and new["State"].get("Health", {}).get("Status") == "healthy"
        ):
            if old:
                if not self.managed(old):
                    raise APIError("Recovery refused an unmanaged backup")
                self.docker.remove(old["Id"])
            LOG.info("Worker update committed: service=%s", pending["service"])
        else:
            if not old or not self.managed(old):
                raise APIError("Worker rollback backup unavailable")
            if new:
                if not self.managed(new):
                    raise APIError("Recovery refused an unmanaged replacement")
                self.docker.remove(new["Id"])
            self.docker.action(old["Id"], "rename", name=pending["name"])
            self.docker.action(old["Id"], "start")
            self.state["failed"][pending["service"]] = pending["image"]
            LOG.warning("Worker update rolled back: service=%s", pending["service"])
        del self.state["pending"]
        self.save()

    def update(self, old, image):
        role = old["Config"]["Labels"][ROLE]
        if self.state["failed"].get(role) == image:
            return
        target = self.docker.pull(image)
        if old["Image"] == target["Id"]:
            return
        if old["Config"]["Labels"].get("io.transcriber.drain-protocol") != "1":
            LOG.warning(
                "Worker needs initial make up before autodeploy: service=%s", role
            )
            return
        if old["State"].get("Health", {}).get("Status") != "healthy":
            LOG.warning(
                "Waiting for worker readiness before draining: service=%s", role
            )
            return
        name, ident = old["Name"].lstrip("/"), old["Id"]
        LOG.info("Waiting for current task before update: service=%s", role)
        self.state["pending"] = {
            "old": ident,
            "started": old["State"].get("StartedAt"),
            "name": name,
            "service": role,
            "image": image,
        }
        self.save()
        try:
            self.docker.action(ident, "kill", signal="SIGUSR1")
            deadline = time.monotonic() + self.drain_timeout
            while not self.docker.drained(ident):
                if STOP.wait(2) or time.monotonic() >= deadline:
                    LOG.warning("Worker drain deferred: service=%s", role)
                    self.recover()
                    return
            # Write a durable recovery record before the first destructive action.
            self.state["pending"] = {
                "old": ident,
                "started": old["State"].get("StartedAt"),
                "name": name,
                "service": role,
                "image": image,
            }
            self.save()
            self.docker.action(ident, "stop", t=30)
            self.docker.action(ident, "rename", name=name + "-rollback-" + ident[:12])
            created = self.docker.call(
                "POST",
                "/containers/create?" + urlencode({"name": name}),
                replacement(
                    old,
                    image,
                    target["Id"],
                    self.docker.call("GET", "/images/" + old["Image"] + "/json")[
                        "Config"
                    ],
                    target.get("Config") or {},
                ),
            )
            self.docker.action(created["Id"], "start")
            deadline = time.monotonic() + self.health_timeout
            while not STOP.wait(2):
                new = self.docker.inspect(created["Id"])
                if new and new["State"].get("Health", {}).get("Status") == "healthy":
                    break
                if (
                    not new
                    or not new["State"]["Running"]
                    or time.monotonic() >= deadline
                ):
                    break
            self.recover()
        except Exception:
            # Recover immediately on a normal API failure; the durable journal
            # also handles process/power loss before this handler can run.
            self.recover()
            raise

    def reconcile(self):
        self.recover()
        marker = self.docker.pull(self.registry + "-release:stable")
        revision, images = release(marker["Config"].get("Labels") or {}, self.registry)
        filters = {
            "label": ["com.docker.compose.project=" + self.project, OPT_IN + "=true"]
        }
        containers = self.docker.call(
            "GET", "/containers/json?" + urlencode({"filters": json.dumps(filters)})
        )
        for item in containers:
            old = self.docker.inspect(item["Id"])
            if old and old["State"]["Running"] and self.managed(old):
                self.update(old, images[old["Config"]["Labels"][ROLE]])
        LOG.info("Approved release checked: revision=%s", revision)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "time": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)
                ),
                "level": record.levelname,
                "msg": record.getMessage(),
                "service": "worker-updater",
            }
        )


def main():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: STOP.set())
    updater = Updater(
        Docker(), os.environ["WORKER_PROJECT"], os.environ["WORKER_REGISTRY"], "/state"
    )

    def heartbeat():
        while not STOP.wait(15):
            Path("/tmp/updater-alive").touch()

    Path("/tmp/updater-alive").touch()
    threading.Thread(target=heartbeat, daemon=True, name="updater-heartbeat").start()
    while not STOP.is_set():
        try:
            updater.reconcile()
        except Exception as error:
            # No exception message: Docker/RPC errors may contain sensitive values.
            LOG.warning(
                "Release reconciliation deferred: error_type=%s reason=%s",
                type(error).__name__,
                str(error) if isinstance(error, APIError) else "unavailable",
            )
        Path("/tmp/updater-alive").touch()
        STOP.wait(60)


if __name__ == "__main__":
    main()
