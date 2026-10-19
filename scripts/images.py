"""Build, publish and activate the transcriber server image by digest."""

import base64
import fcntl
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".images"
PLATFORM = "linux/amd64"
SERVICES = {"whisper-ingress", "whisper-worker"}


def run(*args, capture=False):
    result = subprocess.run(
        args,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    return result.stdout


def compose(runtime=False):
    command = ["docker", "compose", "--project-directory", str(ROOT)]
    if runtime:
        env_file = os.environ.get("IMAGE_RUNTIME_ENV_FILE", "")
        if env_file and (ROOT / env_file).is_file():
            command += ["--env-file", env_file]
    for compose_file in os.environ["IMAGE_COMPOSE_FILES"].split():
        command += ["-f", compose_file]
    return command


def model():
    # Read only build definitions; do not resolve runtime env_file values/secrets.
    return json.loads(
        run(
            *compose(),
            "--profile",
            "*",
            "--env-file",
            "/dev/null",
            "config",
            "--no-interpolate",
            "--no-env-resolution",
            "--no-path-resolution",
            "--format",
            "json",
            capture=True,
        )
    )


def expand(value):
    def replace(match):
        key, operator, default = match.groups()
        resolved = os.environ.get(key, "")
        if not resolved and operator == ":?":
            raise ValueError(f"Missing build variable: {key}; run through Make")
        return resolved or (default if operator == ":-" else "")

    return re.sub(r"\$\{([A-Z_][A-Z_0-9]*)(:\?|:-)?([^}]*)\}", replace, str(value))


def specs():
    services = model()["services"]
    if not SERVICES <= services.keys():
        raise ValueError("Server Compose must define ingress and fallback worker")
    result = {name: services[name]["build"] for name in sorted(SERVICES)}
    if any(not spec for spec in result.values()):
        raise ValueError("Both server services must have build definitions")
    if len({json.dumps(spec, sort_keys=True) for spec in result.values()}) != 1:
        raise ValueError("Ingress and fallback worker must share one runtime image")
    return result


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w") as file:
            file.write(json.dumps(value, indent=2) + "\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def revision():
    return run("git", "rev-parse", "HEAD", capture=True).strip()


def repository():
    value = os.environ.get(
        "IMAGE_REPOSITORY", "ghcr.io/mralexandrovorganization/transcriber"
    ).lower()
    if not re.fullmatch(r"ghcr\.io/[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*", value):
        raise ValueError("IMAGE_REPOSITORY must be one GHCR repository")
    return value


def build():
    repo = repository()
    sha = revision()
    if os.environ.get("SOURCE_REVISION", sha) != sha:
        raise ValueError("SOURCE_REVISION differs from checkout")
    tag = "{}-{}-{}".format(
        sha,
        os.environ.get("GITHUB_RUN_ID", "local"),
        os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
    )
    spec = next(iter(specs().values()))
    image = f"{repo}:{tag}"
    context = ROOT / expand(spec["context"])
    command = [
        "docker",
        "build",
        "--platform",
        PLATFORM,
        "--tag",
        image,
        "--label",
        f"org.opencontainers.image.revision={sha}",
    ]
    source = os.environ.get("GITHUB_REPOSITORY")
    if source:
        command += [
            "--label",
            f"org.opencontainers.image.source=https://github.com/{source}",
        ]
    command += ["--file", str(context / spec.get("dockerfile", "Dockerfile"))]
    if spec.get("target"):
        command += ["--target", spec["target"]]
    for name, value in spec.get("args", {}).items():
        command += [
            "--build-arg",
            f"{name}={expand(value) if value is not None else os.environ[name]}",
        ]
    run(*command, str(context))
    write(
        STATE / "build.json",
        {
            "revision": sha,
            "platform": PLATFORM,
            "images": {s: image for s in sorted(SERVICES)},
        },
    )


def publish():
    manifest = json.loads((STATE / "build.json").read_text())
    if manifest["revision"] != revision():
        raise ValueError("Build revision differs from checkout")
    tags = set(manifest["images"].values())
    if len(tags) != 1 or not next(iter(tags)).startswith(repository() + ":"):
        raise ValueError("Published image does not match IMAGE_REPOSITORY")
    resolved = {}
    for tag in tags:
        run("docker", "push", tag)
        repo = tag.rsplit(":", 1)[0]
        digests = json.loads(
            run(
                "docker",
                "image",
                "inspect",
                "--format",
                "{{json .RepoDigests}}",
                tag,
                capture=True,
            )
        )
        resolved[tag] = next(
            ref for ref in digests if ref.startswith(repo + "@sha256:")
        )
    manifest["images"] = {
        name: resolved[tag] for name, tag in manifest["images"].items()
    }
    write(STATE / "release.json", manifest)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        encoded = base64.b64encode(json.dumps(manifest).encode()).decode()
        with open(output, "a") as file:
            file.write(f"manifest={encoded}\n")


def validate(manifest):
    if manifest.get("revision") != revision() or manifest.get("platform") != PLATFORM:
        raise ValueError("Release revision/platform differs from checkout")
    images = manifest.get("images", {})
    if set(images) != SERVICES or len(set(images.values())) != 1:
        raise ValueError("Release must map both server services to one image")
    digest_ref = re.escape(repository()) + r"@sha256:[0-9a-f]{64}"
    if not all(re.fullmatch(digest_ref, ref) for ref in images.values()):
        raise ValueError("Release must contain an immutable GHCR digest")


def deploy():
    if os.environ.get("IMAGE_MANIFEST_B64"):
        manifest = json.loads(
            base64.b64decode(os.environ["IMAGE_MANIFEST_B64"], validate=True)
        )
    else:
        path = os.environ.get("IMAGE_MANIFEST", str(STATE / "release.json"))
        manifest = json.loads(Path(path).read_text())
    validate(manifest)
    STATE.mkdir(exist_ok=True)
    current = STATE / "current.json"
    previous = json.loads(current.read_text()) if current.exists() else None
    write(STATE / "requested.json", manifest)

    # Pull the app digest before Compose can stop or recreate any service.
    ref = next(iter(manifest["images"].values()))
    run("docker", "pull", "--platform", PLATFORM, ref)
    override = {
        "services": {
            name: {"image": image, "platform": PLATFORM}
            for name, image in manifest["images"].items()
        }
    }
    with tempfile.TemporaryDirectory(dir=STATE) as directory:
        candidate = Path(directory) / "compose.json"
        write(candidate, override)
        command = compose(runtime=True) + ["-f", str(candidate)]
        run("make", "image-activate", "IMAGE_COMPOSE=" + shlex.join(command))

    if previous is not None:
        write(STATE / "previous.json", previous)
    write(current, manifest)
    write(STATE / "compose.json", override)
    print(f"Activated backend image for {manifest['revision']}")


def locked_deploy():
    if os.environ.get("IMAGE_DEPLOY_LOCKED") == "1":
        return deploy()
    lockfile = Path(os.path.expandvars(os.environ["IMAGE_DEPLOY_LOCK"]))
    with lockfile.open("a") as lock:
        deadline = time.monotonic() + 1800
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError("Timed out waiting for deployment lock") from None
                time.sleep(1)
        deploy()


def operate():
    manifest = json.loads((STATE / "current.json").read_text())
    validate(manifest)
    args = shlex.split(os.environ.get("IMAGE_ARGS", "ps"))
    if not args or args[0] not in {"ps", "logs", "exec", "run", "stop", "restart"}:
        raise ValueError("image-compose supports ps/logs/exec/run/stop/restart")
    if args[0] == "run":
        args[1:1] = ["--no-build", "--pull", "never"]
    run(*compose(runtime=True), "-f", str(STATE / "compose.json"), *args)


if __name__ == "__main__":
    try:
        {
            "build": build,
            "publish": publish,
            "deploy": locked_deploy,
            "compose": operate,
        }[sys.argv[1]]()
    except (
        ValueError,
        KeyError,
        OSError,
        subprocess.CalledProcessError,
        StopIteration,
    ):
        print(
            "Image operation failed; inspect the preceding build/pull/check result.",
            file=sys.stderr,
        )
        sys.exit(1)
