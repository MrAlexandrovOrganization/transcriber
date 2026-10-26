"""Real Caddy TLS/HTTP2 and worker RPCs with an isolated synthetic store."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from concurrent import futures
from pathlib import Path
from types import SimpleNamespace

import grpc

from proto import whisper_pb2 as pb
from proto import whisper_pb2_grpc as rpc
from whisper.worker_api import WorkerServicer
from whisper.worker_auth import WorkerCredentials


class SyntheticStore:
    queue_backend = "postgres"
    availability_ttl_seconds = 90
    lease_seconds = 120

    def __init__(self, root):
        self.input = root / "synthetic.wav"
        self.input.write_bytes(b"synthetic-audio-not-production")
        self.owner = ""
        self.result = None

    def report_availability(self, *_):
        return True

    def claim_next_job(self, worker_id, **_):
        if self.owner:
            return None
        self.owner = worker_id + "/" + uuid.uuid4().hex
        self.job = SimpleNamespace(
            job_id="synthetic-job",
            input_path=str(self.input),
            format="wav",
            preset="voice",
            language_override="en",
            initial_prompt_override="",
            traceparent="",
            tracestate="",
            attempts=1,
            lease_owner=self.owner,
        )
        return SimpleNamespace(job=self.job, lease_owner=self.owner)

    def get_owned_job(self, job_id, owner):
        return self.job if job_id == "synthetic-job" and owner == self.owner else None

    def heartbeat(self, job_id, owner, *_):
        return self.get_owned_job(job_id, owner) is not None

    def complete_job(self, *, job_id, worker_id, text, **_):
        if not self.get_owned_job(job_id, worker_id):
            return False
        self.result = text
        return True


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    name = "transcriber-tls-smoke-" + uuid.uuid4().hex[:8]
    token = "synthetic-worker-tls-token-with-at-least-32-characters"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    channel = None
    container = None
    base = Path.home() / ".cache/transcriber-tests"
    base.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(dir=base) as temporary:
            root = Path(temporary)
            store = SyntheticStore(root)
            credentials = WorkerCredentials(
                {"synthetic-cpu": hashlib.sha256(token.encode()).hexdigest()}
            )
            rpc.add_WorkerServiceServicer_to_server(
                WorkerServicer(store, credentials), server
            )
            backend = server.add_insecure_port("0.0.0.0:0")
            server.start()
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=DNS:localhost",
                    "-keyout",
                    str(root / "key.pem"),
                    "-out",
                    str(root / "cert.pem"),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            (root / "key.pem").chmod(0o600)
            (root / "Caddyfile").write_text(
                "{\n admin off\n auto_https disable_redirects\n}\n"
                "https://localhost:8443 {\n tls /test/cert.pem /test/key.pem\n"
                " @worker {\n method POST\n path /whisper.WorkerService/*\n }\n"
                " handle @worker {\n reverse_proxy h2c://host.docker.internal:"
                + str(backend)
                + "\n }\n"
                " handle {\n respond 404\n }\n}\n"
            )
            args = [
                "run",
                "-d",
                "--name",
                name,
                "--label",
                "purpose=" + name,
                "-p",
                "127.0.0.1::8443",
                "-v",
                str(root) + ":/test:ro",
            ]
            if sys.platform != "darwin":
                args += ["--add-host", "host.docker.internal:host-gateway"]
            container = docker(
                *args,
                "caddy:" + os.environ["CADDY_TEST_VERSION"],
                "caddy",
                "run",
                "--config",
                "/test/Caddyfile",
            )
            port = json.loads(
                docker(
                    "inspect", "--format", "{{json .NetworkSettings.Ports}}", container
                )
            )["8443/tcp"][0]["HostPort"]
            target = "localhost:" + port
            channel = grpc.secure_channel(
                target, grpc.ssl_channel_credentials((root / "cert.pem").read_bytes())
            )
            grpc.channel_ready_future(channel).result(timeout=20)
            stub = rpc.WorkerServiceStub(channel)
            metadata = (("authorization", "Bearer " + token),)
            instance = str(uuid.uuid4())
            assert stub.ReportAvailability(
                pb.WorkerAvailabilityRequest(
                    worker_id="synthetic-cpu", instance_id=instance, ready=True
                ),
                metadata=metadata,
                timeout=5,
            ).accepted
            for request, headers, expected in (
                (
                    pb.WorkerClaimRequest(worker_id="synthetic-gpu"),
                    metadata,
                    grpc.StatusCode.PERMISSION_DENIED,
                ),
                (
                    pb.WorkerClaimRequest(worker_id="synthetic-cpu"),
                    (),
                    grpc.StatusCode.UNAUTHENTICATED,
                ),
            ):
                try:
                    stub.Claim(request, metadata=headers, timeout=5)
                    raise AssertionError("Unauthorized worker accepted")
                except grpc.RpcError as error:
                    assert error.code() == expected
            claim = stub.Claim(
                pb.WorkerClaimRequest(worker_id="synthetic-cpu", instance_id=instance),
                metadata=metadata,
                timeout=5,
            )
            request = pb.WorkerJobRequest(
                job_id=claim.job_id, lease_token=claim.lease_token
            )
            assert (
                b"".join(
                    chunk.data
                    for chunk in stub.Download(request, metadata=metadata, timeout=5)
                )
                == store.input.read_bytes()
            )
            assert stub.Heartbeat(
                pb.WorkerHeartbeatRequest(
                    job_id=claim.job_id, lease_token=claim.lease_token
                ),
                metadata=metadata,
                timeout=5,
            ).accepted
            assert stub.Complete(
                pb.WorkerCompleteRequest(
                    job_id=claim.job_id,
                    lease_token=claim.lease_token,
                    text="synthetic-result",
                ),
                metadata=metadata,
                timeout=5,
            ).accepted
            assert store.result == "synthetic-result"
            # The generated test certificate is not trusted by the system CA bundle.
            with grpc.secure_channel(
                target, grpc.ssl_channel_credentials()
            ) as untrusted:
                try:
                    rpc.WorkerServiceStub(untrusted).Claim(
                        pb.WorkerClaimRequest(worker_id="synthetic-cpu"),
                        metadata=metadata,
                        timeout=2,
                    )
                    raise AssertionError("Untrusted TLS certificate accepted")
                except grpc.RpcError as error:
                    assert error.code() == grpc.StatusCode.UNAVAILABLE
            print(
                json.dumps(
                    {
                        "tls_http2": True,
                        "scoped_auth": True,
                        "download_heartbeat_complete": True,
                        "untrusted_certificate_rejected": True,
                    }
                )
            )
    except Exception:
        if container:
            print(docker("logs", "--tail", "12", container), file=sys.stderr)
        raise
    finally:
        if channel:
            channel.close()
        server.stop(0).wait()
        if container:
            docker("rm", "-f", container)


if __name__ == "__main__":
    main()
