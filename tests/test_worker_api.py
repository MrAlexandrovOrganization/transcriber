"""Real gRPC + PostgreSQL tests, always in a random isolated schema."""

import os
import tempfile
import threading
import time
import unittest
import uuid
from concurrent import futures
from pathlib import Path
from unittest.mock import patch

import grpc
import psycopg
from psycopg.conninfo import make_conninfo
from test_worker_auth import CPU_TOKEN, GPU_TOKEN, registry

from proto import whisper_pb2 as pb
from proto import whisper_pb2_grpc as rpc
from whisper.inference import TranscriptionResult
from whisper.remote_worker import Config, RemoteWorker
from whisper.server import TranscriptionServicer, Worker
from whisper.storage import JobStore
from whisper.worker_api import WorkerServicer
from whisper.worker_auth import WorkerCredentials

TOKEN = "synthetic-test-token-with-32-characters"
DSN = os.getenv("TRANSCRIBER_TEST_DATABASE_URL")


@unittest.skipUnless(DSN, "Set TRANSCRIBER_TEST_DATABASE_URL to an isolated PostgreSQL")
class WorkerAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = None
        if os.getenv("TRANSCRIBER_PILOT_AUDIO_FILE"):
            from whisper.inference import FasterWhisperEngine

            # Load native libraries before gRPC creates background threads.
            cls.engine = FasterWhisperEngine()

    def setUp(self):
        self.schema = "pilot_" + uuid.uuid4().hex
        self.tmp = tempfile.TemporaryDirectory()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f"CREATE SCHEMA {self.schema}")
        self.env = patch.dict(
            os.environ,
            {
                "DATABASE_URL": make_conninfo(
                    DSN, options=f"-csearch_path={self.schema}"
                ),
                "JOB_STORAGE_DIR": self.tmp.name,
                "JOB_LEASE_SECONDS": "3",
                "JOB_MAX_ATTEMPTS": "2",
                "PREFERRED_WORKER_IDS": "mac-primary,wsl-gpu,wsl-cpu",
                "WORKER_AVAILABILITY_TTL_S": "90",
            },
        )
        self.env.start()
        self.store = JobStore()
        self.store.init_schema()
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        rpc.add_WorkerServiceServicer_to_server(
            WorkerServicer(self.store, TOKEN), self.server
        )
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.port}")
        self.stub = rpc.WorkerServiceStub(self.channel)
        self.metadata = (("authorization", "Bearer " + TOKEN),)
        self.client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        rpc.add_TranscriptionServiceServicer_to_server(
            TranscriptionServicer(self.store), self.client_server
        )
        client_port = self.client_server.add_insecure_port("127.0.0.1:0")
        self.client_server.start()
        self.client_channel = grpc.insecure_channel(f"127.0.0.1:{client_port}")
        self.client_stub = rpc.TranscriptionServiceStub(self.client_channel)

    def tearDown(self):
        self.client_channel.close()
        self.client_server.stop(0).wait()
        self.channel.close()
        self.server.stop(0).wait()
        self.env.stop()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f"DROP SCHEMA {self.schema} CASCADE")
        self.tmp.cleanup()

    def rpc(self, method, request):
        return method(request, timeout=5, metadata=self.metadata)

    def submit(self, audio=b"synthetic audio"):
        job, path = self.store.create_job(
            fmt="wav",
            preset="voice",
            language_override="en",
            initial_prompt_override="",
        )
        path.write_bytes(audio)
        self.store.mark_queued(job.job_id)
        return job.job_id

    def claim(self):
        return self.rpc(
            self.stub.Claim, pb.WorkerClaimRequest(worker_id="pilot-worker")
        )

    def expire(self, job_id):
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE job_id = %s",
                (job_id,),
            )

    def report_availability(self, worker_id, instance_id, ready=True):
        return self.rpc(
            self.stub.ReportAvailability,
            pb.WorkerAvailabilityRequest(
                worker_id=worker_id, instance_id=instance_id, ready=ready
            ),
        )

    def claim_primary(self, worker_id, instance_id):
        return self.rpc(
            self.stub.Claim,
            pb.WorkerClaimRequest(worker_id=worker_id, instance_id=instance_id),
        )

    def expire_availability(self, worker_id, instance_id):
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE worker_availability SET ready_until = NOW() - INTERVAL '1 second' "
                "WHERE worker_id = %s AND instance_id = %s",
                (worker_id, instance_id),
            )

    def test_primary_offline_allows_fallback_without_consuming_attempts(self):
        job_id = self.submit()
        instance_id = str(uuid.uuid4())
        self.assertFalse(self.claim_primary("mac-primary", instance_id).job_id)
        self.assertEqual(self.store.get_job(job_id).attempts, 0)
        claimed = self.store.claim_next_job("server-fallback", fallback=True)
        self.assertEqual(claimed.job.job_id, job_id)
        self.assertEqual(claimed.job.attempts, 1)

    def test_live_primary_blocks_fallback_while_idle_and_busy(self):
        first = self.submit()
        second = self.submit()
        instance_id = str(uuid.uuid4())
        report = self.report_availability("mac-primary", instance_id)
        self.assertTrue(report.accepted)
        self.assertEqual(report.ttl_seconds, self.store.availability_ttl_seconds)
        self.assertEqual(report.ttl_seconds, 90)
        self.assertIsNone(self.store.claim_next_job("server-fallback", fallback=True))
        self.assertFalse(self.claim_primary("mac-primary", "").job_id)
        self.assertFalse(self.claim_primary("mac-primary", str(uuid.uuid4())).job_id)
        claimed = self.claim_primary("mac-primary", instance_id)
        self.assertEqual(claimed.job_id, first)
        # A primary that is already processing still reserves queued work.
        self.assertIsNone(self.store.claim_next_job("server-fallback", fallback=True))
        self.assertEqual(self.store.get_job(second).status, "queued")
        self.assertEqual(self.store.get_job(second).attempts, 0)

    def test_primary_down_releases_fallback_without_waiting_for_ttl(self):
        job_id = self.submit()
        instance_id = str(uuid.uuid4())
        self.report_availability("mac-primary", instance_id)
        self.assertIsNone(self.store.claim_next_job("server-fallback", fallback=True))
        self.assertTrue(
            self.report_availability("mac-primary", instance_id, ready=False).accepted
        )
        self.assertFalse(self.claim_primary("mac-primary", instance_id).job_id)
        self.assertEqual(
            self.store.claim_next_job("server-fallback", fallback=True).job.job_id,
            job_id,
        )

    def test_expired_primary_releases_fallback_and_requires_new_report(self):
        job_id = self.submit()
        instance_id = str(uuid.uuid4())
        self.report_availability("mac-primary", instance_id)
        self.expire_availability("mac-primary", instance_id)
        self.assertFalse(self.claim_primary("mac-primary", instance_id).job_id)
        self.assertEqual(self.store.get_job(job_id).attempts, 0)
        self.assertEqual(
            self.store.claim_next_job("server-fallback", fallback=True).job.job_id,
            job_id,
        )

    def test_any_primary_instance_blocks_fallback_and_down_is_instance_scoped(self):
        job_id = self.submit()
        old_mac = str(uuid.uuid4())
        new_mac = str(uuid.uuid4())
        gpu = str(uuid.uuid4())
        cpu = str(uuid.uuid4())
        for worker_id, instance in (
            ("mac-primary", old_mac),
            ("mac-primary", new_mac),
            ("wsl-gpu", gpu),
            ("wsl-cpu", cpu),
        ):
            self.report_availability(worker_id, instance)
        self.report_availability("mac-primary", old_mac, ready=False)
        self.expire_availability("wsl-gpu", gpu)
        self.report_availability("wsl-cpu", cpu, ready=False)
        self.assertFalse(self.claim_primary("mac-primary", old_mac).job_id)
        self.assertIsNone(self.store.claim_next_job("server-fallback", fallback=True))
        self.assertEqual(self.claim_primary("mac-primary", new_mac).job_id, job_id)

    def test_unlisted_availability_does_not_suppress_fallback(self):
        job_id = self.submit()
        self.assertTrue(
            self.report_availability("unlisted-worker", str(uuid.uuid4())).accepted
        )
        self.assertEqual(
            self.store.claim_next_job("server-fallback", fallback=True).job.job_id,
            job_id,
        )

    def test_availability_renewal_survives_store_restart(self):
        job_id = self.submit()
        instance_id = str(uuid.uuid4())
        self.report_availability("mac-primary", instance_id)
        self.expire_availability("mac-primary", instance_id)
        self.assertTrue(self.report_availability("mac-primary", instance_id).accepted)
        restarted_store = JobStore()
        restarted_store.init_schema()
        self.assertIsNone(
            restarted_store.claim_next_job("server-fallback", fallback=True)
        )
        self.assertEqual(self.claim_primary("mac-primary", instance_id).job_id, job_id)

    def test_availability_rpc_rejects_unauthenticated_and_invalid_identity(self):
        valid = pb.WorkerAvailabilityRequest(
            worker_id="mac-primary", instance_id=str(uuid.uuid4()), ready=True
        )
        with self.assertRaises(grpc.RpcError) as error:
            self.stub.ReportAvailability(valid, timeout=5)
        self.assertEqual(error.exception.code(), grpc.StatusCode.UNAUTHENTICATED)
        for worker_id, instance_id in (
            ("", valid.instance_id),
            ("bad worker", valid.instance_id),
            ("a" * 129, valid.instance_id),
            ("mac-primary", ""),
            ("mac-primary", "not-a-uuid"),
        ):
            with self.subTest(worker_id=worker_id, instance_id=instance_id):
                with self.assertRaises(grpc.RpcError) as error:
                    self.report_availability(worker_id, instance_id)
                self.assertEqual(
                    error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT
                )
        with self.assertRaises(grpc.RpcError) as error:
            self.claim_primary("mac-primary", "not-a-uuid")
        self.assertEqual(error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        with self.store.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM worker_availability"
                ).fetchone()["n"],
                0,
            )

    def test_concurrent_primary_claims_win_over_fallback_and_keep_fencing(self):
        jobs = {self.submit(), self.submit(), self.submit()}
        mac = str(uuid.uuid4())
        gpu = str(uuid.uuid4())
        self.report_availability("mac-primary", mac)
        self.report_availability("wsl-gpu", gpu)
        barrier = threading.Barrier(3)

        def claim(which):
            barrier.wait(timeout=5)
            if which == "fallback":
                return self.store.claim_next_job("server-fallback", fallback=True)
            worker_id, instance = which
            return self.claim_primary(worker_id, instance)

        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            results = list(
                executor.map(
                    claim, [("mac-primary", mac), ("wsl-gpu", gpu), "fallback"]
                )
            )
        self.assertIsNone(results[2])
        claims = results[:2]
        self.assertEqual(len({c.job_id for c in claims}), 2)
        self.assertTrue(all(c.job_id in jobs for c in claims))
        self.assertNotEqual(claims[0].lease_token, claims[1].lease_token)
        remaining = jobs - {c.job_id for c in claims}
        self.assertEqual(self.store.get_job(remaining.pop()).attempts, 0)
        self.assertFalse(
            self.rpc(
                self.stub.Complete,
                pb.WorkerCompleteRequest(
                    job_id=claims[0].job_id,
                    lease_token=claims[1].lease_token,
                    text="wrong owner",
                ),
            ).accepted
        )

    def test_fallback_reclaim_after_sleep_fences_stale_primary_result(self):
        job_id = self.submit()
        instance_id = str(uuid.uuid4())
        self.report_availability("mac-primary", instance_id)
        original = self.claim_primary("mac-primary", instance_id)
        self.expire_availability("mac-primary", instance_id)
        self.expire(job_id)
        replacement = self.store.claim_next_job("server-fallback", fallback=True)
        self.assertEqual(replacement.job.job_id, job_id)
        self.assertNotEqual(original.lease_token, replacement.lease_owner)
        self.assertFalse(
            self.rpc(
                self.stub.Complete,
                pb.WorkerCompleteRequest(
                    job_id=job_id,
                    lease_token=original.lease_token,
                    text="stale primary",
                ),
            ).accepted
        )
        self.assertTrue(
            self.store.complete_job(
                job_id=job_id,
                worker_id=replacement.lease_owner,
                text="fallback result",
                segments=[],
                result_path="",
            )
        )
        self.assertEqual(self.store.get_job(job_id).text, "fallback result")

    def test_no_workers_then_download_complete_and_duplicate_rejection(self):
        job_id = self.submit()
        self.assertEqual(self.store.get_job(job_id).status, "queued")
        claim = self.claim()
        audio = b"".join(
            x.data
            for x in self.stub.Download(
                pb.WorkerJobRequest(job_id=job_id, lease_token=claim.lease_token),
                timeout=5,
                metadata=self.metadata,
            )
        )
        self.assertEqual(audio, b"synthetic audio")
        request = pb.WorkerCompleteRequest(
            job_id=job_id, lease_token=claim.lease_token, text="synthetic transcript"
        )
        self.assertTrue(self.rpc(self.stub.Complete, request).accepted)
        self.assertFalse(self.rpc(self.stub.Complete, request).accepted)
        self.assertEqual(self.store.get_job(job_id).text, "synthetic transcript")

    def test_scoped_tokens_fence_worker_identity_and_all_lease_operations(self):
        credentials = WorkerCredentials(registry()["workers"])
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        rpc.add_WorkerServiceServicer_to_server(
            WorkerServicer(self.store, credentials), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = rpc.WorkerServiceStub(channel)
        cpu_meta = (("authorization", "Bearer " + CPU_TOKEN),)
        gpu_meta = (("authorization", "Bearer " + GPU_TOKEN),)
        try:
            self.submit()
            instance = str(uuid.uuid4())
            for method, request in (
                (
                    stub.ReportAvailability,
                    pb.WorkerAvailabilityRequest(
                        worker_id="wsl-gpu", instance_id=instance, ready=True
                    ),
                ),
                (
                    stub.Claim,
                    pb.WorkerClaimRequest(worker_id="wsl-gpu", instance_id=instance),
                ),
            ):
                with self.assertRaises(grpc.RpcError) as error:
                    method(request, timeout=5, metadata=cpu_meta)
                self.assertEqual(
                    error.exception.code(), grpc.StatusCode.PERMISSION_DENIED
                )
            stub.ReportAvailability(
                pb.WorkerAvailabilityRequest(
                    worker_id="wsl-cpu", instance_id=instance, ready=True
                ),
                timeout=5,
                metadata=cpu_meta,
            )
            claimed = stub.Claim(
                pb.WorkerClaimRequest(worker_id="wsl-cpu", instance_id=instance),
                timeout=5,
                metadata=cpu_meta,
            )
            self.assertTrue(claimed.job_id)
            for method, request in (
                (
                    stub.Download,
                    pb.WorkerJobRequest(
                        job_id=claimed.job_id, lease_token=claimed.lease_token
                    ),
                ),
                (
                    stub.Heartbeat,
                    pb.WorkerHeartbeatRequest(
                        job_id=claimed.job_id, lease_token=claimed.lease_token
                    ),
                ),
                (
                    stub.Complete,
                    pb.WorkerCompleteRequest(
                        job_id=claimed.job_id,
                        lease_token=claimed.lease_token,
                        text="wrong worker",
                    ),
                ),
                (
                    stub.Fail,
                    pb.WorkerFailRequest(
                        job_id=claimed.job_id,
                        lease_token=claimed.lease_token,
                        error_code="inference_failed",
                    ),
                ),
            ):
                with self.assertRaises(grpc.RpcError) as error:
                    response = method(request, timeout=5, metadata=gpu_meta)
                    if method == stub.Download:
                        list(response)
                self.assertEqual(
                    error.exception.code(), grpc.StatusCode.PERMISSION_DENIED
                )
            self.assertTrue(
                stub.Heartbeat(
                    pb.WorkerHeartbeatRequest(
                        job_id=claimed.job_id, lease_token=claimed.lease_token
                    ),
                    timeout=5,
                    metadata=cpu_meta,
                ).accepted
            )
            self.assertTrue(
                stub.Complete(
                    pb.WorkerCompleteRequest(
                        job_id=claimed.job_id,
                        lease_token=claimed.lease_token,
                        text="correct worker",
                    ),
                    timeout=5,
                    metadata=cpu_meta,
                ).accepted
            )
            for metadata in (self.metadata, cpu_meta + cpu_meta):
                with self.assertRaises(grpc.RpcError) as error:
                    stub.Claim(
                        pb.WorkerClaimRequest(worker_id="wsl-cpu"),
                        timeout=5,
                        metadata=metadata,
                    )
                self.assertEqual(
                    error.exception.code(), grpc.StatusCode.UNAUTHENTICATED
                )
        finally:
            channel.close()
            server.stop(0).wait()

    def test_auth_and_worker_only_listener(self):
        job_id = self.submit()
        with self.assertRaises(grpc.RpcError) as error:
            self.stub.Claim(pb.WorkerClaimRequest(worker_id="pilot"), timeout=5)
        self.assertEqual(error.exception.code(), grpc.StatusCode.UNAUTHENTICATED)
        self.assertEqual(self.store.get_job(job_id).attempts, 0)
        with self.assertRaises(grpc.RpcError) as error:
            rpc.TranscriptionServiceStub(self.channel).GetStatus(
                pb.StatusRequest(job_id=job_id), timeout=5
            )
        self.assertEqual(error.exception.code(), grpc.StatusCode.UNIMPLEMENTED)

    def test_expired_owner_cannot_renew_fail_or_complete_after_reclaim(self):
        job_id = self.submit()
        old = self.claim()
        self.expire(job_id)
        new = self.claim()
        self.assertNotEqual(old.lease_token, new.lease_token)
        self.assertFalse(
            self.rpc(
                self.stub.Heartbeat,
                pb.WorkerHeartbeatRequest(job_id=job_id, lease_token=old.lease_token),
            ).accepted
        )
        self.assertFalse(
            self.rpc(
                self.stub.Complete,
                pb.WorkerCompleteRequest(
                    job_id=job_id, lease_token=old.lease_token, text="stale"
                ),
            ).accepted
        )
        self.assertFalse(
            self.rpc(
                self.stub.Fail,
                pb.WorkerFailRequest(
                    job_id=job_id,
                    lease_token=old.lease_token,
                    error_code="inference_failed",
                    retryable=True,
                ),
            ).accepted
        )
        self.assertTrue(
            self.rpc(
                self.stub.Complete,
                pb.WorkerCompleteRequest(
                    job_id=job_id, lease_token=new.lease_token, text="current"
                ),
            ).accepted
        )
        self.assertEqual(self.store.get_job(job_id).text, "current")

    def test_final_crash_is_terminal_and_cancel_rejects_result(self):
        job_id = self.submit()
        self.claim()
        self.expire(job_id)
        self.claim()
        self.expire(job_id)
        self.assertFalse(self.claim().job_id)
        self.assertEqual(self.store.get_job(job_id).error, "attempts_exhausted")
        other = self.submit()
        claim = self.claim()
        self.assertTrue(self.store.cancel_job(other))
        self.assertFalse(
            self.rpc(
                self.stub.Complete,
                pb.WorkerCompleteRequest(
                    job_id=other, lease_token=claim.lease_token, text="late"
                ),
            ).accepted
        )

    def test_cancelled_upload_cannot_be_queued_again(self):
        job, _ = self.store.create_job(
            fmt="wav",
            preset="voice",
            language_override="",
            initial_prompt_override="",
        )
        self.assertTrue(self.store.cancel_job(job.job_id))
        self.store.mark_downloading(job.job_id)
        self.assertIsNone(self.store.mark_queued(job.job_id))
        self.assertEqual(self.store.get_job(job.job_id).error, "cancelled")

    def test_remote_worker_real_transport_failure_returns_to_queue(self):
        job_id = self.submit()

        class BrokenEngine:
            def transcribe(self, *args, **kwargs):
                raise RuntimeError("synthetic-sensitive-content")

        worker = RemoteWorker(
            Config(f"127.0.0.1:{self.port}", "/unused", "pilot"),
            self.stub,
            BrokenEngine(),
            TOKEN,
        )
        worker.process(self.claim())
        job = self.store.get_job(job_id)
        self.assertEqual(job.status, "queued")
        self.assertEqual(job.error, "inference_failed")

    def test_two_workers_claim_different_jobs(self):
        self.submit()
        self.submit()
        barrier = threading.Barrier(2)

        def claim():
            barrier.wait()
            return self.claim().job_id

        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: claim(), range(2)))
        self.assertEqual(len(set(results)), 2)

    def test_local_worker_renews_lease_before_first_segment(self):
        job_id = self.submit()
        claimed = self.store.claim_next_job("local-pilot")

        class SlowEngine:
            def transcribe(self, *args, **kwargs):
                # The job has a three second lease. There are no progress events.
                time.sleep(3.2)
                return TranscriptionResult("synthetic local result", [])

        worker = Worker.__new__(Worker)
        worker._store = self.store
        worker._worker_id = "local-pilot"
        worker._engine = SlowEngine()
        worker._process_claimed_job(claimed, threading.Event())
        self.assertEqual(self.store.get_job(job_id).status, "done")

    def test_invalid_format_rejected_before_writing(self):
        class Context:
            def abort(self, *args):
                raise ValueError(args[1])

        with self.assertRaisesRegex(ValueError, "Invalid audio format"):
            TranscriptionServicer(self.store).Submit(
                iter([pb.TranscribeChunk(format="../../outside", data=b"x")]), Context()
            )
        self.assertEqual(list(Path(self.tmp.name).iterdir()), [])

    def client_chunk(
        self,
        request_id="notes-bot:synthetic-operation",
        audio=b"synthetic audio",
        retain=True,
        **options,
    ):
        return pb.TranscribeChunk(
            format="wav",
            data=audio,
            request_id=request_id,
            retain_until_ack=retain,
            options=pb.TranscriptionOptions(
                preset=options.get("preset", "voice"), language="en"
            ),
        )

    def client_submit(self, **kwargs):
        return self.client_stub.Submit(iter([self.client_chunk(**kwargs)]), timeout=5)

    def request_job(self, request_id="notes-bot:synthetic-operation"):
        with self.store.connect() as conn:
            row = conn.execute(
                "SELECT job_id FROM transcription_jobs WHERE request_id = %s",
                (request_id,),
            ).fetchone()
        return self.store.get_job(row["job_id"]) if row else None

    def wait_upload_started(self):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            job = self.request_job()
            if job and job.status == "downloading":
                return job
            time.sleep(0.01)
        self.fail("Upload did not enter downloading")

    def test_client_replay_after_lost_response_returns_same_job(self):
        first = self.client_submit()
        restarted_store = JobStore()
        restarted_store.init_schema()
        second = self.client_submit()
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(second.queue_position, 1)
        job = restarted_store.get_job(first.job_id)
        self.assertEqual(job.status, "queued")
        self.assertEqual(job.attempts, 0)
        self.assertTrue(job.retain_until_ack)
        self.assertEqual(Path(job.input_path).read_bytes(), b"synthetic audio")
        with self.store.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM transcription_jobs").fetchone()[
                    "n"
                ],
                1,
            )

    def test_client_concurrent_duplicate_cannot_overwrite_upload(self):
        release = threading.Event()

        def stream():
            yield self.client_chunk(audio=b"original ")
            release.wait(3)
            yield pb.TranscribeChunk(data=b"audio")

        pending = self.client_stub.Submit.future(stream(), timeout=5)
        try:
            original = self.wait_upload_started()
            with self.assertRaises(grpc.RpcError) as error:
                self.client_submit(audio=b"competing payload")
            self.assertEqual(error.exception.code(), grpc.StatusCode.ABORTED)
            release.set()
            accepted = pending.result(timeout=5)
            self.assertEqual(accepted.job_id, original.job_id)
            self.assertEqual(Path(original.input_path).read_bytes(), b"original audio")
        finally:
            release.set()
            pending.cancel()

    def test_interrupted_client_upload_replays_same_job_and_replaces_partial(self):
        release = threading.Event()

        def stream():
            yield self.client_chunk(audio=b"partial")
            release.wait(3)
            yield pb.TranscribeChunk(data=b"not accepted after cancellation")

        pending = self.client_stub.Submit.future(stream(), timeout=5)
        original = self.wait_upload_started()
        pending.cancel()
        release.set()
        deadline = time.monotonic() + 3
        while True:
            try:
                replay = self.client_submit(audio=b"complete replacement audio")
                break
            except grpc.RpcError as error:
                if (
                    error.code() != grpc.StatusCode.ABORTED
                    or time.monotonic() > deadline
                ):
                    raise
                time.sleep(0.02)
        self.assertEqual(replay.job_id, original.job_id)
        self.assertEqual(self.store.get_job(original.job_id).status, "queued")
        self.assertEqual(
            Path(original.input_path).read_bytes(), b"complete replacement audio"
        )
        self.assertEqual(
            list(Path(original.input_path).parent.glob("upload-*.part")), []
        )

    def test_recover_upload_left_downloading_by_a_previous_process(self):
        with self.store.reserve_upload(
            request_id="notes-bot:synthetic-operation",
            retain_until_ack=True,
            fmt="wav",
            preset="voice",
            language_override="en",
            initial_prompt_override="",
        ) as reservation:
            self.store.mark_downloading(reservation.job.job_id)
            (reservation.input_path.parent / "upload-abandoned.part").write_bytes(
                b"incomplete"
            )
        replay = self.client_submit()
        self.assertEqual(replay.job_id, reservation.job.job_id)
        self.assertEqual(Path(reservation.input_path).read_bytes(), b"synthetic audio")
        self.assertFalse(
            (reservation.input_path.parent / "upload-abandoned.part").exists()
        )

    def test_client_identity_options_conflict_is_rejected(self):
        first = self.client_submit()
        for change in ({"preset": "lecture"}, {"retain": False}):
            with self.subTest(change=change), self.assertRaises(grpc.RpcError) as error:
                self.client_submit(**change)
            self.assertEqual(error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(self.request_job().job_id, first.job_id)

    def test_retained_result_survives_cleanup_until_idempotent_ack(self):
        job_id = self.client_submit().job_id
        claimed = self.claim()
        self.assertTrue(
            self.rpc(
                self.stub.Complete,
                pb.WorkerCompleteRequest(
                    job_id=job_id,
                    lease_token=claimed.lease_token,
                    text="durable client result",
                ),
            ).accepted
        )
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET finished_at = NOW() - INTERVAL '1 day' WHERE job_id = %s",
                (job_id,),
            )
        self.assertEqual(self.store.cleanup_expired_jobs(1), 0)
        self.assertEqual(
            self.client_stub.GetStatus(pb.StatusRequest(job_id=job_id), timeout=5).text,
            "durable client result",
        )
        self.assertTrue(Path(self.store.get_job(job_id).input_path).exists())
        request = pb.AckRequest(job_id=job_id)
        self.assertTrue(self.client_stub.Ack(request, timeout=5).acknowledged)
        acknowledged = self.store.get_job(job_id).acknowledged_at
        self.assertTrue(self.client_stub.Ack(request, timeout=5).acknowledged)
        self.assertEqual(self.store.get_job(job_id).acknowledged_at, acknowledged)
        self.assertEqual(self.store.cleanup_expired_jobs(1), 0)
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET acknowledged_at = NOW() - INTERVAL '1 day' WHERE job_id = %s",
                (job_id,),
            )
        self.assertEqual(self.store.cleanup_expired_jobs(1), 1)
        self.assertIsNone(self.store.get_job(job_id))

    def test_cancelled_retained_error_can_be_acknowledged_and_cleaned(self):
        job_id = self.client_submit().job_id
        self.assertTrue(
            self.client_stub.Cancel(
                pb.CancelRequest(job_id=job_id), timeout=5
            ).cancelled
        )
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET finished_at = NOW() - INTERVAL '1 day' WHERE job_id = %s",
                (job_id,),
            )
        self.assertEqual(self.store.cleanup_expired_jobs(1), 0)
        self.assertEqual(
            self.client_stub.GetStatus(
                pb.StatusRequest(job_id=job_id), timeout=5
            ).error,
            "cancelled",
        )
        self.assertEqual(self.client_submit().job_id, job_id)
        self.assertTrue(
            self.client_stub.Ack(pb.AckRequest(job_id=job_id), timeout=5).acknowledged
        )
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET acknowledged_at = NOW() - INTERVAL '1 day' WHERE job_id = %s",
                (job_id,),
            )
        self.assertEqual(self.store.cleanup_expired_jobs(1), 1)

    def test_ack_pending_rejected_and_legacy_cleanup_unchanged(self):
        pending = self.client_submit()
        with self.assertRaises(grpc.RpcError) as error:
            self.client_stub.Ack(pb.AckRequest(job_id=pending.job_id), timeout=5)
        self.assertEqual(error.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
        with self.assertRaises(grpc.RpcError) as error:
            self.client_stub.Ack(pb.AckRequest(job_id="unknown"), timeout=5)
        self.assertEqual(error.exception.code(), grpc.StatusCode.NOT_FOUND)
        legacy = self.client_submit(request_id="", retain=False)
        self.store.cancel_job(legacy.job_id)
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET finished_at = NOW() - INTERVAL '1 day' WHERE job_id = %s",
                (legacy.job_id,),
            )
        self.assertEqual(self.store.cleanup_expired_jobs(1), 1)
        self.assertIsNotNone(self.store.get_job(pending.job_id))
        self.assertTrue(Path(self.store.get_job(pending.job_id).input_path).exists())

    def test_retained_worker_failure_can_be_replayed_received_and_acked(self):
        job_id = self.client_submit().job_id
        claimed = self.claim()
        self.assertTrue(
            self.rpc(
                self.stub.Fail,
                pb.WorkerFailRequest(
                    job_id=job_id,
                    lease_token=claimed.lease_token,
                    error_code="inference_failed",
                    retryable=False,
                ),
            ).accepted
        )
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE transcription_jobs SET finished_at = NOW() - INTERVAL '1 day' WHERE job_id = %s",
                (job_id,),
            )
        self.assertEqual(self.store.cleanup_expired_jobs(1), 0)
        self.assertEqual(self.client_submit().job_id, job_id)
        self.assertEqual(
            self.client_stub.GetStatus(
                pb.StatusRequest(job_id=job_id), timeout=5
            ).error,
            "inference_failed",
        )
        self.assertTrue(
            self.client_stub.Ack(pb.AckRequest(job_id=job_id), timeout=5).acknowledged
        )

    def test_oversized_idempotent_upload_is_terminal_and_can_be_acked(self):
        with patch.dict(os.environ, {"MAX_AUDIO_BYTES": "1"}):
            with self.assertRaises(grpc.RpcError) as error:
                self.client_submit()
        self.assertEqual(error.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        job = self.request_job()
        self.assertEqual(job.error, "input_too_large")
        self.assertEqual(self.client_submit().job_id, job.job_id)
        self.assertTrue(
            self.client_stub.Ack(
                pb.AckRequest(job_id=job.job_id), timeout=5
            ).acknowledged
        )

    def test_migration_adopts_previous_schema_without_losing_jobs(self):
        job_id = self.submit()
        original = self.store.get_job(job_id)
        with self.store.connect() as conn:
            conn.execute(
                "ALTER TABLE transcription_jobs DROP COLUMN request_id, DROP COLUMN retain_until_ack, DROP COLUMN acknowledged_at"
            )
            # Emulate the old production schema, which predates availability.
            conn.execute("DROP TABLE worker_availability")
            conn.execute("DROP TABLE transcriber_schema_migrations")
        self.store.init_schema()
        self.store.init_schema()
        adopted = self.store.get_job(job_id)
        self.assertEqual(adopted.status, original.status)
        self.assertEqual(Path(adopted.input_path).read_bytes(), b"synthetic audio")
        self.assertEqual(adopted.request_id, "")
        self.assertFalse(adopted.retain_until_ack)
        with self.store.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM transcriber_schema_migrations"
                ).fetchone()["n"],
                3,
            )

    def test_real_mac_inference_optional(self):
        audio_file = os.getenv("TRANSCRIBER_PILOT_AUDIO_FILE")
        if not audio_file:
            self.skipTest(
                "Set TRANSCRIBER_PILOT_AUDIO_FILE for cached model speech smoke"
            )
        job_id = self.submit(Path(audio_file).read_bytes())
        worker = RemoteWorker(
            Config(f"127.0.0.1:{self.port}", "/unused", "mac-pilot"),
            self.stub,
            self.engine,
            TOKEN,
        )
        worker.process(self.claim())
        job = self.store.get_job(job_id)
        self.assertEqual(job.status, "done")
        self.assertIn("worker", job.text.lower())


if __name__ == "__main__":
    unittest.main()
