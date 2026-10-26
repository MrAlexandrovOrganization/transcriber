"""Synthetic transport tests; production endpoints/credentials are never used."""

import asyncio
import dataclasses
import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

import grpc
import test_worker_api as legacy

from proto import whisper_pb2 as pb
from whisper.broker import BrokerConfig, Publisher, delivery
from whisper.inference import InferenceStopped, TranscriptionResult
from whisper.remote_worker import Config, RemoteWorkerPool

BROKER_FILE = os.getenv("TRANSCRIBER_TEST_RABBITMQ_FILE")


class BrokerConfigTests(unittest.TestCase):
    def test_credentials_are_validated_without_disclosure(self):
        value = dict(
            host="localhost",
            port=5672,
            virtual_host="synthetic",
            username="pilot",
            password="synthetic-password-at-least-32-characters",
            tls=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connection.json"
            path.write_text(json.dumps(value))
            self.assertNotIn(value["password"], repr(BrokerConfig.from_file(path)))
            for bad in (
                {"host": "remote.example"},
                {"port": 0},
                {"password": "short"},
                {"tls": "false"},
                {"namespace": "../unexpected"},
            ):
                path.write_text(json.dumps(value | bad))
                with self.assertRaisesRegex(
                    ValueError, "^Invalid RabbitMQ connection file$"
                ):
                    BrokerConfig.from_file(path)

    def test_envelopes_reject_poison_and_mismatched_delivery_identity(self):
        message_id = str(uuid.uuid4())
        value = dict(version=1, job_id=str(uuid.uuid4()), message_id=message_id)
        self.assertEqual(delivery(json.dumps(value).encode(), message_id), value)
        for body in (
            b"bad",
            b"x" * 1025,
            b"[]",
            json.dumps(value | {"version": True}).encode(),
            json.dumps(value | {"message_id": str(uuid.uuid4())}).encode(),
        ):
            with self.assertRaises(ValueError):
                delivery(body, message_id)


class GateEngine:
    def __init__(self, gate=None, fail=False):
        self.gate = gate or threading.Event()
        self.active = 0
        self.calls = 0
        self.peak = 0
        self.fail = fail
        self.lock = threading.Lock()

    def transcribe(self, path, *, should_stop, **kwargs):
        assert Path(path).read_bytes() == b"synthetic audio"
        with self.lock:
            self.calls += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            while not self.gate.wait(0.02):
                if should_stop.is_set():
                    raise InferenceStopped()
            if self.fail:
                raise RuntimeError("sensitive inference error")
            return TranscriptionResult("synthetic transcript", [])
        finally:
            with self.lock:
                self.active -= 1


@unittest.skipUnless(
    legacy.DSN and BROKER_FILE,
    "Set isolated PostgreSQL and RabbitMQ test configuration",
)
class RabbitIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = legacy.WorkerAPITests(methodName="runTest")
        self.fixture.setUp()
        self.store = self.fixture.store
        self.store.queue_backend = "rabbitmq"
        self.store._lease_seconds = 10
        self.store.worker_policies = {
            "wsl-cpu": {"priority": 10, "max_concurrency": 3},
            "mac-primary": {"priority": 20, "max_concurrency": 2},
            "wsl-gpu": {"priority": 30, "max_concurrency": 1},
        }
        self.config = dataclasses.replace(
            BrokerConfig.from_file(BROKER_FILE),
            namespace="transcriber.test" + uuid.uuid4().hex,
        )
        self.connection_file = Path(self.fixture.tmp.name) / "broker.json"
        self.connection_file.write_text(
            json.dumps(
                {
                    k: v
                    for k, v in dataclasses.asdict(self.config).items()
                    if k != "ca_file"
                }
            )
        )
        self.publisher = Publisher(self.config)
        self.pools = []

    def tearDown(self):
        for pool, thread in self.pools:
            pool.stop.set()
        for pool, thread in self.pools:
            thread.join(timeout=5)
        self.publisher.close()

        async def cleanup():
            connection = await self.config.connect()
            async with connection:
                channel = await connection.channel()
                for suffix in ("jobs", "retry", "dead"):
                    await channel.queue_delete(self.config.name(suffix))
                for suffix in ("tasks", "defer"):
                    await channel.exchange_delete(self.config.name(suffix))

        try:
            asyncio.run(cleanup())
        finally:
            self.fixture.tearDown()

    def wait(self, predicate, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail("Timed out waiting for synthetic broker processing")

    def submit(self):
        return self.fixture.submit()

    def publish(self):
        while self.store.publish_next(self.publisher.publish):
            pass

    def start_pool(self, worker_id, slots, engine):
        config = Config(
            f"127.0.0.1:{self.fixture.port}",
            "/unused",
            worker_id,
            concurrency=slots,
            poll_interval=0.1,
            rpc_timeout=5,
            broker_file=str(self.connection_file),
        )
        pool = RemoteWorkerPool(
            config, self.fixture.stub, engine, self.fixture.token(worker_id)
        )
        thread = threading.Thread(target=pool.run_forever, daemon=True)
        thread.start()
        self.pools.append((pool, thread))
        return pool

    def ready(self, pool):
        return all(worker._broker_connected.is_set() for worker in pool.workers)

    def slot(self, worker_id="wsl-cpu"):
        instance = str(uuid.uuid4())
        self.fixture.report_availability(worker_id, instance)
        priority = self.store.register_consumer(worker_id, instance)
        self.assertIsNotNone(priority)
        return instance

    def begin(
        self, job, instance, delivery_id=None, worker_id="wsl-cpu", message_id=None
    ):
        return self.fixture.rpc(
            self.fixture.stub.BeginAttempt,
            pb.WorkerBeginRequest(
                worker_id=worker_id,
                instance_id=instance,
                job_id=job,
                message_id=message_id or self.store.get_job(job).dispatch_id,
                delivery_id=delivery_id or str(uuid.uuid4()),
            ),
        )

    def test_outbox_rollback_retry_and_ambiguous_duplicate_are_safe(self):
        job = self.submit()
        with self.assertRaises(RuntimeError):
            self.store.publish_next(
                lambda *_: (_ for _ in ()).throw(RuntimeError("offline"))
            )
        with self.store.connect() as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT published_at FROM transcription_outbox WHERE job_id = %s",
                    (job,),
                ).fetchone()["published_at"]
            )
        # Simulate confirmed publication followed by a lost database commit.
        self.publisher.publish(job, self.store.get_job(job).dispatch_id)
        self.publish()
        gate = threading.Event()
        gate.set()
        engine = GateEngine(gate)
        self.start_pool("wsl-cpu", 2, engine)
        self.wait(lambda: self.store.get_job(job).status == "done")
        time.sleep(1)
        self.assertEqual(engine.calls, 1)
        self.assertEqual(self.store.get_job(job).attempts, 1)

    def test_real_broker_preserves_priorities_and_shared_model_slots(self):
        # Create topology before subscribing, without publishing work yet.
        from whisper.broker import declare_topology

        async def topology():
            connection = await self.config.connect()
            async with connection:
                channel = await connection.channel()
                await declare_topology(channel, self.config)

        asyncio.run(topology())
        engines = [GateEngine() for _ in range(3)]
        cpu = self.start_pool("wsl-cpu", 3, engines[0])
        mac = self.start_pool("mac-primary", 2, engines[1])
        gpu = self.start_pool("wsl-gpu", 1, engines[2])
        self.wait(lambda: all(self.ready(pool) for pool in (cpu, mac, gpu)))
        jobs = [self.submit() for _ in range(3)]
        self.publish()
        self.wait(lambda: engines[0].active == 3)
        self.assertEqual([engine.active for engine in engines], [3, 0, 0])
        jobs.extend(self.submit() for _ in range(2))
        self.publish()
        self.wait(lambda: engines[1].active == 2)
        self.assertEqual(engines[2].active, 0)
        jobs.append(self.submit())
        self.publish()
        self.wait(lambda: engines[2].active == 1)
        self.assertEqual([engine.peak for engine in engines], [3, 2, 1])
        for engine in engines:
            engine.gate.set()
        self.wait(lambda: all(self.store.get_job(job).status == "done" for job in jobs))

    def test_atomic_pool_admission_and_disabled_identity(self):
        from concurrent.futures import ThreadPoolExecutor

        slots = [str(uuid.uuid4()) for _ in range(8)]
        for slot in slots:
            self.fixture.report_availability("wsl-cpu", slot)
        with ThreadPoolExecutor(max_workers=8) as executor:
            grants = list(
                executor.map(
                    lambda slot: self.store.register_consumer("wsl-cpu", slot), slots
                )
            )
        self.assertEqual(sum(grant is not None for grant in grants), 3)
        self.assertIsNone(self.store.register_consumer("unlisted-worker", slots[0]))
        self.store.worker_policies["wsl-cpu"]["max_concurrency"] = 0
        self.assertIsNone(self.store.register_consumer("wsl-cpu", slots[0]))

    def test_attempt_replay_expiry_stale_owner_and_exhaustion(self):
        job = self.submit()
        slot = self.slot()
        receipt = str(uuid.uuid4())
        first = self.begin(job, slot, receipt)
        replay = self.begin(job, slot, receipt)
        self.assertEqual(first.job.lease_token, replay.job.lease_token)
        self.assertEqual(self.store.get_job(job).attempts, 1)
        self.assertEqual(self.begin(job, self.slot()).disposition, "busy")
        self.fixture.expire(job)
        replacement = self.begin(job, slot)
        self.assertNotEqual(first.job.lease_token, replacement.job.lease_token)
        self.assertFalse(self.store.heartbeat(job, first.job.lease_token, 50))
        self.assertFalse(
            self.store.complete_job(
                job_id=job,
                worker_id=first.job.lease_token,
                text="stale",
                segments=[],
                result_path="",
            )
        )
        self.fixture.expire(job)
        self.assertEqual(self.begin(job, slot).disposition, "discard")
        self.assertEqual(self.store.get_job(job).error, "attempts_exhausted")

    def test_retry_outbox_and_cancel_invalidate_old_delivery(self):
        job = self.submit()
        slot = self.slot()
        old_message = self.store.get_job(job).dispatch_id
        first = self.begin(job, slot)
        self.assertTrue(
            self.store.fail_job(
                job_id=job,
                worker_id=first.job.lease_token,
                error="inference_failed",
                retryable=True,
            )
        )
        self.assertEqual(self.store.get_job(job).status, "queued")
        self.assertNotEqual(old_message, self.store.get_job(job).dispatch_id)
        self.assertEqual(
            self.begin(job, slot, message_id=old_message).disposition, "discard"
        )
        self.store.cancel_job(job)
        self.assertEqual(self.begin(job, slot).disposition, "discard")

    def test_cutover_adopts_jobs_accepted_after_schema_upgrade(self):
        self.store.queue_backend = "postgres"
        job = self.submit()
        self.assertEqual(self.store.get_job(job).dispatch_id, "")
        self.store.queue_backend = "rabbitmq"
        self.store.init_schema()
        adopted = self.store.get_job(job)
        self.assertTrue(adopted.dispatch_id)
        self.assertEqual(Path(adopted.input_path).read_bytes(), b"synthetic audio")
        self.store.init_schema()
        self.assertEqual(adopted.dispatch_id, self.store.get_job(job).dispatch_id)
        self.publish()
        gate = threading.Event()
        gate.set()
        self.start_pool("wsl-cpu", 1, GateEngine(gate))
        self.wait(lambda: self.store.get_job(job).status == "done")

    def test_drain_finishes_delivery_and_resume_preserves_all_slots(self):
        first = self.submit()
        self.publish()
        engine = GateEngine()
        pool = self.start_pool("wsl-cpu", 2, engine)
        self.wait(lambda: engine.active == 1 and self.ready(pool))
        pool.drain.set()
        self.assertFalse(all(worker._drained.is_set() for worker in pool.workers))
        engine.gate.set()
        self.wait(lambda: self.store.get_job(first).status == "done")
        self.wait(lambda: all(worker._drained.is_set() for worker in pool.workers))
        second = self.submit()
        self.publish()
        time.sleep(0.5)
        self.assertEqual(self.store.get_job(second).status, "queued")
        pool.drain.clear()
        self.wait(lambda: self.store.get_job(second).status == "done")
        self.assertEqual(engine.calls, 2)

    def test_long_inference_keeps_broker_networking_and_api_lease_alive(self):
        job = self.submit()
        self.store._lease_seconds = 3
        self.publish()
        engine = GateEngine()
        pool = self.start_pool("wsl-cpu", 1, engine)
        self.wait(lambda: engine.active == 1)
        # Longer than both the execution lease and AMQP heartbeat interval.
        time.sleep(22)
        self.assertTrue(self.ready(pool))
        self.assertEqual(self.store.get_job(job).status, "running")
        running = self.store.get_job(job)
        self.assertIsNotNone(self.store.get_owned_job(job, running.lease_owner))
        engine.gate.set()
        self.wait(lambda: self.store.get_job(job).status == "done")
        self.assertEqual(self.store.get_job(job).attempts, 1)

    def test_broker_redelivery_after_disconnect_and_completed_job_deduplication(self):
        job = self.submit()
        self.publish()

        async def receive_and_disconnect():
            connection = await self.config.connect()
            channel = await connection.channel()
            queue = await channel.get_queue(self.config.name("jobs"))
            message = await queue.get(timeout=5)
            self.assertEqual(delivery(message.body, message.message_id)["job_id"], job)
            await connection.close()  # No acknowledgement: RabbitMQ owns recovery.

        asyncio.run(receive_and_disconnect())
        gate = threading.Event()
        gate.set()
        engine = GateEngine(gate)
        self.start_pool("wsl-cpu", 1, engine)
        self.wait(lambda: self.store.get_job(job).status == "done")
        self.publisher.publish(job, self.store.get_job(job).dispatch_id)
        time.sleep(1)
        self.assertEqual(engine.calls, 1)

    def test_client_api_idempotency_retention_ack_and_sql_claim_is_disabled(self):
        def chunks():
            return iter(
                [
                    pb.TranscribeChunk(
                        data=b"synthetic audio",
                        format="wav",
                        request_id="synthetic:" + self.fixture.schema,
                        retain_until_ack=True,
                    )
                ]
            )

        first = self.fixture.client_stub.Submit(chunks(), timeout=5)
        self.assertEqual(
            first.job_id, self.fixture.client_stub.Submit(chunks(), timeout=5).job_id
        )
        self.publish()
        gate = threading.Event()
        gate.set()
        self.start_pool("wsl-cpu", 1, GateEngine(gate))
        self.wait(lambda: self.store.get_job(first.job_id).status == "done")
        status = self.fixture.client_stub.GetStatus(
            pb.StatusRequest(job_id=first.job_id), timeout=5
        )
        self.assertEqual(status.text, "synthetic transcript")
        self.assertTrue(
            self.fixture.client_stub.Ack(
                pb.AckRequest(job_id=first.job_id), timeout=5
            ).acknowledged
        )
        with self.assertRaises(grpc.RpcError) as error:
            self.fixture.claim()
        self.assertEqual(error.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
