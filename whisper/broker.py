"""RabbitMQ transport. Audio and results remain behind the authenticated API."""

from __future__ import annotations

import asyncio
import dataclasses
import ipaddress
import json
import logging
import re
import ssl
import threading
import uuid
from pathlib import Path

import aio_pika

LOG = logging.getLogger(__name__)
# Client exceptions/connection diagnostics can contain credentials and URLs.
for _name in ("aio_pika", "aiormq", "pamqp"):
    logging.getLogger(_name).setLevel(logging.CRITICAL)


@dataclasses.dataclass(frozen=True, repr=False)
class BrokerConfig:
    host: str
    port: int
    virtual_host: str
    username: str
    password: str
    tls: bool = True
    ca_file: str = ""
    namespace: str = "transcriber"

    @classmethod
    def from_file(cls, path, ca_file=""):
        try:
            raw = Path(path).read_bytes()
            if len(raw) > 8192:
                raise ValueError()
            config = cls(**json.loads(raw), ca_file=ca_file)
            if (
                not isinstance(config.host, str)
                or not config.host
                or re.search(r"[\s/@?#]", config.host)
                or type(config.port) is not int
                or not 1 <= config.port <= 65535
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", config.username)
                or not re.fullmatch(r"[A-Za-z0-9_./-]{1,128}", config.virtual_host)
                or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", config.namespace)
                or not isinstance(config.password, str)
                or not 32 <= len(config.password) <= 512
                or any(not 33 <= ord(c) <= 126 for c in config.password)
                or type(config.tls) is not bool
            ):
                raise ValueError()
            if not config.tls:
                if (
                    config.host != "localhost"
                    and not ipaddress.ip_address(config.host).is_loopback
                ):
                    raise ValueError()
            return config
        except (OSError, ValueError, TypeError):
            raise ValueError("Invalid RabbitMQ connection file") from None

    def name(self, suffix):
        return self.namespace + "." + suffix

    async def connect(self):
        context = (
            ssl.create_default_context(cafile=self.ca_file or None)
            if self.tls
            else None
        )
        return await aio_pika.connect(
            host=self.host,
            port=self.port,
            virtualhost=self.virtual_host,
            login=self.username,
            password=self.password,
            ssl=self.tls,
            ssl_context=context,
            timeout=5,
            heartbeat=20,
        )


async def declare_topology(channel, config):
    tasks = await channel.declare_exchange(config.name("tasks"), "direct", durable=True)
    defer = await channel.declare_exchange(config.name("defer"), "direct", durable=True)
    dead = await channel.declare_queue(
        config.name("dead"), durable=True, arguments={"x-queue-type": "quorum"}
    )
    await dead.bind(tasks, "dead")
    jobs = await channel.declare_queue(
        config.name("jobs"),
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": -1,
            "x-dead-letter-exchange": tasks.name,
            "x-dead-letter-routing-key": "dead",
        },
    )
    await jobs.bind(tasks, "ready")
    retry = await channel.declare_queue(
        config.name("retry"),
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": -1,
            "x-message-ttl": 5000,
            "x-overflow": "reject-publish",
            "x-dead-letter-exchange": tasks.name,
            "x-dead-letter-routing-key": "ready",
        },
    )
    await retry.bind(defer, "retry")


def delivery(body, message_id):
    try:
        if len(body) > 1024:
            raise ValueError()
        value = json.loads(body)
        if (
            set(value) != {"version", "job_id", "message_id"}
            or type(value["version"]) is not int
            or value["version"] != 1
        ):
            raise ValueError()
        for name in ("job_id", "message_id"):
            if str(uuid.UUID(value[name])) != value[name]:
                raise ValueError()
        if value["message_id"] != message_id:
            raise ValueError()
        return value
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ValueError("Invalid RabbitMQ task envelope") from None


class Publisher:
    """One event loop/connection, used only by the outbox thread; confirms are bounded."""

    def __init__(self, config):
        self.config = config
        self.runner = asyncio.Runner()
        self.runner.get_loop().set_exception_handler(
            lambda _loop, _context: LOG.warning("RabbitMQ background operation failed")
        )
        self.connection = None
        self.channel = None

    async def _publish(self, job_id, message_id):
        if self.connection is None or self.connection.is_closed:
            self.connection = await self.config.connect()
            self.channel = await self.connection.channel(
                publisher_confirms=True, on_return_raises=True
            )
            await declare_topology(self.channel, self.config)
        exchange = await self.channel.get_exchange(
            self.config.name("tasks"), ensure=False
        )
        body = json.dumps(
            {"version": 1, "job_id": job_id, "message_id": message_id}
        ).encode()
        await exchange.publish(
            aio_pika.Message(
                body=body,
                message_id=message_id,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="ready",
            mandatory=True,
            timeout=5,
        )

    def publish(self, job_id, message_id):
        try:
            self.runner.run(
                asyncio.wait_for(self._publish(job_id, message_id), timeout=10)
            )
        except Exception:
            self.runner.run(self._close())
            raise RuntimeError("RabbitMQ publication unavailable") from None

    async def _close(self):
        if self.connection is not None:
            try:
                await asyncio.wait_for(self.connection.close(), timeout=3)
            except Exception:
                pass
        self.connection = self.channel = None

    def close(self):
        self.runner.run(self._close())
        self.runner.close()


def publish_outbox(store, config, stop):
    publisher = Publisher(config)
    try:
        while not stop.is_set():
            try:
                if not store.publish_next(publisher.publish):
                    stop.wait(0.5)
            except Exception:
                LOG.warning("RabbitMQ outbox publication failed; retry scheduled")
                stop.wait(2)
    finally:
        publisher.close()


async def daemon_call(function, *args):
    """Blocking gRPC/inference never runs on the broker's networking loop."""
    loop = asyncio.get_running_loop()
    result = loop.create_future()

    def finish(value=None, error=None):
        if not result.done():
            result.set_exception(error) if error else result.set_result(value)

    def run():
        try:
            value = function(*args)
        except Exception as error:
            if not loop.is_closed():
                loop.call_soon_threadsafe(finish, None, error)
        else:
            if not loop.is_closed():
                loop.call_soon_threadsafe(finish, value)

    threading.Thread(target=run, daemon=True, name="rabbit-inference").start()
    return await result


async def consume_slot(worker, config):
    """One admitted slot = one consumer with prefetch one; no polling of job rows."""
    asyncio.get_running_loop().set_exception_handler(
        lambda _loop, _context: LOG.warning("RabbitMQ background operation failed")
    )
    connection = await config.connect()
    worker._broker_lost.clear()
    work = None
    try:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        await channel.set_qos(prefetch_count=1)
        queue = await channel.get_queue(config.name("jobs"), ensure=False)
        retry = await channel.get_exchange(config.name("defer"), ensure=False)
        incoming = asyncio.Queue(maxsize=1)
        priority = worker._broker_priority
        await queue.consume(
            incoming.put, no_ack=False, arguments={"x-priority": priority}
        )
        worker._broker_connected.set()
        worker._health(True)
        while (
            not worker.stop.is_set()
            and not worker.drain.is_set()
            and worker._available.is_set()
            and worker._broker_admitted.is_set()
        ):
            if priority != worker._broker_priority or connection.is_closed:
                break
            try:
                message = await asyncio.wait_for(incoming.get(), timeout=0.25)
            except asyncio.TimeoutError:
                continue
            # Drain/withdrawal may arrive while awaiting a delivery. Closing the
            # channel returns this unstarted message without consuming an attempt.
            if (
                worker.stop.is_set()
                or worker.drain.is_set()
                or not worker._available.is_set()
                or not worker._broker_admitted.is_set()
            ):
                break
            try:
                task = delivery(message.body, message.message_id)
            except ValueError:
                await message.reject(requeue=False)
                LOG.warning("Malformed RabbitMQ delivery rejected")
                continue
            work = asyncio.create_task(daemon_call(worker._process_delivery, task))
            while not work.done():
                if (
                    connection.is_closed
                    or worker.stop.is_set()
                    or not worker._available.is_set()
                ):
                    worker._broker_lost.set()
                if worker.stop.is_set():
                    break
                await asyncio.sleep(0.1)
            if worker.stop.is_set():
                break
            disposition = await work
            work = None
            if connection.is_closed:
                break
            if disposition == "busy":
                # Confirm the delayed copy before acknowledging this delivery.
                await retry.publish(
                    aio_pika.Message(
                        body=message.body,
                        message_id=message.message_id,
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key="retry",
                    mandatory=True,
                    timeout=5,
                )
                await message.ack()
            elif disposition == "settled":
                await message.ack()
            else:
                # API unavailable or ambiguous completion: preserve broker ownership.
                break
    finally:
        worker._broker_lost.set()
        worker._broker_connected.clear()
        worker._health(False)
        try:
            await asyncio.wait_for(connection.close(), timeout=3)
        except Exception:
            pass
        if work is not None:
            work.cancel()
