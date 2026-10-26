"""Real RabbitMQ TLS and least-privilege credentials, using disposable fixtures."""

import asyncio
import dataclasses
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import aio_pika
from aiormq.exceptions import ChannelAccessRefused

from scripts.provision_rabbitmq import provision
from whisper.broker import BrokerConfig, Publisher, delivery


def docker(*args):
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=120
    )
    if result.returncode:
        raise RuntimeError("Synthetic RabbitMQ Docker operation failed")
    return result.stdout.strip()


async def receive(config, job_id):
    connection = await config.connect()
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue(config.name("jobs"), ensure=False)
        message = await queue.get(timeout=5)
        assert delivery(message.body, message.message_id)["job_id"] == job_id
        await message.ack()


async def permissions(config):
    connection = await config.connect()
    async with connection:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        retry = await channel.get_exchange(config.name("defer"), ensure=False)
        job_id, message_id = str(uuid.uuid4()), str(uuid.uuid4())
        await retry.publish(
            aio_pika.Message(
                body=json.dumps(
                    {"version": 1, "job_id": job_id, "message_id": message_id}
                ).encode(),
                message_id=message_id,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="retry",
            mandatory=True,
            timeout=5,
        )
    await asyncio.sleep(6)
    await receive(config, job_id)
    for operation in ("declare", "publish"):
        connection = await config.connect()
        async with connection:
            channel = await connection.channel()
            try:
                if operation == "declare":
                    await channel.declare_queue("unauthorized", durable=True)
                else:
                    tasks = await channel.get_exchange(
                        config.name("tasks"), ensure=False
                    )
                    await tasks.publish(
                        aio_pika.Message(body=b"unauthorized"),
                        routing_key="ready",
                        timeout=5,
                    )
            except ChannelAccessRefused:
                pass
            else:
                raise AssertionError("Worker permission isolation failed")
    for invalid in (
        dataclasses.replace(config, ca_file=""),
        dataclasses.replace(config, host="127.0.0.1"),
    ):
        try:
            connection = await invalid.connect()
        except Exception:
            pass
        else:
            await connection.close()
            raise AssertionError("TLS trust/hostname verification failed")


def main():
    name = "transcriber-tls-test-" + uuid.uuid4().hex[:12]
    container = None
    with tempfile.TemporaryDirectory(prefix="transcriber-rabbit-tls-") as tmp:
        root = Path(tmp) / "bundle"
        provision(root, "localhost", ["wsl-cpu"], port=56749)
        publisher_path = root / "publisher" / "connection.json"
        value = json.loads(publisher_path.read_text())
        value["host"] = "localhost"
        publisher_path.write_text(json.dumps(value))
        ca = str(root / "publisher" / "ca.crt")
        publisher_config = BrokerConfig.from_file(publisher_path, ca)
        worker_config = BrokerConfig.from_file(root / "wsl-cpu" / "connection.json", ca)
        try:
            container = docker(
                "create",
                "--name",
                name,
                "--label",
                "purpose=transcriber-rabbit-tls-test",
                "--memory=512m",
                "--cpus=1",
                "-e",
                "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=+S 2:2 +SDcpu 1 +SDio 1",
                "-e",
                "RABBITMQ_CTL_ERL_ARGS=+S 1:1 +SDcpu 1 +SDio 1",
                "-p",
                "127.0.0.1:56749:5671",
                "rabbitmq:" + os.environ["RABBITMQ_VERSION"],
            )
            docker("cp", str(root / "server") + "/.", container + ":/etc/rabbitmq")
            docker("start", container)
            for _ in range(45):
                try:
                    docker(
                        "exec", container, "rabbitmq-diagnostics", "-q", "check_running"
                    )
                    break
                except RuntimeError:
                    time.sleep(1)
            publisher = Publisher(publisher_config)
            try:
                job_id = str(uuid.uuid4())
                deadline = time.monotonic() + 30
                while True:
                    try:
                        publisher.publish(job_id, str(uuid.uuid4()))
                        break
                    except RuntimeError:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.5)
                asyncio.run(receive(worker_config, job_id))
                asyncio.run(permissions(worker_config))
            finally:
                publisher.close()
            print(
                "RabbitMQ TLS, scoped credentials and confirmed deferred delivery verified"
            )
        finally:
            if container:
                docker("rm", "-f", "-v", container)


if __name__ == "__main__":
    main()
