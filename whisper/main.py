"""Entry point for the Whisper transcription service."""

import logging
import os

from whisper.jsonlog import setup_logging
from whisper.server import run_ingress, run_worker
from whisper.telemetry import init_telemetry

setup_logging(os.getenv("SERVICE_ROLE", "ingress").strip().lower())
logger = logging.getLogger(__name__)


def serve() -> None:
    role = os.getenv("SERVICE_ROLE", "ingress").strip().lower()
    shutdown = init_telemetry(f"transcriber-{role}")
    try:
        if role == "worker":
            logger.info("Starting Whisper worker role")
            run_worker()
            return

        logger.info("Starting Whisper ingress role")
        run_ingress()
    finally:
        shutdown()


if __name__ == "__main__":
    serve()
