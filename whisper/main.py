"""Entry point for the Whisper transcription service."""

import logging
import os

from whisper.server import run_ingress, run_worker

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def serve() -> None:
    role = os.getenv("SERVICE_ROLE", "ingress").strip().lower()
    if role == "worker":
        logger.info("Starting Whisper worker role")
        run_worker()
        return

    logger.info("Starting Whisper ingress role")
    run_ingress()


if __name__ == "__main__":
    serve()
