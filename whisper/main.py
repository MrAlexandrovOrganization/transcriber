"""Entry point for the Whisper transcription gRPC service."""

import logging
import os
from concurrent import futures

import grpc

from proto import whisper_pb2_grpc
from whisper.server import TranscriptionServicer

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

_50MB = 50 * 1024 * 1024


def serve():
    port = os.getenv("GRPC_PORT", "50053")
    grpc_workers = int(os.getenv("GRPC_WORKERS", "4"))
    server = grpc.server(
        # I/O-bound threads for receiving files concurrently while one transcribes.
        futures.ThreadPoolExecutor(max_workers=grpc_workers),
        options=[
            ("grpc.max_receive_message_length", _50MB),
            ("grpc.max_send_message_length", _50MB),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    whisper_pb2_grpc.add_TranscriptionServiceServicer_to_server(
        TranscriptionServicer(), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("Whisper gRPC server started on port %s", port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
