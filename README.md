# Transcriber Backend

Standalone Whisper gRPC transcription service. Runs independently and can be shared across multiple projects on the same machine via Docker network `whisper-net`.

## Features

- Async job queue — submit audio and poll for results
- Single CPU worker (serializes inference, avoids OOM)
- `int8` quantization + VAD filter for faster CPU inference
- Russian language transcription (configurable)
- Completed jobs kept in memory for 2h then cleaned up

## gRPC API (`proto/whisper.proto`)

| RPC | Type | Description |
|-----|------|-------------|
| `Transcribe` | client-streaming (legacy) | Blocks until done |
| `Submit` | client-streaming | Returns `job_id` + queue position immediately |
| `GetStatus` | unary | Returns `PENDING/RUNNING/DONE/FAILED` + text |

Audio is streamed in chunks (recommended 1MB). Supported formats: any format accepted by `faster-whisper` (OGG, MP4, WAV, etc.).

## Running

```bash
# Create shared network (once per machine)
docker network create whisper-net

make up       # Build and start
make logs     # Follow logs
make down     # Stop
make deploy   # Full rebuild without cache
make restart  # Restart without rebuild
```

First start downloads the Whisper model — takes 1–2 minutes.

## Connecting a project

Add to the project's `docker-compose.yml`:

```yaml
networks:
  whisper-net:
    external: true

services:
  your-service:
    networks:
      - whisper-net
    environment:
      - WHISPER_GRPC_HOST=whisper
      - WHISPER_GRPC_PORT=50053
```

Then generate client stubs from `proto/whisper.proto` for your language:

**Go:**
```bash
protoc -I proto \
  --go_out=gen/whisper --go_opt=paths=source_relative \
  --go-grpc_out=gen/whisper --go-grpc_opt=paths=source_relative \
  proto/whisper.proto
```

**Python:**
```bash
poetry run python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  proto/whisper.proto
```

Use `Submit` + `GetStatus` for async (recommended), or `Transcribe` for blocking calls.

> The gRPC port is **not** published to the host — only containers in `whisper-net` can reach it.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50053` | gRPC server port |
| `WHISPER_MODEL` | `small` | Model size: `tiny`, `base`, `small`, `medium`, `large` |
| `JOB_TTL_S` | `7200` | Seconds to keep completed jobs in memory |
| `TRANSCRIBE_QUEUE_TIMEOUT_S` | `600` | Max seconds a sync `Transcribe` call waits in queue |

## Project Structure

```
.
├── proto/
│   ├── whisper.proto              # gRPC service definition (source of truth)
│   ├── whisper_pb2.py             # Generated Python stubs
│   └── whisper_pb2_grpc.py
├── whisper/
│   ├── server.py                  # TranscriptionServicer + async job queue
│   ├── main.py                    # gRPC server entry point
│   └── Dockerfile
├── pyproject.toml
└── docker-compose.yml
```

## Development

```bash
make install  # Poetry install
make proto    # Regenerate Python stubs from proto/whisper.proto
make clean    # Remove __pycache__ files
```

Requires: `protoc`, `grpcio-tools` (installed via dev dependencies).
