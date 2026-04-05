# CLAUDE.md

## Project Overview

Standalone Whisper gRPC transcription service. Shared across multiple projects via Docker network `whisper-net`.
Clients: `transcriber-bot` (Go), `notes_bot` (Go).

**Language:** Python 3.11+
**Package manager:** Poetry (`pyproject.toml`)
**Deployment:** Docker Compose

## Key Files

| File | Purpose |
|------|---------|
| `whisper/server.py` | `TranscriptionServicer` — async job queue, single CPU worker thread |
| `whisper/main.py` | gRPC server entry point — binds to `[::]`, keepalive options, `max_workers=8` |
| `proto/whisper.proto` | gRPC service definition — source of truth for ALL clients |
| `proto/whisper_pb2.py` | Generated Python stubs (do not edit) |
| `proto/whisper_pb2_grpc.py` | Generated Python stubs (do not edit) |
| `docker-compose.yml` | Single `whisper` service on `whisper-net`, no `ports:` directive |
| `Makefile` | Dev and deploy commands |

## Running & Building

```bash
docker network create whisper-net  # once, on any machine
make up                            # build and start
make deploy                        # full rebuild --no-cache
make logs                          # follow logs
make restart                       # restart whisper container
make proto                         # regenerate Python stubs
make install                       # Poetry install (local dev)
make clean                         # remove __pycache__
```

## Architecture Notes

- Single background thread processes jobs sequentially (CPU-bound, avoids OOM)
- `Submit` RPC: queues job, returns `job_id` + queue position immediately
- `GetStatus` RPC: polls in-memory dict for job status/result
- `Transcribe` RPC (legacy): routes through same queue, blocks until done
- Cleanup thread removes jobs older than `JOB_TTL_S` (default 7200s)
- `max_workers=8` in gRPC server (for concurrent file receives, not inference)
- Service binds to `[::]` — security handled by Docker network isolation (no `ports:`)

## Proto / gRPC

`proto/whisper.proto` is the single source of truth. All clients must conform to it.

When changing the proto:

```bash
make proto  # regenerates proto/whisper_pb2.py and proto/whisper_pb2_grpc.py
```

Also fixes the relative import in `whisper_pb2_grpc.py` via `sed`.

## Environment Variables

Set in `docker-compose.yml` or overridden in `.env`:
- `GRPC_PORT` — default `50053`
- `WHISPER_MODEL` — default `small`
- `JOB_TTL_S` — default `7200`
- `TRANSCRIBE_QUEUE_TIMEOUT_S` — default `600`

## Common Pitfalls

- `whisper-net` Docker network must be created before `make up`
- First start downloads the model — slow, check logs with `make logs`
- `proto/whisper_pb2_grpc.py` import path is fixed by `sed` after generation — do not manually edit the import
- Port is NOT published to host — this is intentional; only containers in `whisper-net` can reach it
