# Transcriber backend

Durable asynchronous Whisper transcription service.

## Architecture

- [`whisper-ingress`](docker-compose.yml) accepts gRPC uploads and returns a `job_id` quickly.
- Uploaded files are persisted to [`/data/jobs`](docker-compose.yml) on a Docker volume.
- Job metadata, status, retries, and worker leases are stored in Postgres.
- [`whisper-worker`](docker-compose.yml) claims jobs via Postgres using `FOR UPDATE SKIP LOCKED`.
- A job is acknowledged only after the worker stores the final result and marks it `done`.
- If a worker dies before lease renewal, the job becomes claimable again and is retried.

## Local run

```bash
make proto
make up
```

Ingress endpoint inside Docker network: `whisper-ingress:50053`.

## Main environment variables

- `DATABASE_URL`
- `JOB_STORAGE_DIR`
- `JOB_MAX_ATTEMPTS`
- `JOB_LEASE_SECONDS`
- `JOB_TTL_S`
- `SERVICE_ROLE=ingress|worker`
- `WORKER_POLL_INTERVAL_S`
- `WHISPER_MODEL`
- `WHISPER_CPU_THREADS`
