# Transcriber context

Read README.md and docs/distributed-workers.md for current commands, constraints
and the verified runtime audit. This repository owns proto/whisper.proto.

- Python/Poetry versions and image/tool versions: versions.mk; dependencies and
  grpcio-tools generator: pyproject.toml/poetry.lock. Do not hand-edit generated
  proto/*_pb2*.py or .pyi files; `make proto-gen` regenerates ignored artifacts.
- Ingress accepts streaming Submit and exposes GetStatus/Cancel. PostgreSQL
  holds job metadata/results/leases; the ingress filesystem holds uploaded audio.
- Local worker uses JobStore; optional remote workers use a separate authenticated
  WorkerService. Ordinary deployment does not publish a worker endpoint.
- whisper/inference.py is shared CPU/CUDA inference. Mac CPU works; Metal is a
  future backend. whisper/remote_worker.py is a native pull process for Mac/WSL.
- Every attempt gets a unique token; SQL mutations require current token, running
  status and live lease. Heartbeats run independently from yielded segments.
- Cleanup belongs to ingress. Waiting jobs are not expired by finished-job TTL;
  completed results are currently retained only JOB_TTL_S (default 2 hours).
- notes-bot frontend delivery is not durable yet. Do not disable its permanent
  worker or claim safe overnight delivery until that consumer is adapted.

`make install`, `make format`, `make check`, `make test-integration`, `make build`
are the shared developer/CI entry points. Integration DB must be explicitly
isolated; tests never contact Telegram or production. `make pilot
PILOT_AUDIO_FILE=/path/to/synthetic.wav` uses temporary Docker/PostgreSQL and a
cached CPU model, without model/image downloads or production tasks.

Keep token files out of Git/logs/command arguments. Remote plaintext is loopback
through SSH or an explicitly trusted VPN; the built-in worker listener has no
TLS. No changes to production services as a side effect of testing.
