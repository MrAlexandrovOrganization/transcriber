# Transcriber context

Read README.md, docs/container-workers.md, docs/worker-tls.md and
docs/worker-autodeploy.md for the supported lifecycle. This repository owns
proto/whisper.proto. docs/distributed-workers.md is a historical audit.

- Versions: makefiles/versions.mk; dependencies and grpcio-tools generator:
  pyproject.toml/poetry.lock. Generated proto files are ignored; use make proto-gen.
- Compose files live in compose/. The root Makefile anchors their paths to the
  checkout root. Machine .env selects server/workers, CPU/GPU profiles and models.
- PostgreSQL holds durable job metadata/results/leases. Ingress stores audio on
  its volume. Retained submissions remain until clients durably receive and Ack.
- Remote workers use Caddy gRPC/TLS and individual tokens bound to exact worker
  IDs. No SSH transport, shared-token listener or native machine launcher remains.
  Internal Caddy-to-ingress h2c stays isolated on caddy-transcriber-net.
- Loopback plaintext with scoped credentials is only for isolated tests/pilots;
  remote worker CLI defaults to TLS. Never log credentials or raw configuration.
- Mac uses ARM64 CPU inference; WSL supports separate CPU/CUDA containers.
  Each attempt has a lease token; stale owners cannot update or complete jobs.
- Preferred-worker availability blocks server fallback, including while busy.
  Image updates drain workers and restore predecessors on failed readiness.
- make install, format, check, build and config-check are developer/CI entry points.
  Integration DB and audio fixtures must be synthetic and isolated. Tests do not
  contact production/Telegram or download models by default.
- Deploy through Git/CI. Bootstrap/Compose changes also require updating Mac/WSL
  checkouts and running make up after draining, preserving all persistent volumes.
