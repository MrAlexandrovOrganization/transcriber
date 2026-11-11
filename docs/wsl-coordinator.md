# Russia router and WSL coordinator

## Boundaries

Russia remains the stable entry point. The router forwards client gRPC on
50053, worker gRPC on its dedicated Docker network at 50056, and AMQPS on
5671. Existing client addresses, worker TLS hostname, bearer registries,
RabbitMQ certificate, users and permissions are preserved. The public Caddy
route still exposes only worker RPCs; the client API and AMQPS bind only to
the existing private tailnet address. No host routes or DNS records change.

HAProxy uses two-second connection deadlines, no retries and no durable queue.
Its TCP checks also close existing sessions after the upstream becomes unhealthy;
an already-open channel must not hide a dead WSL indefinitely. Detection depends
on the two-check window and network latency (normally a few seconds).
Long healthy streams retain an eight-hour idle budget. The router's `/health`
reports its own liveness even when the backend is down; `/metrics` exposes proxy
health separately. It cannot keep transcribing while the coordinator is offline.

WSL has a separate `transcriber-coordinator` checkout, Compose project, persistent
tailnet identity and volumes. The existing CPU/GPU worker checkout and worker IDs
remain independent. The sidecar joins the local whisper/Prometheus/Jaeger Docker
networks; ingress and RabbitMQ share its network namespace. No new public or
Windows host ports are published. Inside this namespace, raw-table rules accept
the private API/AMQPS from Russia's configured IPv4 address only and block these
ports over tailnet IPv6; Docker-local traffic and administrative SSH are unaffected.
The firewall does not modify the WSL host or other Docker namespaces.

## Availability and accepted work

`REQUIRE_AVAILABLE_WORKERS=1` rejects a new upload with gRPC `UNAVAILABLE` when no
enabled worker has a fresh ready report. Busy workers count as online: spare
capacity and outages are distinct. No database row or audio file is created for
that rejected upload. The coordinator example uses a 30-second worker TTL;
graceful withdrawal is visible immediately, abrupt worker loss after expiry.
The existing 45-second report-age ceiling remains an additional bound.

Idempotent replay of an already-accepted upload and GetStatus/Cancel/Ack remain
available without inference workers. Already accepted jobs, retained results,
outbox records and audio remain durable. A worker can disappear after admission;
the existing leases and retries still apply. Router outages are not a reason to
delete accepted work or turn on inference on Russia.

The Telegram frontend's existing retry and quiet-group policies remain unchanged;
an API outage does not introduce unsolicited Telegram messages.

## Provisioning and delivery

Choose the router role with `.env.router.example`. In a separate WSL checkout,
use `.env.coordinator.example`, existing worker policies and the protected copies
of the credential registry and broker bundle. Keep the original RabbitMQ hostname
and cookie. `POSTGRES_IMAGE` and `RABBITMQ_IMAGE` optionally select immutable
stateful image references; pin them to the source digests for migration. Defaults
still resolve from `makefiles/versions.mk`.

Provision the coordinator with a single-use Headscale key for the existing owner.
Keep its state volume and the protected registration file. Start only the sidecar
before restoring data. No worker is registered or enabled by this step.
If the sidecar is recreated later, recreate ingress and RabbitMQ with it so that
their shared network namespace is current. A container-only restart retains the
namespace. Ordinary `make image-deploy` uses Compose dependency restart semantics.

CI first checks code/configuration and the real isolated broker/router fixtures,
then publishes the backend digest. With `COORDINATOR_DEPLOY_ENABLED=true`, it
deploys the same checked SHA/digest to WSL through Russia before updating the
Russia router. A failed coordinator deployment blocks router deployment.
Push and explicit workflow_dispatch use the same path. Worker release approval
still waits for successful application delivery and both worker architectures.

Coordinator SSH deployment uses these repository secrets:

- `COORDINATOR_TRANSPORT_JSON`: `jump` and `target`, each with host/port/user.
- `COORDINATOR_SSH_KEY`: dedicated Ed25519 key, never an agent-forwarded key.
- `COORDINATOR_KNOWN_HOSTS`: pinned `transcriber-jump` and `transcriber-target` keys.

The jump key permits only forwarding to WSL SSH. On WSL its forced command runs
the explicitly installed `scripts/coordinator_receiver.py` outside the checkout.
The receiver accepts only this project's latest main SHA and existing immutable
image manifest contract. Registry credentials travel in SSH stdin, not arguments
or files retained on either host. The previous Russia deployment key stays in use
for that router checkout. Receiver bootstrap/update is an explicit administrator
operation; a normal application release cannot replace the installed receiver.
This scoped SSH client uses the tested Curve25519 key exchange: the default
larger hybrid exchange stalled on the jump path. Strict host-key verification
and pinned host keys remain enabled on both connections.

## Consistent cutover

1. Prepare empty destination volumes and confirm disk/RAM headroom. Back up
   private configuration and take an initial logical PostgreSQL dump off Russia.
2. Complete/drain active jobs. Stop ingress so uploads, cleanup and publications
   cannot change storage; stop the broker and PostgreSQL after a final logical
   dump. Disable restart of these source containers and preserve all volumes.
3. Copy the final PostgreSQL dump, complete job-volume archive and cold RabbitMQ
   data archive, including cookie, under private permissions. Keep an independent
   off-source backup. Restore PostgreSQL with its original ownership/schema and
   restore all broker/audio bytes with original ownership before application start.
4. Compare every public table's row count and ordered content digest, sequence
   state and every broker/audio file's SHA-256 before any destination writers start.
   Restore the same broker image digest and hostname, not a fresh empty broker.
5. Start the destination broker and ingress from the checked digest. Keep source
   writers stopped. Switch Russia to router role without changing client addresses
   or worker broker files, and update CI delivery before leaving the migration.
6. Verify consumer authentication, broker reconnection and an end-to-end synthetic
   upload/GetStatus/Ack through Russia. Exercise no-worker admission, broken WSL
   routing on a scoped test, and recovery. Check monitoring and the bot's database.

The Telegram bot and its database are not part of this storage move. Do not start
a duplicate polling bot or replace its database with a backend database dump.

## Rollback

Before WSL accepts writes, stop its application/broker and restore the original
Russia configuration, restart policies and network aliases. Stop the router before
starting the source listeners. Retain the destination volumes for inspection.

After WSL accepts writes, the old Russia volumes are stale. First freeze the
destination, export its latest PostgreSQL, RabbitMQ and audio together, restore
those to the chosen source, and validate before reactivating writers. Never switch
back to the original cold volumes over newer accepted work. Keep the same
credentials, worker identities, lease fencing and published image for rollback.

## Checks

`make check` validates both role examples, proxy syntax and admission code.
`make test-router-docker` uses only named disposable fixtures and synthetic TCP
streams to check forwarding, existing-channel failure bounds, router liveness and
recovery. PostgreSQL integration tests check rejection without orphan jobs and
idempotent replay/status/cancel/Ack while no worker is online. The normal broker
suite still tests durable delivery, TLS, permissions, leases and priorities.
