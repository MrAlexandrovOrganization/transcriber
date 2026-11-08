# Optional client availability checks

`TranscriptionService.GetAvailability` is an advisory, read-only preflight for
clients that prefer to skip new work while inference is offline. `available`
requires a ready report newer than 45 seconds from an enabled pool; RabbitMQ
mode also requires a valid consumer admission. Busy slots count as available.
Disabled/unlisted pools do not count when operator policies are configured.

This does not reserve a worker, probe model inference or guarantee availability
after upload. Reports normally refresh every 20 seconds, so an abrupt outage
can take up to 45 seconds to appear. Worker readiness TTL and existing job
leases are unchanged. `Submit` continues accepting durable jobs without workers
for existing clients. Deploy the backend before clients that require this RPC.
