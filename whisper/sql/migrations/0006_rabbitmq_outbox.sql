-- RabbitMQ owns delivery; PostgreSQL retains durable submission and attempt fencing.
ALTER TABLE transcription_jobs
    ADD COLUMN dispatch_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN delivery_id TEXT NOT NULL DEFAULT '';
ALTER TABLE worker_availability
    ADD COLUMN consumer_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity';

CREATE TABLE transcription_outbox (
    message_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES transcription_jobs(job_id) ON DELETE CASCADE,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX transcription_outbox_pending_idx
    ON transcription_outbox (available_at) WHERE published_at IS NULL;

-- Adopt waiting and in-flight jobs without losing audio, results or request IDs.
-- A delivery for an old running attempt waits until completion or lease expiry.
UPDATE transcription_jobs SET dispatch_id = gen_random_uuid()::TEXT
WHERE status IN ('queued', 'running');
INSERT INTO transcription_outbox (message_id, job_id)
SELECT dispatch_id, job_id FROM transcription_jobs WHERE dispatch_id <> '';
