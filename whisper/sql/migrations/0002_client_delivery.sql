ALTER TABLE transcription_jobs ADD COLUMN request_id TEXT NOT NULL DEFAULT '';
ALTER TABLE transcription_jobs ADD COLUMN retain_until_ack BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transcription_jobs ADD COLUMN acknowledged_at TIMESTAMPTZ;
CREATE UNIQUE INDEX idx_transcription_jobs_request_id
    ON transcription_jobs (request_id) WHERE request_id <> '';
