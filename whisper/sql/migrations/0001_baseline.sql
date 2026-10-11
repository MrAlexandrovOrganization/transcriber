-- Adopt the existing startup-managed schema without replacing existing rows.
CREATE TABLE IF NOT EXISTS transcription_jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    format TEXT NOT NULL,
    preset TEXT NOT NULL,
    language_override TEXT NOT NULL DEFAULT '',
    initial_prompt_override TEXT NOT NULL DEFAULT '',
    input_path TEXT NOT NULL,
    result_path TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    progress_percent REAL NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    text TEXT NOT NULL DEFAULT '',
    segments_json JSONB NOT NULL DEFAULT '[]'::jsonb
);
ALTER TABLE transcription_jobs ADD COLUMN IF NOT EXISTS traceparent TEXT NOT NULL DEFAULT '';
ALTER TABLE transcription_jobs ADD COLUMN IF NOT EXISTS tracestate TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_claim
    ON transcription_jobs (status, lease_expires_at, created_at);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_updated_at
    ON transcription_jobs (updated_at);
