-- Explicit recovery after a SQL-mode rollback or broker storage restoration.
-- Never invalidate a currently live execution attempt.
UPDATE transcription_jobs SET dispatch_id = ''
WHERE status = 'queued' OR (status = 'running' AND
    (lease_expires_at IS NULL OR lease_expires_at < clock_timestamp()));
