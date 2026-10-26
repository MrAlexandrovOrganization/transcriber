WITH adopted AS (
    UPDATE transcription_jobs SET dispatch_id = gen_random_uuid()::TEXT
    WHERE status IN ('queued', 'running') AND dispatch_id = ''
    RETURNING dispatch_id, job_id, created_at
)
INSERT INTO transcription_outbox (message_id, job_id, available_at)
SELECT dispatch_id, job_id, created_at FROM adopted;
