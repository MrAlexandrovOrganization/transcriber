WITH candidate AS (
    SELECT job_id FROM transcription_jobs
    WHERE status IN ('queued', 'running') AND attempts < max_attempts
      AND (status = 'queued' OR lease_expires_at IS NULL OR lease_expires_at < clock_timestamp())
    ORDER BY created_at, job_id
    FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE transcription_jobs AS jobs
SET status = 'running', attempts = jobs.attempts + 1,
    lease_owner = %s, lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
    assigned_worker_id = %s, assigned_instance_id = %s,
    started_at = COALESCE(jobs.started_at, NOW()), updated_at = NOW(), error = ''
FROM candidate WHERE jobs.job_id = candidate.job_id RETURNING jobs.*;
