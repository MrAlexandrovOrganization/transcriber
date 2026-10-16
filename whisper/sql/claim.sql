WITH candidate AS (
    SELECT job_id
    FROM transcription_jobs
    WHERE status IN ('queued', 'running')
      AND (NOT %s OR NOT EXISTS (
          SELECT 1 FROM worker_availability
          WHERE worker_id = ANY(%s) AND ready_until > NOW()
      ))
      AND (NOT (%s = ANY(%s)) OR EXISTS (
          SELECT 1 FROM worker_availability
          WHERE worker_id = %s AND instance_id = %s AND ready_until > NOW()
      ))
      AND attempts < max_attempts
      AND (status = 'queued' OR lease_expires_at IS NULL OR lease_expires_at < NOW())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE transcription_jobs AS jobs
SET status = 'running', attempts = jobs.attempts + 1,
    lease_owner = %s,
    lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
    assigned_worker_id = %s, assigned_instance_id = %s,
    started_at = COALESCE(jobs.started_at, NOW()), updated_at = NOW(), error = ''
FROM candidate
WHERE jobs.job_id = candidate.job_id
RETURNING jobs.*;
