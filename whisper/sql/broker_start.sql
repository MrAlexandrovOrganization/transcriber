UPDATE transcription_jobs
SET status = 'running', attempts = attempts + 1,
    lease_owner = %s, lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
    assigned_worker_id = %s, assigned_instance_id = %s, delivery_id = %s,
    started_at = COALESCE(started_at, NOW()), updated_at = NOW(),
    progress_percent = 0, error = ''
WHERE job_id = %s RETURNING *;
