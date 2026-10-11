UPDATE transcription_jobs
SET lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
    progress_percent = %s, updated_at = NOW()
WHERE job_id = %s AND lease_owner = %s
  AND status = 'running' AND lease_expires_at > NOW();
