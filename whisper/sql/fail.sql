UPDATE transcription_jobs
SET status = CASE WHEN %s AND attempts < max_attempts THEN 'queued' ELSE 'failed' END,
    error = %s, progress_percent = 0, lease_owner = '', lease_expires_at = NULL,
    finished_at = CASE WHEN %s AND attempts < max_attempts THEN NULL ELSE NOW() END,
    updated_at = NOW()
WHERE job_id = %s AND lease_owner = %s
  AND ((status = 'running' AND lease_expires_at > NOW())
       OR (%s = '' AND status IN ('accepted', 'downloading')));
