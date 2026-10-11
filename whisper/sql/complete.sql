UPDATE transcription_jobs
SET status = 'done', text = %s, segments_json = %s, result_path = %s,
    progress_percent = 100, error = '', lease_owner = '', lease_expires_at = NULL,
    finished_at = NOW(), updated_at = NOW()
WHERE job_id = %s AND lease_owner = %s
  AND status = 'running' AND lease_expires_at > NOW();
