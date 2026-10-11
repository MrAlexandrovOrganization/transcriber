SELECT * FROM transcription_jobs
WHERE job_id = %s AND lease_owner = %s
  AND status = 'running' AND lease_expires_at > NOW();
