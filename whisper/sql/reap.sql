UPDATE transcription_jobs
SET status = 'failed', error = 'attempts_exhausted',
    lease_owner = '', lease_expires_at = NULL,
    finished_at = NOW(), updated_at = NOW()
WHERE status = 'running' AND attempts >= max_attempts
  AND (lease_expires_at IS NULL OR lease_expires_at < NOW());
