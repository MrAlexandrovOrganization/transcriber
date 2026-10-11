UPDATE transcription_jobs
SET acknowledged_at = COALESCE(acknowledged_at, NOW()), updated_at = NOW()
WHERE job_id = %s AND status IN ('done', 'failed')
RETURNING job_id;
