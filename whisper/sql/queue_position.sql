SELECT COUNT(*) FROM transcription_jobs
WHERE status IN ('queued', 'running')
  AND created_at <= (SELECT created_at FROM transcription_jobs WHERE job_id = %s);
