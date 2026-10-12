SELECT SPLIT_PART(lease_owner, '/', 1) AS worker_id, COUNT(*) AS active,
    AVG(progress_percent) AS progress,
    MAX(EXTRACT(EPOCH FROM NOW() - COALESCE(attempt_started_at, started_at))) AS attempt_age
FROM transcription_jobs WHERE status = 'running' AND lease_expires_at > NOW()
GROUP BY SPLIT_PART(lease_owner, '/', 1);
