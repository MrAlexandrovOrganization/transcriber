SELECT status, COUNT(*) AS jobs,
    COUNT(*) FILTER (WHERE attempts > 0) AS retried,
    COALESCE(MAX(EXTRACT(EPOCH FROM NOW() - created_at)), 0) AS oldest_seconds,
    COUNT(*) FILTER (WHERE status = 'running' AND
        (lease_expires_at IS NULL OR lease_expires_at <= NOW())) AS expired,
    COUNT(*) FILTER (WHERE retain_until_ack AND acknowledged_at IS NULL) AS unacknowledged,
    COALESCE(MAX(EXTRACT(EPOCH FROM NOW() - finished_at))
        FILTER (WHERE retain_until_ack AND acknowledged_at IS NULL), 0) AS oldest_unacknowledged_seconds
FROM transcription_jobs GROUP BY status;
