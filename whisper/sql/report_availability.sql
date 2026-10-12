WITH report AS (
    INSERT INTO worker_availability (worker_id, instance_id, ready_until, reported_at)
    VALUES (%s, %s, NOW() + (%s * INTERVAL '1 second'), NOW())
    ON CONFLICT (worker_id, instance_id) DO UPDATE
    SET ready_until = EXCLUDED.ready_until, reported_at = EXCLUDED.reported_at
    RETURNING worker_id, reported_at
)
INSERT INTO transcription_worker_reports SELECT worker_id, reported_at FROM report
ON CONFLICT (worker_id) DO UPDATE
SET reported_at = GREATEST(transcription_worker_reports.reported_at, EXCLUDED.reported_at);
