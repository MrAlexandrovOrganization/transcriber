WITH availability AS (
    SELECT worker_id, BOOL_OR(ready_until > NOW()) AS online,
        COUNT(*) FILTER (WHERE ready_until > NOW()) AS instances
    FROM worker_availability GROUP BY worker_id
)
SELECT reports.worker_id, COALESCE(availability.online, FALSE) AS online,
    COALESCE(availability.instances, 0) AS instances,
    EXTRACT(EPOCH FROM NOW() - reports.reported_at) AS last_report_age
FROM transcription_worker_reports AS reports
LEFT JOIN availability USING (worker_id);
