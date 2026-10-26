WITH availability AS (
    SELECT worker_id, BOOL_OR(ready_until > NOW()) AS online,
        COUNT(*) FILTER (WHERE ready_until > NOW()) AS instances,
        COUNT(*) FILTER (WHERE ready_until > NOW() AND (demand_until > NOW() OR consumer_until > NOW())
            AND NOT EXISTS (
                SELECT 1 FROM transcription_jobs AS jobs
                WHERE jobs.status = 'running' AND jobs.lease_expires_at > NOW()
                  AND jobs.assigned_worker_id = worker_availability.worker_id
                  AND (jobs.assigned_instance_id = worker_availability.instance_id OR jobs.assigned_instance_id = '')
            )) AS idle_slots
    FROM worker_availability GROUP BY worker_id
)
SELECT reports.worker_id, COALESCE(availability.online, FALSE) AS online,
    COALESCE(availability.instances, 0) AS instances,
    COALESCE(availability.idle_slots, 0) AS idle_slots,
    EXTRACT(EPOCH FROM NOW() - reports.reported_at) AS last_report_age
FROM transcription_worker_reports AS reports
LEFT JOIN availability USING (worker_id);
