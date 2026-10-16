WITH policies AS (
    SELECT key AS worker_id, (value->>'priority')::INTEGER AS priority,
        (value->>'max_concurrency')::INTEGER AS capacity
    FROM jsonb_each(%s::JSONB)
), active AS (
    SELECT assigned_worker_id AS worker_id, assigned_instance_id AS instance_id
    FROM transcription_jobs
    WHERE status = 'running' AND lease_expires_at > clock_timestamp()
), eligible AS (
    SELECT availability.worker_id, availability.instance_id, policies.priority
    FROM worker_availability AS availability
    JOIN policies USING (worker_id)
    WHERE ready_until > clock_timestamp() AND demand_until > clock_timestamp()
      AND NOT EXISTS (
          SELECT 1 FROM active
          WHERE active.worker_id = availability.worker_id
            -- Old leases have no slot UUID: conservatively hold the whole pool
            -- until they complete or expire during a rolling upgrade.
            AND (active.instance_id = availability.instance_id OR active.instance_id = '')
      )
      AND (SELECT COUNT(*) FROM active WHERE active.worker_id = availability.worker_id) < policies.capacity
)
SELECT * FROM eligible WHERE priority = (SELECT MIN(priority) FROM eligible)
    AND worker_id = %s AND instance_id = %s;
