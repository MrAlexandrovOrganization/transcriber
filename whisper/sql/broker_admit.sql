WITH occupied AS (
    SELECT instance_id FROM worker_availability
    WHERE worker_id = %s AND ready_until > clock_timestamp()
      AND consumer_until > clock_timestamp()
    UNION
    SELECT assigned_instance_id FROM transcription_jobs
    WHERE assigned_worker_id = %s AND status = 'running'
      AND lease_expires_at > clock_timestamp()
), eligible AS (
    SELECT 1 WHERE
        EXISTS (SELECT 1 FROM occupied WHERE instance_id = %s)
        OR (SELECT COUNT(*) FROM occupied) < %s
)
UPDATE worker_availability
SET consumer_until = ready_until
WHERE worker_id = %s AND instance_id = %s AND ready_until > clock_timestamp()
  AND EXISTS (SELECT 1 FROM eligible)
RETURNING instance_id;
