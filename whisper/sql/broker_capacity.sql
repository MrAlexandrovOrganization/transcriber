SELECT
    EXISTS (SELECT 1 FROM worker_availability
        WHERE worker_id = %s AND instance_id = %s
          AND ready_until > clock_timestamp() AND consumer_until > clock_timestamp()) AS ready,
    EXISTS (SELECT 1 FROM transcription_jobs WHERE status = 'running'
        AND assigned_worker_id = %s AND assigned_instance_id = %s
        AND lease_expires_at > clock_timestamp()) AS busy,
    (SELECT COUNT(*) FROM transcription_jobs WHERE status = 'running'
        AND assigned_worker_id = %s AND lease_expires_at > clock_timestamp()) AS active;
