INSERT INTO worker_availability (worker_id, instance_id, ready_until, reported_at)
VALUES (%s, %s, NOW() + (%s * INTERVAL '1 second'), NOW())
ON CONFLICT (worker_id, instance_id) DO UPDATE
SET ready_until = EXCLUDED.ready_until, reported_at = EXCLUDED.reported_at;
