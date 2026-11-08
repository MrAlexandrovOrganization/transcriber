SELECT EXISTS (
    SELECT 1 FROM worker_availability
    WHERE ready_until > NOW()
      AND reported_at > NOW() - INTERVAL '45 seconds'
      AND (NOT %s OR consumer_until > NOW())
      AND (%s::jsonb = '{}'::jsonb
          OR COALESCE((%s::jsonb -> worker_id ->> 'max_concurrency')::integer, 0) > 0)
) AS available;
