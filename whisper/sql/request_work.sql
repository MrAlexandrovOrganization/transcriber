UPDATE worker_availability
SET demand_until = clock_timestamp() + (%s * INTERVAL '1 second')
WHERE worker_id = %s AND instance_id = %s AND ready_until > clock_timestamp();
