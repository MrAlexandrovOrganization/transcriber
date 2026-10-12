SELECT worker_id, event, total, seconds_sum, buckets,
    EXTRACT(EPOCH FROM NOW() - last_at) AS last_event_age
FROM transcription_observations;
