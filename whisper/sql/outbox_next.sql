SELECT message_id, job_id FROM transcription_outbox
WHERE published_at IS NULL AND available_at <= clock_timestamp()
ORDER BY available_at, message_id
FOR UPDATE SKIP LOCKED LIMIT 1;
