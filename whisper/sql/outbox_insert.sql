INSERT INTO transcription_outbox (message_id, job_id, available_at)
VALUES (%s, %s, clock_timestamp() + (%s * INTERVAL '1 second'));
