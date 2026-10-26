UPDATE transcription_outbox SET published_at = NOW() WHERE message_id = %s;
