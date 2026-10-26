SELECT COUNT(*) AS pending,
    COALESCE(MAX(EXTRACT(EPOCH FROM NOW() - available_at)), 0) AS oldest_seconds
FROM transcription_outbox WHERE %s AND published_at IS NULL AND available_at <= NOW();
