DELETE FROM transcription_jobs
WHERE finished_at IS NOT NULL
  AND ((NOT retain_until_ack AND finished_at < NOW() - (%s * INTERVAL '1 second'))
       OR (retain_until_ack AND acknowledged_at IS NOT NULL
           AND acknowledged_at < NOW() - (%s * INTERVAL '1 second')))
RETURNING input_path, result_path;
