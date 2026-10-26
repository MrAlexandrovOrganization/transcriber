SELECT *, lease_expires_at > clock_timestamp() AS live
FROM transcription_jobs WHERE job_id = %s FOR UPDATE;
