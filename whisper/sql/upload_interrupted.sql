UPDATE transcription_jobs
SET error = 'upload_interrupted', updated_at = NOW()
WHERE job_id = %s AND status IN ('accepted', 'downloading');
