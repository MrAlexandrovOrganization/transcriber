-- A slot is a fresh UUID per execution loop, including loops sharing a model.
ALTER TABLE transcription_jobs
    ADD COLUMN assigned_worker_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN assigned_instance_id TEXT NOT NULL DEFAULT '';
UPDATE transcription_jobs SET assigned_worker_id = SPLIT_PART(lease_owner, '/', 1)
WHERE status = 'running';
CREATE INDEX transcription_jobs_active_slot_idx
    ON transcription_jobs (assigned_worker_id, assigned_instance_id)
    WHERE status = 'running';

-- Availability reporters must not indefinitely reserve work for a stuck loop.
ALTER TABLE worker_availability
    ADD COLUMN demand_until TIMESTAMPTZ NOT NULL DEFAULT NOW();
