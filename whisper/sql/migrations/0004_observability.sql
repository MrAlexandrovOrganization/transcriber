-- Transactional observations survive process restarts and job TTL cleanup.
-- Existing jobs are not backfilled: observation history starts at migration.
ALTER TABLE transcription_jobs ADD COLUMN attempt_started_at TIMESTAMPTZ;

-- Keep the last report even when obsolete per-process instances are cleaned up.
CREATE TABLE transcription_worker_reports (
    worker_id TEXT PRIMARY KEY,
    reported_at TIMESTAMPTZ NOT NULL
);
INSERT INTO transcription_worker_reports
SELECT worker_id, MAX(reported_at) FROM worker_availability GROUP BY worker_id;

CREATE TABLE transcription_observations (
    worker_id TEXT NOT NULL,
    event TEXT NOT NULL,
    total BIGINT NOT NULL DEFAULT 0,
    seconds_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    buckets BIGINT[] NOT NULL,
    last_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (worker_id, event)
);

CREATE FUNCTION transcriber_observe(w TEXT, e TEXT, seconds DOUBLE PRECISION DEFAULT 0)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    increments BIGINT[];
BEGIN
    seconds := GREATEST(0, seconds);
    SELECT ARRAY_AGG(CASE WHEN seconds <= bound THEN 1::BIGINT ELSE 0::BIGINT END ORDER BY ordinal)
    INTO increments FROM UNNEST(ARRAY[1,5,15,30,60,120,300,600,1800,3600,10800])
        WITH ORDINALITY AS limits(bound, ordinal);
    INSERT INTO transcription_observations AS observations
        (worker_id, event, total, seconds_sum, buckets)
    VALUES (COALESCE(NULLIF(w, ''), 'ingress'), e, 1, seconds, increments)
    ON CONFLICT (worker_id, event) DO UPDATE SET
        total = observations.total + 1,
        seconds_sum = observations.seconds_sum + EXCLUDED.seconds_sum,
        buckets = ARRAY(SELECT observations.buckets[i] + EXCLUDED.buckets[i]
            FROM GENERATE_SUBSCRIPTS(observations.buckets, 1) AS i ORDER BY i),
        last_at = NOW();
END;
$$;

CREATE FUNCTION transcriber_observe_job() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    previous_worker TEXT;
    next_worker TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM transcriber_observe('ingress', 'submitted');
        RETURN NEW;
    END IF;
    previous_worker := SPLIT_PART(OLD.lease_owner, '/', 1);
    next_worker := SPLIT_PART(NEW.lease_owner, '/', 1);
    IF NEW.attempts > OLD.attempts THEN
        NEW.attempt_started_at := NOW();
        PERFORM transcriber_observe(next_worker, 'attempts');
        IF NEW.attempts > 1 THEN
            PERFORM transcriber_observe(next_worker, 'retries');
        ELSE
            PERFORM transcriber_observe(next_worker, 'queue_wait',
                EXTRACT(EPOCH FROM NOW() - NEW.created_at)::DOUBLE PRECISION);
        END IF;
    END IF;
    IF OLD.status = 'running' AND
       (NEW.attempts > OLD.attempts OR
        (NEW.status = 'failed' AND NEW.error = 'attempts_exhausted')) THEN
        PERFORM transcriber_observe(previous_worker, 'lease_expirations');
    END IF;
    IF OLD.status = 'running' AND NEW.status IN ('queued', 'failed')
       AND NEW.error <> 'cancelled' THEN
        PERFORM transcriber_observe(previous_worker, 'attempt_failures');
    END IF;
    IF NEW.status = 'done' AND OLD.status <> 'done' THEN
        PERFORM transcriber_observe(previous_worker, 'completed');
        IF OLD.attempt_started_at IS NOT NULL THEN
            PERFORM transcriber_observe(previous_worker, 'processing',
                EXTRACT(EPOCH FROM NOW() - OLD.attempt_started_at)::DOUBLE PRECISION);
        END IF;
        PERFORM transcriber_observe(previous_worker, 'turnaround',
            EXTRACT(EPOCH FROM NOW() - NEW.created_at)::DOUBLE PRECISION);
    ELSIF NEW.status = 'failed' AND OLD.status NOT IN ('done', 'failed') THEN
        PERFORM transcriber_observe(previous_worker,
            CASE WHEN NEW.error = 'cancelled' THEN 'cancelled' ELSE 'failed' END);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER transcription_job_observation
BEFORE INSERT OR UPDATE OF status, attempts ON transcription_jobs
FOR EACH ROW EXECUTE FUNCTION transcriber_observe_job();
