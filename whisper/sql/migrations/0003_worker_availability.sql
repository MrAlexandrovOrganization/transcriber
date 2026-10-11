CREATE TABLE worker_availability (
    worker_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    ready_until TIMESTAMPTZ NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (worker_id, instance_id)
);
CREATE INDEX worker_availability_ready_until_idx ON worker_availability (ready_until);
