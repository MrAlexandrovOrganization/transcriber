DELETE FROM worker_availability WHERE ready_until < NOW() - INTERVAL '1 day';
