-- Serialize only slot selection + leasing, never inference/download/heartbeat.
SET LOCAL statement_timeout = '3s';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '5s';
SELECT pg_advisory_xact_lock(hashtextextended('transcriber:scheduler:' || current_schema(), 0));
