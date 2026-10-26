"""Explicitly republish unfinished work after rollback or broker recovery."""

from whisper.storage import JobStore, _sql


def main():
    store = JobStore()
    if store.queue_backend != "rabbitmq":
        raise ValueError("Reconciliation requires RabbitMQ mode")
    store.init_schema()
    with store.connect() as connection:
        connection.execute(_sql("scheduler_lock"))
        connection.execute(_sql("broker_reconcile"))
        connection.execute(_sql("broker_seed"))
    print("Unfinished jobs scheduled for confirmed republication")


if __name__ == "__main__":
    main()
