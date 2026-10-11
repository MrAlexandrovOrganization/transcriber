"""Explicitly apply the same ordered SQL migrations used by service startup."""

from whisper.storage import JobStore


def main() -> int:
    try:
        JobStore().init_schema()
    except Exception:
        print("Schema migration failed; check database configuration and permissions")
        return 1
    print("Transcriber schema migrations applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
