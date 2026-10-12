"""Install only this project's monitoring files into the shared file providers."""

import os
import tempfile
from pathlib import Path


def install(root: Path):
    source = Path(__file__).resolve().parent.parent / "monitoring"
    destinations = [
        (
            source / "prometheus/transcriber.yml",
            root / "prometheus-targets/transcriber.yml",
        ),
        *[
            (path, root / "grafana-dashboards/Transcription" / path.name)
            for path in sorted((source / "grafana").glob("*.json"))
        ],
    ]
    for path, target in destinations:
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=".transcriber-", dir=target.parent
        )
        try:
            with os.fdopen(descriptor, "wb") as file:
                file.write(path.read_bytes())
                file.flush()
                os.fsync(file.fileno())
                os.fchmod(file.fileno(), 0o644)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
        print("Installed " + target.name)


if __name__ == "__main__":
    install(Path(os.environ["MONITORING_DATA_DIR"]).expanduser())
