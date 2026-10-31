"""Shared wire limits for the remote transcription worker protocol."""

MAX_RESULT_BYTES = 4 * 1024 * 1024


# One default for ingress and native/container remote workers.
DEFAULT_MAX_AUDIO_BYTES = 256 * 1024 * 1024


def audio_limit() -> int:
    import os

    try:
        value = int(os.getenv("MAX_AUDIO_BYTES", str(DEFAULT_MAX_AUDIO_BYTES)))
    except ValueError:
        raise ValueError("MAX_AUDIO_BYTES must be a positive integer") from None
    if value <= 0:
        raise ValueError("MAX_AUDIO_BYTES must be a positive integer")
    return value
