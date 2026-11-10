"""JSON logging setup compatible with slog JSONHandler output."""

from __future__ import annotations

import datetime
import json
import logging
import os

import grpc
from opentelemetry import trace

SERVICE_NAME = "whisper"

_ROLE = os.getenv("SERVICE_ROLE", "ingress").strip().lower() or "ingress"

_VALID_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        payload: dict[str, object] = {
            "time": datetime.datetime.fromtimestamp(
                record.created, tz=datetime.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": message,
            "service": SERVICE_NAME,
            "role": _ROLE,
        }
        # Only application-owned diagnostic fields are copied. Exception text
        # can contain credentials, signed URLs or input, so failure_fields omits it.
        for name in ("error_type", "rpc_code", "retry_attempt", "retry_in_seconds"):
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            payload["trace_id"] = format(span_context.trace_id, "032x")
            payload["span_id"] = format(span_context.span_id, "016x")
            payload["trace_flags"] = format(int(span_context.trace_flags), "02x")
        return json.dumps(payload, ensure_ascii=False)


def failure_fields(error: Exception, **retry) -> dict[str, object]:
    fields: dict[str, object] = {"error_type": type(error).__name__}
    if isinstance(error, grpc.RpcError):
        code = error.code() if callable(getattr(error, "code", None)) else None
        if isinstance(code, grpc.StatusCode):
            fields["rpc_code"] = code.name
    for name in ("retry_attempt", "retry_in_seconds"):
        if name in retry:
            fields[name] = retry[name]
    return fields


def _resolve_level() -> int:
    raw = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    if raw not in _VALID_LEVELS:
        raw = "INFO"
    return logging.getLevelName(raw)


def setup_logging(service_role: str | None = None) -> None:
    """Configure root logger to emit one JSON object per line to stdout."""
    global _ROLE
    from logging.config import dictConfig

    if service_role:
        _ROLE = service_role.strip().lower() or _ROLE

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {"()": JSONFormatter},
            },
            "handlers": {
                "stdout": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "json",
                    "level": "NOTSET",
                },
            },
            "root": {
                "handlers": ["stdout"],
                "level": _resolve_level(),
            },
        }
    )
