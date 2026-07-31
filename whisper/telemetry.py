"""OpenTelemetry bootstrap shared by ingress and worker processes."""

from __future__ import annotations

import os
from collections.abc import Callable

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer
from opentelemetry.instrumentation.grpc.filters import (
    any_of,
    full_method_name,
    health_check,
    negate,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def init_telemetry(service_name: str) -> Callable[[], None]:
    """Configure OTLP tracing and return a shutdown callback."""
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return lambda: None

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    GrpcInstrumentorServer(
        filter_=negate(
            any_of(
                health_check(),
                full_method_name("/whisper.TranscriptionService/GetStatus"),
            )
        )
    ).instrument()
    return provider.shutdown


def current_trace_carrier() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def context_from_carrier(traceparent: str, tracestate: str):
    carrier = {"traceparent": traceparent}
    if tracestate:
        carrier["tracestate"] = tracestate
    return propagate.extract(carrier)
