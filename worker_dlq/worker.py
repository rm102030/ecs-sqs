import json
import time
import boto3

from datetime import datetime

from prometheus_client import (
    Counter,
    start_http_server
)

from opentelemetry import trace

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter
)

# =========================================================
# Prometheus Metrics
# =========================================================

dlq_counter = Counter(
    "notifications_dlq_processed_total",
    "Total DLQ processed notifications"
)

worker_dlq_error_counter = Counter(
    "notifications_worker_dlq_error_total",
    "Total worker DLQ internal errors"
)

# Metrics endpoint
start_http_server(8005)

print("Prometheus metrics available on port 8005")

# =========================================================
# OpenTelemetry
# =========================================================

resource = Resource.create({
    "service.name": "worker-dlq" })
