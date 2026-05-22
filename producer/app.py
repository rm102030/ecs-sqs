import json
import uuid
import boto3

from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

from prometheus_client import (
    Counter,
    start_http_server
)

from opentelemetry import trace

from opentelemetry.trace import (
    get_current_span
)

from opentelemetry.propagate import (
    inject
)

from opentelemetry.sdk.resources import (
    Resource
)

from opentelemetry.sdk.trace import (
    TracerProvider
)

from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor
)

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter
)

# ==============================
# FastAPI
# ==============================

app = FastAPI()

# ==============================
# Prometheus Metrics
# ==============================

requests_counter = Counter(
    "notifications_requests_total",
    "Total notification requests"
)

email_counter = Counter(
    "notifications_email_total",
    "Total EMAIL notifications"
)

sms_counter = Counter(
    "notifications_sms_total",
    "Total SMS notifications"
)

push_counter = Counter(
    "notifications_push_total",
    "Total PUSH notifications"
)

error_counter = Counter(
    "notifications_error_total",
    "Total ERROR notifications"
)

# Metrics endpoint
start_http_server(8004)

print("Prometheus metrics available on port 8004")

# ==============================
# OpenTelemetry
# ==============================

resource = Resource.create({
    "service.name": "producer-service"
})

provider = TracerProvider(
    resource=resource
)

processor = BatchSpanProcessor(
    OTLPSpanExporter(
        endpoint="http://otel-collector:4317",
        insecure=True
    )
)

provider.add_span_processor(
    processor
)

trace.set_tracer_provider(
    provider
)

tracer = trace.get_tracer(
    __name__
)

# ==============================
# AWS SQS Client
# ==============================

sqs = boto3.client(

    "sqs",

    endpoint_url="http://localstack:4566",

    region_name="us-east-1",

    aws_access_key_id="test",

    aws_secret_access_key="test"
)

# ==============================
# Request Model
# ==============================

class NotificationRequest(BaseModel):

    eventId: str | None = None

    channel: str

    recipient: str

    message: str

# ==============================
# API Endpoint
# ==============================

@app.post("/notifications")

def create_notification(

    request: NotificationRequest
):

    requests_counter.inc()

    queue_name = "notifications"

    if request.channel == "ERROR":

        error_counter.inc()

        queue_name = "notifications-error"

    elif request.channel == "EMAIL":

        email_counter.inc()

    elif request.channel == "SMS":

        sms_counter.inc()

    elif request.channel == "PUSH":

        push_counter.inc()

    response = sqs.get_queue_url(
        QueueName=queue_name
    )

    queue_url = response["QueueUrl"]

    payload = {

        "eventId":
        request.eventId or str(uuid.uuid4()),

        "correlationId":
        str(uuid.uuid4()),

        "channel":
        request.channel,

        "recipient":
        request.recipient,

        "message":
        request.message,

        "createdAt":
        datetime.utcnow().isoformat()
    }

    with tracer.start_as_current_span(
        "producer-send-message"
    ):

        # =====================================
        # Trace actual
        # =====================================

        span = get_current_span()

        trace_id = format(
            span.get_span_context().trace_id,
            '032x'
        )

        print()
        print("==============================")
        print("TRACE PRODUCER")
        print("==============================")
        print(f"traceId: {trace_id}")
        print(f"eventId: {payload['eventId']}")

        # =====================================
        # OpenTelemetry Context Propagation
        # =====================================

        carrier = {}

        inject(carrier)

        print()
        print("==============================")
        print("TRACE CONTEXT")
        print("==============================")
        print(carrier)

        message_attributes = {}

        for key, value in carrier.items():

            message_attributes[key] = {

                "StringValue": value,

                "DataType": "String"
            }

        # =====================================
        # Custom traceId
        # =====================================

        message_attributes["traceId"] = {

            "StringValue": trace_id,

            "DataType": "String"
        }

        # =====================================
        # Send SQS Message
        # =====================================

        sqs.send_message(

            QueueUrl=queue_url,

            MessageBody=json.dumps(payload),

            MessageAttributes=message_attributes
        )

        print("Mensaje enviado a SQS")

    return {

        "status": "accepted",

        "queue": queue_name,

        "payload": payload,

        "traceId": trace_id
    }
