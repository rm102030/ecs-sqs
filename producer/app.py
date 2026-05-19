import json
import uuid
import boto3

from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ==============================
# FastAPI
# ==============================

app = FastAPI()

# ==============================
# OpenTelemetry
# ==============================

resource = Resource.create({
    "service.name": "producer-service"
})

provider = TracerProvider(resource=resource)

processor = BatchSpanProcessor(
    OTLPSpanExporter(
        endpoint="http://otel-collector:4317",
        insecure=True
    )
)

provider.add_span_processor(processor)

trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

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

    queue_name = "notifications"

    if request.channel == "ERROR":

        queue_name = "notifications-error"

    response = sqs.get_queue_url(
        QueueName=queue_name
    )

    queue_url = response["QueueUrl"]

    payload = {
        "eventId": request.eventId or str(uuid.uuid4()),
        "correlationId": str(uuid.uuid4()),
        "channel": request.channel,
        "recipient": request.recipient,
        "message": request.message,
        "createdAt": datetime.utcnow().isoformat()
    }

    with tracer.start_as_current_span(
        "producer-send-message"
    ):

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload)
        )

    return {
        "status": "accepted",
        "queue": queue_name,
        "payload": payload
    }
