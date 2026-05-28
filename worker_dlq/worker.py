
import json
import time
import boto3
import socket

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
# Worker Metadata
# =========================================================

WORKER_ID = socket.gethostname()

print(f"Worker DLQ ID: {WORKER_ID}")

# =========================================================
# Prometheus Metrics
# =========================================================

dlq_counter = Counter(
    "notifications_dlq_processed_total",
    "Total DLQ processed notifications",
    ["worker"]
)

worker_dlq_error_counter = Counter(
    "notifications_worker_dlq_error_total",
    "Total worker DLQ internal errors",
    ["worker"]
)

# Metrics endpoint
start_http_server(8005)

print("Prometheus metrics available on port 8005")

# =========================================================
# OpenTelemetry
# =========================================================

resource = Resource.create({
    "service.name": "worker-dlq-service"
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

# =========================================================
# AWS Clients
# =========================================================

print("Iniciando worker-dlq...")

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://dynamodb-local:8000",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

s3 = boto3.client(
    "s3",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

table = dynamodb.Table(
    "notifications-idempotency"
)

# =========================================================
# Esperar cola DLQ
# =========================================================

queue_url = None

while not queue_url:

    try:

        response = sqs.get_queue_url(
            QueueName="notifications-error-dlq"
        )

        queue_url = response["QueueUrl"]

        print(f"Cola encontrada: {queue_url}")

    except Exception as e:

        print("Esperando cola DLQ...")
        print(e)

        time.sleep(5)

print("Worker DLQ iniciado correctamente")

# =========================================================
# SAVE TO S3
# =========================================================

def save_to_s3(payload):

    key = (
        f"dlq/"
        f"{datetime.utcnow().strftime('%Y/%m/%d/')}"
        f"{payload['eventId']}.json"
    )

    s3.put_object(
        Bucket="notifications-history",
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )

# =========================================================
# Worker Loop
# =========================================================

while True:

    try:

        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        messages = response.get(
            "Messages",
            []
        )

        if not messages:
            continue

        for message in messages:

            body = json.loads(
                message["Body"]
            )

            with tracer.start_as_current_span(
                "worker-dlq-process-message"
            ) as span:

                trace_id = format(
                    span.get_span_context().trace_id,
                    "032x"
                )

                print()
                print("==============================")
                print("MENSAJE DLQ RECIBIDO")
                print("==============================")

                print(
                    f"eventId: "
                    f"{body.get('eventId')}"
                )

                print(
                    f"traceId: "
                    f"{trace_id}"
                )

                item = {

                    "eventId":
                    body["eventId"],

                    "correlationId":
                    body["correlationId"],

                    "channel":
                    body["channel"],

                    "status":
                    "DLQ",

                    "service":
                    "worker-dlq-service",

                    "workerId":
                    WORKER_ID,

                    "traceId":
                    trace_id,

                    "processedAt":
                    datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                }

                table.put_item(
                    Item=item
                )

                dlq_counter.labels(
                    worker=WORKER_ID
                ).inc()

                save_to_s3(item)

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message[
                        "ReceiptHandle"
                    ]
                )

                print("Mensaje DLQ procesado")

    except Exception as e:

        worker_dlq_error_counter.labels(
            worker=WORKER_ID
        ).inc()

        print()
        print("==============================")
        print("ERROR EN WORKER DLQ")
        print("==============================")

        print(e)

        time.sleep(5)

