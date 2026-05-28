
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
from opentelemetry.propagate import extract

from opentelemetry.sdk.resources import (
    Resource
)

from opentelemetry.sdk.trace import (
    TracerProvider
)

from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor
)

from opentelemetry.trace import (
    Status,
    StatusCode
)

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter
)

# =========================================================
# Worker Metadata
# =========================================================

WORKER_ID = socket.gethostname()

print(f"Worker ID: {WORKER_ID}")

# =========================================================
# Prometheus Metrics
# =========================================================

failed_counter = Counter(
    "notifications_failed_total",
    "Total failed notifications",
    ["worker"]
)

retry_counter = Counter(
    "notifications_retry_total",
    "Total retry attempts",
    ["worker"]
)

dlq_counter = Counter(
    "notifications_dlq_total",
    "Total DLQ notifications",
    ["worker"]
)

worker_error_counter = Counter(
    "notifications_worker_error_total",
    "Total worker internal errors",
    ["worker"]
)

# Metrics endpoint
start_http_server(8003)

print("Prometheus metrics available on port 8003")

# =========================================================
# OpenTelemetry
# =========================================================

resource = Resource.create({
    "service.name": "worker-error-service"
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

print("Iniciando worker-error...")

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
# S3 Historical Storage
# =========================================================

def save_to_s3(status, payload):

    key = (
        f"{status.lower()}/"
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
# Config
# =========================================================

MAX_RETRIES = 3

queue_url = None

# =========================================================
# Esperar cola ERROR
# =========================================================

while not queue_url:

    try:

        response = sqs.get_queue_url(
            QueueName="notifications-error"
        )

        queue_url = response["QueueUrl"]

        print(f"Cola encontrada: {queue_url}")

    except Exception as e:

        print("Esperando cola SQS...")
        print(e)

        time.sleep(5)

print("Worker-error iniciado correctamente")

# =========================================================
# Worker Loop
# =========================================================

while True:

    try:

        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"]
        )

        messages = response.get(
            "Messages",
            []
        )

        if not messages:
            continue

        for message in messages:

            carrier = {}

            for key, value in message[
                "MessageAttributes"
            ].items():

                carrier[key] = value[
                    "StringValue"
                ]

            ctx = extract(carrier)

            body = json.loads(
                message["Body"]
            )

            if body.get("channel") != "ERROR":

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message[
                        "ReceiptHandle"
                    ]
                )

                continue

            with tracer.start_as_current_span(
                "worker-error-process-message",
                context=ctx
            ) as span:

                trace_id = format(
                    span.get_span_context().trace_id,
                    "032x"
                )

                existing = table.get_item(
                    Key={
                        "eventId":
                        body["eventId"]
                    }
                )

                retry_count = 1

                if "Item" in existing:

                    retry_count = existing[
                        "Item"
                    ].get(
                        "retryCount",
                        0
                    ) + 1

                retry_counter.labels(
                    worker=WORKER_ID
                ).inc()

                status_value = "FAILED"

                if retry_count >= MAX_RETRIES:
                    status_value = "DLQ"

                item = {

                    "eventId":
                    body["eventId"],

                    "correlationId":
                    body["correlationId"],

                    "channel":
                    body["channel"],

                    "status":
                    status_value,

                    "retryCount":
                    retry_count,

                    "terminalFailure":
                    retry_count >= MAX_RETRIES,

                    "service":
                    "worker-error-service",

                    "workerId":
                    WORKER_ID,

                    "traceId":
                    trace_id,

                    "errorType":
                    "SimulatedProcessingFailure",

                    "createdAt":
                    body["createdAt"],

                    "failedAt":
                    datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),

                    "lastFailedAt":
                    datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),

                    "dlqAt":
                    datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ) if retry_count >= MAX_RETRIES else "N/A"
                }

                table.put_item(
                    Item=item
                )

                failed_counter.labels(
                    worker=WORKER_ID
                ).inc()

                save_payload = {
                    "eventId": body["eventId"],
                    "correlationId": body["correlationId"],
                    "channel": body["channel"],
                    "status": status_value,
                    "retryCount": retry_count,
                    "traceId": trace_id,
                    "workerId": WORKER_ID,
                    "failedAt": datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                }

                save_to_s3(
                    status_value,
                    save_payload
                )

                if retry_count >= MAX_RETRIES:

                    dlq_counter.labels(
                        worker=WORKER_ID
                    ).inc()

                    dlq_response = sqs.get_queue_url(
                        QueueName="notifications-error-dlq"
                    )

                    dlq_url = dlq_response["QueueUrl"]

                    sqs.send_message(
                        QueueUrl=dlq_url,
                        MessageBody=json.dumps(body),
                        MessageAttributes=message[
                            "MessageAttributes"
                        ]
                    )

                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message[
                            "ReceiptHandle"
                        ]
                    )

                    continue

                span.record_exception(
                    Exception(
                        "Simulated processing failure"
                    )
                )

                span.set_status(
                    Status(
                        StatusCode.ERROR
                    )
                )

                raise Exception(
                    "Simulated processing failure"
                )

    except Exception as e:

        worker_error_counter.labels(
            worker=WORKER_ID
        ).inc()

        print(e)

        time.sleep(5)

