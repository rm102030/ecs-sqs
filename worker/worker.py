import json
import time
import socket
import boto3

from datetime import datetime

from prometheus_client import (
    Counter,
    Histogram,
    start_http_server
)

from opentelemetry import trace
from opentelemetry.propagate import extract

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter
)

from providers.provider_factory import (
    send_notification
)

# =========================================================
# WORKER ID
# =========================================================

WORKER_ID = socket.gethostname()

print(f"Worker iniciado: {WORKER_ID}")

# =========================================================
# Prometheus Metrics
# =========================================================

processed_counter = Counter(
    "notifications_processed_total",
    "Total processed notifications",
    ["worker"]
)

duplicate_counter = Counter(
    "notifications_duplicate_total",
    "Total duplicate notifications",
    ["worker"]
)

error_counter = Counter(
    "notifications_worker_errors_total",
    "Total worker processing errors",
    ["worker"]
)

# =========================================================
# Histogram Metrics
# =========================================================

processing_latency = Histogram(
    "notification_processing_seconds",
    "Notification processing latency"
)

provider_latency = Histogram(
    "provider_request_seconds",
    "Provider request latency"
)

# Metrics endpoint
start_http_server(8002)

print("Prometheus metrics available on port 8002")

# =========================================================
# OpenTelemetry
# =========================================================

resource = Resource.create({
    "service.name": "worker-service"
})

provider = TracerProvider(resource=resource)

processor = BatchSpanProcessor(
    OTLPSpanExporter(
        endpoint="http://otel-collector:4317",
        insecure=True
    ),
    max_export_batch_size=32
)

provider.add_span_processor(processor)

trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# =========================================================
# AWS Clients
# =========================================================

print("Iniciando worker...")

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

print("Cliente SQS creado")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://dynamodb-local:8000",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

print("Cliente DynamoDB creado")

# =========================================================
# CREATE DYNAMODB TABLE IF NOT EXISTS
# =========================================================

try:

    existing_tables = dynamodb.meta.client.list_tables()

    if "notifications-idempotency" not in existing_tables["TableNames"]:

        print()
        print("==============================")
        print("CREANDO TABLA DYNAMODB")
        print("==============================")

        dynamodb.create_table(

            TableName="notifications-idempotency",

            KeySchema=[
                {
                    "AttributeName": "eventId",
                    "KeyType": "HASH"
                }
            ],

            AttributeDefinitions=[
                {
                    "AttributeName": "eventId",
                    "AttributeType": "S"
                }
            ],

            BillingMode="PAY_PER_REQUEST"
        )

        print("Tabla notifications-idempotency creada")

    else:

        print()
        print("==============================")
        print("TABLA DYNAMODB EXISTE")
        print("==============================")

except Exception as e:

    print()
    print("==============================")
    print("ERROR CREANDO TABLA DYNAMODB")
    print("==============================")
    print(e)

s3 = boto3.client(
    "s3",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

print("Cliente S3 creado")

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

    print()
    print("==============================")
    print("EVENTO GUARDADO EN S3")
    print("==============================")
    print(f"bucket: notifications-history")
    print(f"key: {key}")

# =========================================================
# Queue URL
# =========================================================

queue_name = "notifications"

while True:

    try:

        response = sqs.get_queue_url(
            QueueName=queue_name
        )

        queue_url = response["QueueUrl"]

        print(f"Cola encontrada: {queue_url}")

        break

    except Exception as e:

        print("Esperando cola SQS...")
        print(e)

        time.sleep(5)

print("Worker iniciado correctamente")

# =========================================================
# Main Loop
# =========================================================

while True:

    try:

        print()
        print("Consultando mensajes SQS...")

        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"]
        )

        if "Messages" not in messages:

            print("No hay mensajes disponibles")

            continue

        message = messages["Messages"][0]

        body = json.loads(
            message["Body"]
        )

        # =====================================================
        # TRACE CONTEXT EXTRACTION
        # =====================================================

        carrier = {}

        attributes = message.get(
            "MessageAttributes",
            {}
        )

        if "traceparent" in attributes:

            carrier["traceparent"] = attributes[
                "traceparent"
            ]["StringValue"]

        print()
        print("==============================")
        print("TRACE CONTEXT")
        print("==============================")
        print(carrier)

        ctx = extract(carrier)

        # =====================================================
        # PROCESS MESSAGE
        # =====================================================

        processing_start = time.time()

        with tracer.start_as_current_span(
            "worker-process-message",
            context=ctx
        ) as span:

            trace_id = format(
                span.get_span_context().trace_id,
                "032x"
            )

            print()
            print("==============================")
            print("TRACE PROPAGADO")
            print("==============================")
            print(f"traceId: {trace_id}")

            print()
            print("==============================")
            print("MENSAJE RECIBIDO")
            print("==============================")

            print(f"eventId: {body['eventId']}")
            print(f"correlationId: {body['correlationId']}")
            print(f"channel: {body['channel']}")
            print(f"recipient: {body['recipient']}")
            print(f"message: {body['message']}")

            # =================================================
            # PROVIDER LAYER
            # =================================================

            print()
            print("==============================")
            print("PROVIDER LAYER")
            print("==============================")

            provider_start = time.time()

            provider_response = send_notification(body)

            provider_latency.observe(
                time.time() - provider_start
            )

            print()
            print("==============================")
            print("PROVIDER RESPONSE")
            print("==============================")

            print(provider_response)

            try:

                table.put_item(
                    Item={
                        "eventId": body["eventId"],
                        "correlationId": body["correlationId"],
                        "channel": body["channel"],
                        "status": "PROCESSED",
                        "duplicateCount": 0,
                        "createdAt": body["createdAt"],
                        "processedAt": datetime.utcnow().strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        "traceId": trace_id,
                        "service": "worker-service",
                        "provider": provider_response.get(
                            "provider"
                        ),
                        "providerStatus": provider_response.get(
                            "status"
                        ),
                        "providerMessageId": provider_response.get(
                            "providerMessageId",
                            "N/A"
                        )
                    },
                    ConditionExpression=
                    "attribute_not_exists(eventId)"
                )

                processed_counter.labels(
                    worker=WORKER_ID
                ).inc()

                print()
                print("==============================")
                print("EVENTO ALMACENADO")
                print("==============================")

                print(f"eventId: {body['eventId']}")
                print("service: worker-service")
                print(f"traceId: {trace_id}")

                save_payload = {
                    "eventId": body["eventId"],
                    "correlationId": body["correlationId"],
                    "channel": body["channel"],
                    "status": "SUCCESS",
                    "processedAt": datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "traceId": trace_id,
                    "provider": provider_response.get(
                        "provider"
                    ),
                    "providerStatus": provider_response.get(
                        "status"
                    ),
                    "providerMessageId": provider_response.get(
                        "providerMessageId",
                        "N/A"
                    )
                }

                save_to_s3(
                    "SUCCESS",
                    save_payload
                )

            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:

                duplicate_counter.labels(
                    worker=WORKER_ID
                ).inc()

                print()
                print("Evento duplicado detectado")
                print("Mensaje ignorado")

                table.update_item(
                    Key={
                        "eventId": body["eventId"]
                    },
                    UpdateExpression="""
                        SET duplicateCount =
                        if_not_exists(duplicateCount, :zero) + :inc,
                        lastDuplicateAt = :timestamp
                    """,
                    ExpressionAttributeValues={
                        ":inc": 1,
                        ":zero": 0,
                        ":timestamp": datetime.utcnow().strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        )
                    }
                )

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"]
            )

            print()
            print("Mensaje eliminado de SQS")

            processing_latency.observe(
                time.time() - processing_start
            )

    except Exception as e:

        error_counter.labels(
            worker=WORKER_ID
        ).inc()

        print()
        print("==============================")
        print("ERROR EN WORKER")
        print("==============================")

        print(e)

        try:

            error_payload = {
                "eventId": "worker-error",
                "error": str(e),
                "timestamp": datetime.utcnow().strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            }

            save_to_s3(
                "FAILED",
                error_payload
            )

        except Exception as s3_error:

            print()
            print("ERROR GUARDANDO FAILED EN S3")
            print(s3_error)

        time.sleep(5)