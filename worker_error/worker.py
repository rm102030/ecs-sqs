import json
import time
import boto3

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ==============================
# OpenTelemetry
# ==============================

resource = Resource.create({
    "service.name": "worker-error-service"
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
# AWS Clients
# ==============================

print("Iniciando worker-error...")

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

table = dynamodb.Table("notifications-idempotency")

queue_url = None

# ==============================
# Esperar cola ERROR
# ==============================

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

# ==============================
# Worker Loop
# ==============================

while True:

    try:

        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        messages = response.get("Messages", [])

        if not messages:
            continue

        for message in messages:

            body = json.loads(message["Body"])

            # ==============================
            # SOLO mensajes ERROR
            # ==============================

            if body.get("channel") != "ERROR":

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                continue

            # ==============================
            # Crear span SOLO para ERROR
            # ==============================

            with tracer.start_as_current_span(
                "worker-error-process-message"
            ):

                print("\n==============================")
                print("MENSAJE ERROR RECIBIDO")
                print("==============================")

                print(f"eventId: {body.get('eventId')}")
                print(f"correlationId: {body.get('correlationId')}")
                print(f"channel: {body.get('channel')}")

                # ==============================
                # Retry Tracking
                # ==============================

                existing = table.get_item(
                    Key={
                        "eventId": body["eventId"]
                    }
                )

                retry_count = 1

                if "Item" in existing:

                    retry_count = existing["Item"].get(
                        "retryCount",
                        0
                    ) + 1

                # ==============================
                # Persistir estado FAILED
                # ==============================

                table.put_item(
                    Item={
                        "eventId": body["eventId"],
                        "correlationId": body["correlationId"],
                        "channel": body["channel"],
                        "status": "FAILED",
                        "createdAt": body["createdAt"],
                        "failedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "lastFailedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "retryCount": retry_count
                    }
                )

                print(
                    f"Evento FAILED almacenado en DynamoDB "
                    f"(retry {retry_count})"
                )

                # ==============================
                # Simulación error
                # ==============================

                print("\nSimulando error de procesamiento...")

                raise Exception(
                    "Simulated processing failure"
                )

    except Exception as e:

        print("\nERROR EN WORKER")
        print(e)

        time.sleep(5)
