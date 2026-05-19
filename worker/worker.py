import json
import time
import boto3

from datetime import datetime

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

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
    )
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

table = dynamodb.Table(
    "notifications-idempotency"
)

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
            WaitTimeSeconds=5
        )

        if "Messages" not in messages:

            print("No hay mensajes disponibles")

            continue

        message = messages["Messages"][0]

        body = json.loads(
            message["Body"]
        )

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
        # Validación idempotencia
        # =================================================

        existing = table.get_item(
            Key={
                "eventId": body["eventId"]
            }
        )

        # =================================================
        # Evento duplicado
        # =================================================

        if "Item" in existing:

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

            continue

        # =================================================
        # Procesamiento normal
        # =================================================

        with tracer.start_as_current_span(
            "worker-process-message"
        ):

            print()
            print("Procesando EMAIL provider...")

            time.sleep(2)

            print("EMAIL enviado correctamente")

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
                    )
                }
            )

            print("Evento almacenado en DynamoDB")

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"]
            )

            print("Mensaje eliminado de SQS")

    except Exception as e:

        print()
        print("ERROR EN WORKER")
        print(e)

        time.sleep(5)
