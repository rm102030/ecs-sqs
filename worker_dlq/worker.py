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
    "service.name": "worker-dlq-service"
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

print("Iniciando worker-dlq...")

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
# Esperar DLQ
# ==============================

while not queue_url:

    try:

        response = sqs.get_queue_url(
            QueueName="notifications-error-dlq"
        )

        queue_url = response["QueueUrl"]

        print(f"DLQ encontrada: {queue_url}")

    except Exception as e:

        print("Esperando DLQ...")
        print(e)

        time.sleep(5)

print("Worker-DLQ iniciado correctamente")

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

            with tracer.start_as_current_span(
                "worker-dlq-process-message"
            ):

                print("\n==============================")
                print("MENSAJE DLQ RECIBIDO")
                print("==============================")

                print(f"eventId: {body.get('eventId')}")

                existing = table.get_item(
                    Key={
                        "eventId": body["eventId"]
                    }
                )

                retry_count = 0

                if "Item" in existing:

                    retry_count = existing["Item"].get(
                        "retryCount",
                        0
                    )

                table.put_item(
                    Item={
                        "eventId": body["eventId"],
                        "correlationId": body["correlationId"],
                        "channel": body["channel"],
                        "status": "DLQ",
                        "createdAt": body["createdAt"],
                        "dlqAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "retryCount": retry_count,
                        "terminalFailure": True
                    }
                )

                print("Evento marcado como DLQ")

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                print("Mensaje eliminado de DLQ")

    except Exception as e:

        print("\nERROR EN WORKER-DLQ")
        print(e)

        time.sleep(5)
