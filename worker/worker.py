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

# ==============================
# AWS Clients
# ==============================

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

table = dynamodb.Table("notifications-idempotency")

queue_url = None

# ==============================
# Esperar cola normal
# ==============================

while not queue_url:

    try:

        response = sqs.get_queue_url(
            QueueName="notifications"
        )

        queue_url = response["QueueUrl"]

        print(f"Cola encontrada: {queue_url}")

    except Exception as e:

        print("Esperando cola SQS...")
        print(e)

        time.sleep(5)

print("Worker iniciado correctamente")

# ==============================
# Worker Loop
# ==============================

while True:

    try:

        print("\nConsultando mensajes SQS...")

        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        messages = response.get("Messages", [])

        if not messages:

            print("No hay mensajes disponibles")
            continue

        for message in messages:

            body = json.loads(message["Body"])

            # ==============================
            # Ignorar mensajes ERROR
            # ==============================

            if body.get("channel") == "ERROR":

                print("\nMensaje ignorado")
                print("Pertenece al flujo ERROR")

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                continue

            # ==============================
            # Crear span SOLO para EMAIL
            # ==============================

            with tracer.start_as_current_span(
                "worker-process-message"
            ):

                print("\n==============================")
                print("MENSAJE RECIBIDO")
                print("==============================")

                print(f"eventId: {body.get('eventId')}")
                print(f"correlationId: {body.get('correlationId')}")
                print(f"channel: {body.get('channel')}")
                print(f"recipient: {body.get('recipient')}")
                print(f"message: {body.get('message')}")

                # ==============================
                # Idempotencia
                # ==============================

                existing = table.get_item(
                    Key={
                        "eventId": body["eventId"]
                    }
                )

                if "Item" in existing:

                    print("\nEvento duplicado detectado")
                    print("Mensaje ignorado")

                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message["ReceiptHandle"]
                    )

                    continue

                # ==============================
                # Simulación procesamiento
                # ==============================

                print(f"\nProcesando {body.get('channel')} provider...")

                time.sleep(2)

                print(f"{body.get('channel')} enviado correctamente")

                # ==============================
                # Persistencia DynamoDB
                # ==============================

                table.put_item(
                    Item={
                        "eventId": body["eventId"],
                        "correlationId": body["correlationId"],
                        "channel": body["channel"],
                        "status": "PROCESSED",
                        "createdAt": body["createdAt"],
                        "processedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                )

                print("Evento PROCESSED almacenado en DynamoDB")

                # ==============================
                # ACK SQS
                # ==============================

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                print("Mensaje eliminado de SQS")

    except Exception as e:

        print("\nERROR EN WORKER")
        print(e)

        time.sleep(5)
