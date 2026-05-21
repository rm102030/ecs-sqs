import json
import time
import boto3
import socket

from datetime import datetime

from opentelemetry import trace

from opentelemetry.context import (
    attach,
    detach
)

from opentelemetry.propagate import (
    extract
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

from opentelemetry.trace import (
    Status,
    StatusCode
)

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter
)

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
# Worker Metadata
# =========================================================

worker_id = socket.gethostname()

print(f"Worker ID: {worker_id}")

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

            # =====================================================
            # OpenTelemetry Context Extraction
            # =====================================================

            carrier = {}

            for key, value in message[
                "MessageAttributes"
            ].items():

                carrier[key] = value[
                    "StringValue"
                ]

            print()
            print("==============================")
            print("TRACE CONTEXT")
            print("==============================")
            print(carrier)

            ctx = extract(carrier)

            token = attach(ctx)

            incoming_trace_id = carrier[
                "traceId"
            ]

            print()
            print("==============================")
            print("TRACE PROPAGADO")
            print("==============================")
            print(f"traceId: {incoming_trace_id}")

            body = json.loads(
                message["Body"]
            )

            # =====================================================
            # SOLO mensajes ERROR
            # =====================================================

            if body.get("channel") != "ERROR":

                sqs.delete_message(

                    QueueUrl=queue_url,

                    ReceiptHandle=message[
                        "ReceiptHandle"
                    ]
                )

                continue

            # =====================================================
            # CONTINÚA TRACE ORIGINAL
            # =====================================================

            with tracer.start_as_current_span(
                "worker-error-process-message",
                context=ctx
            ) as span:

                print()
                print("==============================")
                print("MENSAJE ERROR RECIBIDO")
                print("==============================")

                print(
                    f"eventId: "
                    f"{body.get('eventId')}"
                )

                print(
                    f"correlationId: "
                    f"{body.get('correlationId')}"
                )

                print(
                    f"channel: "
                    f"{body.get('channel')}"
                )

                # ==============================================
                # Retry Tracking
                # ==============================================

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

                # ==============================================
                # Persistir estado FAILED
                # ==============================================

                status_value = "FAILED"

                if retry_count >= MAX_RETRIES:
                    status_value = "DLQ"

                table.put_item(

                    Item={

                        # =====================================
                        # Evento
                        # =====================================

                        "eventId":
                        body["eventId"],

                        "correlationId":
                        body["correlationId"],

                        "channel":
                        body["channel"],

                        # =====================================
                        # Estado
                        # =====================================

                        "status":
                        status_value,

                        "retryCount":
                        retry_count,

                        "terminalFailure":
                        retry_count >= MAX_RETRIES,

                        # =====================================
                        # Observabilidad
                        # =====================================

                        "service":
                        "worker-error-service",

                        "workerId":
                        worker_id,

                        "traceId":
                        incoming_trace_id,

                        "errorType":
                        "SimulatedProcessingFailure",

                        # =====================================
                        # Fechas
                        # =====================================

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
                )

                print()
                print("==============================")
                print("EVENTO FAILED")
                print("==============================")

                print(
                    f"eventId: "
                    f"{body['eventId']}"
                )

                print(
                    f"service: "
                    f"worker-error-service"
                )

                print(
                    f"workerId: "
                    f"{worker_id}"
                )

                print(
                    f"traceId: "
                    f"{incoming_trace_id}"
                )

                print(
                    f"retryCount: "
                    f"{retry_count}"
                )

                # ==============================================
                # Manual DLQ handling
                # ==============================================

                if retry_count >= MAX_RETRIES:

                    print()
                    print("==============================")
                    print("MOVIENDO A DLQ")
                    print("==============================")

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

                    print("Mensaje movido a DLQ")

                    continue

                # ==============================================
                # Simulación error
                # ==============================================

                print()
                print(
                    "Simulando error de procesamiento..."
                )

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

            detach(token)

    except Exception as e:

        print()
        print("==============================")
        print("ERROR EN WORKER")
        print("==============================")

        print(e)

        time.sleep(5)
