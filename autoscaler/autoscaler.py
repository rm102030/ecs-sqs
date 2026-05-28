
import time
import boto3
import subprocess

QUEUE_NAME = "notifications"

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

print("Esperando cola SQS...")

while True:

    try:

        queue_url = sqs.get_queue_url(
            QueueName=QUEUE_NAME
        )["QueueUrl"]

        print(f"Cola encontrada: {queue_url}")

        break

    except Exception as e:

        print(e)

        time.sleep(5)

current_scale = 1

MAX_WORKERS = 5

while True:

    try:

        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages"
            ]
        )

        messages = int(
            attrs["Attributes"][
                "ApproximateNumberOfMessages"
            ]
        )

        print()
        print("==============================")
        print(f"Mensajes en cola: {messages}")
        print(f"Workers actuales: {current_scale}")

        desired_scale = 1

        # =====================================
        # Escalado suave
        # =====================================

        if messages > 300:
            desired_scale = 5

        elif messages > 200:
            desired_scale = 4

        elif messages > 100:
            desired_scale = 3

        elif messages > 50:
            desired_scale = 2

        desired_scale = min(
            desired_scale,
            MAX_WORKERS
        )

        print(f"Workers deseados: {desired_scale}")

        # =====================================
        # Evitar escalado innecesario
        # =====================================

        if desired_scale != current_scale:

            print(
                f"Escalando workers a {desired_scale}"
            )

            subprocess.run([
                "docker",
                "compose",
                "-p",
                "lab_notificaciones",
                "-f",
                "/workspace/docker-compose.yml",
                "up",
                "--no-build",
                "--no-deps",
                "--scale",
                f"worker={desired_scale}",
                "-d",
                "worker"
            ])

            current_scale = desired_scale

        else:

            print("No se requiere escalado")

    except Exception as e:

        print()
        print("==============================")
        print("ERROR AUTOSCALER")
        print("==============================")

        print(e)

    time.sleep(15)

