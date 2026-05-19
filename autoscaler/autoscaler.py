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

while True:

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

    print(f"Mensajes en cola: {messages}")

    desired_scale = 1

    if messages > 100:
        desired_scale = 10

    elif messages > 50:
        desired_scale = 5

    elif messages > 10:
        desired_scale = 3

    if desired_scale != current_scale:

        print(
            f"Escalando workers a {desired_scale}"
        )

        subprocess.run([
            "docker",
            "compose",
            "-f",
            "/workspace/docker-compose.yml",
            "up",
            "--scale",
            f"worker={desired_scale}",
            "-d"
        ])

        current_scale = desired_scale

    time.sleep(10)
