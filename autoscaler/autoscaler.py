import time
import boto3
import subprocess

QUEUE_NAME = "notifications"

MIN_WORKERS = 1
MAX_WORKERS = 3

CHECK_INTERVAL = 45
SCALE_COOLDOWN = 120

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
        queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        print(f"Cola encontrada: {queue_url}")
        break
    except Exception as e:
        print(f"Error conectando SQS: {e}")
        time.sleep(5)

current_scale = 1
last_scale_time = 0

while True:
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )

        messages = int(attrs["Attributes"]["ApproximateNumberOfMessages"])

        print("=" * 60)
        print(f"Mensajes en cola: {messages}")
        print(f"Workers actuales: {current_scale}")

        desired_scale = MIN_WORKERS

        # =====================================================
        # ESCALADO MUY SUAVE
        # =====================================================
        if messages > 100:
            desired_scale = 3
        elif messages > 50:
            desired_scale = 2
        else:
            desired_scale = 1

        if desired_scale > MAX_WORKERS:
            desired_scale = MAX_WORKERS

        print(f"Workers deseados: {desired_scale}")

        # =====================================================
        # COOLDOWN
        # =====================================================
        cooldown_passed = (time.time() - last_scale_time) > SCALE_COOLDOWN

        if desired_scale != current_scale and cooldown_passed:
            print("=" * 60)
            print(f"Escalando workers a {desired_scale}")

            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "-p",
                    "lab_notificaciones",
                    "-f",
                    "/workspace/docker-compose.yml",
                    "up",
                    "--scale",
                    f"worker={desired_scale}",
                    "-d",
                    "worker"
                ],
                capture_output=True,
                text=True
            )

            print("=" * 60)
            print("STDOUT:")
            print(result.stdout)

            print("=" * 60)
            print("STDERR:")
            print(result.stderr)
            print("=" * 60)

            # FIX: Actualizar estados para el control de la lógica
            current_scale = desired_scale
            last_scale_time = time.time()

    except Exception as e:
        print(f"Error en el bucle de monitoreo: {e}")

    # FIX: Evita el bucle infinito al 100% de CPU
    time.sleep(CHECK_INTERVAL)
