FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install \
    boto3 \
    opentelemetry-sdk \
    opentelemetry-api \
    opentelemetry-exporter-otlp \
    prometheus-client

CMD ["python", "-u", "worker.py"]
