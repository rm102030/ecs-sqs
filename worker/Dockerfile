FROM python:3.11

WORKDIR /app

COPY worker.py .

RUN pip install \
    boto3 \
    opentelemetry-sdk \
    opentelemetry-api \
    opentelemetry-exporter-otlp

CMD ["python", "-u", "worker.py"]
