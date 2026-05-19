FROM docker:latest

WORKDIR /workspace

RUN apk add --no-cache \
    python3 \
    py3-pip \
    docker-cli-compose

RUN pip3 install boto3 --break-system-packages

CMD ["python3", "-u", "autoscaler/autoscaler.py"]
