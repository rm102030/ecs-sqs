#!/bin/bash

echo "=========================================="
echo "Inicializando infraestructura AWS local"
echo "=========================================="

sleep 5

# ==========================================
# CREAR DLQ PRINCIPAL
# ==========================================

echo "Creando notifications-dlq..."

awslocal sqs create-queue \
  --queue-name notifications-dlq

awslocal s3 mb s3://notifications-history || true  

# ==========================================
# OBTENER ARN DLQ
# ==========================================

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/notifications-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

echo "DLQ ARN: $DLQ_ARN"

# ==========================================
# CREAR COLA PRINCIPAL
# ==========================================

echo "Creando notifications..."

awslocal sqs create-queue \
  --queue-name notifications \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

echo "Queue notifications creada"

# ==========================================
# CREAR ERROR DLQ
# ==========================================

echo "Creando notifications-error-dlq..."

awslocal sqs create-queue \
  --queue-name notifications-error-dlq

# ==========================================
# OBTENER ARN ERROR DLQ
# ==========================================

ERROR_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/notifications-error-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

echo "ERROR DLQ ARN: $ERROR_DLQ_ARN"

# ==========================================
# CREAR ERROR QUEUE
# ==========================================

echo "Creando notifications-error..."

awslocal sqs create-queue \
  --queue-name notifications-error \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$ERROR_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

echo "Queue notifications-error creada"

# ==========================================
# CREAR BUCKET S3
# ==========================================

echo "Creando bucket S3..."

awslocal s3 mb s3://notifications-history || true

# ==========================================
# APLICAR LIFECYCLE POLICY
# ==========================================

echo "Aplicando lifecycle policy..."

awslocal s3api put-bucket-lifecycle-configuration \
  --bucket notifications-history \
  --lifecycle-configuration file:///etc/localstack/init/ready.d/lifecycle.json

# ==========================================
# VALIDACIONES
# ==========================================

echo "=========================================="
echo "VALIDANDO RECURSOS"
echo "=========================================="

echo "Queues:"

awslocal sqs list-queues

echo "Buckets:"

awslocal s3 ls

echo "Lifecycle:"

awslocal s3api get-bucket-lifecycle-configuration \
  --bucket notifications-history

echo "=========================================="
echo "Infraestructura inicializada correctamente"
echo "=========================================="
