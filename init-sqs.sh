#!/bin/bash

echo "Inicializando SQS..."

# ==========================================
# Crear DLQ
# ==========================================

awslocal sqs create-queue \
  --queue-name notifications-dlq

# ==========================================
# Obtener ARN DLQ
# ==========================================

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/notifications-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

echo "DLQ ARN: $DLQ_ARN"

# ==========================================
# Crear cola principal con DLQ policy
# ==========================================

awslocal sqs create-queue \
  --queue-name notifications \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

echo "SQS configurado correctamente"

# ==========================================
# Crear DLQ ERROR
# ==========================================

awslocal sqs create-queue \
  --queue-name notifications-error-dlq

ERROR_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/notifications-error-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

# ==========================================
# Crear cola ERROR
# ==========================================

awslocal sqs create-queue \
  --queue-name notifications-error \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$ERROR_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

