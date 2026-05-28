
#!/bin/bash

echo "=========================================="
echo "Inicializando infraestructura AWS local"
echo "=========================================="

sleep 5

# ==========================================
<<<<<<< HEAD
# CREAR DLQ PRINCIPAL
# ==========================================

echo "Creando notifications-dlq..."

awslocal sqs create-queue \
  --queue-name notifications-dlq
=======
# ESPERAR S3
# ==========================================

echo "Esperando LocalStack S3..."
>>>>>>> 1596098 (uopdate policy cycle)

until aws --endpoint-url=http://localstack:4566 s3 ls >/dev/null 2>&1
do
  echo "S3 no disponible aún..."
  sleep 2
done

echo "S3 disponible"

# ==========================================
<<<<<<< HEAD
# OBTENER ARN DLQ
=======
# CREAR DLQ
>>>>>>> 1596098 (uopdate policy cycle)
# ==========================================

echo "Creando notifications-dlq..."

aws --endpoint-url=http://localstack:4566 \
sqs create-queue \
--queue-name notifications-dlq

# ==========================================
# OBTENER ARN DLQ
# ==========================================

DLQ_ARN=$(aws --endpoint-url=http://localstack:4566 \
sqs get-queue-attributes \
--queue-url http://localstack:4566/000000000000/notifications-dlq \
--attribute-names QueueArn \
--query 'Attributes.QueueArn' \
--output text)

echo "DLQ ARN: $DLQ_ARN"

# ==========================================
# CREAR COLA PRINCIPAL
# ==========================================

echo "Creando notifications..."
<<<<<<< HEAD

awslocal sqs create-queue \
  --queue-name notifications \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

=======

aws --endpoint-url=http://localstack:4566 \
sqs create-queue \
--queue-name notifications \
--attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

>>>>>>> 1596098 (uopdate policy cycle)
echo "Queue notifications creada"

# ==========================================
# CREAR ERROR DLQ
# ==========================================

echo "Creando notifications-error-dlq..."
<<<<<<< HEAD

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
=======

aws --endpoint-url=http://localstack:4566 \
sqs create-queue \
--queue-name notifications-error-dlq
>>>>>>> 1596098 (uopdate policy cycle)

echo "ERROR DLQ ARN: $ERROR_DLQ_ARN"

# ==========================================
<<<<<<< HEAD
# CREAR ERROR QUEUE
# ==========================================

echo "Creando notifications-error..."

awslocal sqs create-queue \
  --queue-name notifications-error \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$ERROR_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

=======
# OBTENER ARN ERROR DLQ
# ==========================================

ERROR_DLQ_ARN=$(aws --endpoint-url=http://localstack:4566 \
sqs get-queue-attributes \
--queue-url http://localstack:4566/000000000000/notifications-error-dlq \
--attribute-names QueueArn \
--query 'Attributes.QueueArn' \
--output text)

echo "ERROR DLQ ARN: $ERROR_DLQ_ARN"

# ==========================================
# CREAR ERROR QUEUE
# ==========================================

echo "Creando notifications-error..."

aws --endpoint-url=http://localstack:4566 \
sqs create-queue \
--queue-name notifications-error \
--attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$ERROR_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

>>>>>>> 1596098 (uopdate policy cycle)
echo "Queue notifications-error creada"

# ==========================================
# CREAR BUCKET S3
# ==========================================

echo "Creando bucket S3..."

<<<<<<< HEAD
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
=======
aws --endpoint-url=http://localstack:4566 \
s3 mb s3://notifications-history || true

# ==========================================
# ESPERAR BUCKET
# ==========================================

echo "Esperando bucket..."

until aws --endpoint-url=http://localstack:4566 \
s3api head-bucket \
--bucket notifications-history >/dev/null 2>&1
do
  echo "Bucket no disponible aún..."
  sleep 2
done

echo "Bucket disponible"

sleep 10

# ==========================================
# APLICAR LIFECYCLE
# ==========================================

echo "Aplicando lifecycle configuration..."

aws --endpoint-url=http://localstack:4566 \
s3api put-bucket-lifecycle-configuration \
--bucket notifications-history \
--lifecycle-configuration file:///lifecycle.json

echo "Lifecycle aplicado"

# ==========================================
# VALIDACIONES
# ==========================================

echo "=========================================="
echo "VALIDANDO RECURSOS"
echo "=========================================="

echo "Queues:"

aws --endpoint-url=http://localstack:4566 \
sqs list-queues

echo "Buckets:"

aws --endpoint-url=http://localstack:4566 \
s3 ls

echo "Lifecycle:"

aws --endpoint-url=http://localstack:4566 \
s3api get-bucket-lifecycle-configuration \
--bucket notifications-history

echo "=========================================="
echo "Infraestructura inicializada correctamente"
echo "=========================================="

>>>>>>> 1596098 (uopdate policy cycle)
