# Laboratorio Arquitectura Distribuida Event-Driven con OpenTelemetry

## Descripción

- Amazon SQS
- FastAPI
- Workers distribuidos
- OpenTelemetry
- Jaeger
- DynamoDB
- DLQ
- Auto Scaling
- Idempotencia

## Levantar Ambiente

```bash
docker compose down
docker compose up --build
bash init-sqs.sh
```

## Resultado esperado

- Distributed Tracing
- Context Propagation
- Retry Handling
- DLQ Routing
- Jaeger Observability
- DynamoDB Audit Trail
