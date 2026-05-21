# Prueba de Auto Scaling, Resiliencia e Idempotencia

## Objetivo de la prueba

Validar el comportamiento de una arquitectura distribuida basada en:

- SQS
- Workers paralelos
- Auto Scaling
- DynamoDB
- Idempotencia

La prueba consistió en enviar 200 eventos concurrentes para evaluar:

- escalabilidad
- resiliencia
- procesamiento paralelo
- manejo de duplicados
- reducción automática de backlog

---

# Arquitectura validada

```text
Producer API
    ↓
SQS Queue
    ↓
Workers Paralelos
    ↓
DynamoDB (Idempotency Store)
```

---

# Resultado final

```text
200 eventos enviados
200 eventos procesados
0 duplicados
Auto Scaling activado
Procesamiento paralelo validado
Backlog reducido automáticamente
Sistema resilient