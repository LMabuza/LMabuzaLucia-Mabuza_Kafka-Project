# Notification producer, consumer, topic and events

This file completes the `notification.md` configuration required by the project brief. The POC uses `paymentId` as the Kafka key so events belonging to the same payment remain ordered in one partition.

## Topic

```bash
kubectl exec -it kafka-0 -- kafka-topics \
--bootstrap-server kafka-service:9092 \
--create \
--if-not-exists \
--topic notifications.events.v1 \
--partitions 100 \
--replication-factor 3 \
--config retention.ms=2592000000 \
--config cleanup.policy=delete \
--config min.insync.replicas=2 \
--config compression.type=producer \
--config max.message.bytes=1048576
```

## Consumer

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
--bootstrap-server kafka-service:9092 \
--topic payments.events.v1 \
--group customer-notifications-v1 \
--property print.key=true \
--property print.partition=true \
--property print.offset=true \
--property max.poll.records=100 \
--property session.timeout.ms=30000 \
--property heartbeat.interval.ms=10000 \
--property auto.offset.reset=earliest \
--property enable.auto.commit=false \
--property max.poll.interval.ms=300000 \
--property fetch.min.bytes=1 \
--property fetch.max.wait.ms=500 \
--property max.partition.fetch.bytes=1048576 \
--property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Manual offset commits are selected for the application implementation so an event is acknowledged only after successful processing. Console consumers are used here to demonstrate the stream; business filtering and durable offset handling belong in the application consumer.

## Producer

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
--bootstrap-server kafka-service:9092 \
--topic notifications.events.v1 \
--property parse.key=true \
--property key.separator=: \
--property acks=all \
--property retries=2147483647 \
--property max.in.flight.requests.per.connection=5 \
--property enable.idempotence=true \
--property compression.type=lz4 \
--property linger.ms=5 \
--property batch.size=32768 \
--property delivery.timeout.ms=120000 \
--property request.timeout.ms=30000
```

## Events

The notification consumer processes payment failure events and `PAYMENT_VALIDATED`. The producer records whether the payment notification was approved/sent or failed.

```json
{"eventId":"evt-note-001","eventType":"PAYMENT_NOTIFICATION_APPROVED","paymentId":"pay-1001","status":"SENT"}
{"eventId":"evt-note-002","eventType":"PAYMENT_NOTIFICATION_FAILED","paymentId":"pay-1002","status":"FAILED"}
```

## Design rationale

- **100 partitions** align with the brief's 1,000 Mb/s peak and 10 Mb/s-per-consumer assumption for a full-stream group.
- **Replication factor 3**, `min.insync.replicas=2` and `acks=all` favour durability over availability.
- **30-day retention** is used for notification outcomes; the seven-year requirement in the brief applies specifically to payment and fraud events.
- **LZ4** reduces bandwidth/storage while keeping compression latency low.
- **Idempotence** protects against duplicate records caused by producer retries.
- `paymentId` is the message key for per-payment ordering and correlation.

## Verification

```bash
kubectl exec -it kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --describe --topic notifications.events.v1
kubectl exec -it kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group customer-notifications-v1
```

> Record the real command output after running against the supplied cluster. No fabricated execution output is included in this submission.
