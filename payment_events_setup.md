# Kafka Payment Events System Implementation

**Author:** Lucia Mabuza  
**Project:** Digital Payments Event Streaming Infrastructure

## Submission overview

This POC implements three Kafka event domains: payment, fraud and notification. It satisfies the brief with durable producers, keyed ordering, independent consumer groups, regulatory retention for payment/fraud events, monitoring commands and explicit trade-offs.

## Capacity design

Peak throughput is 1,000 Mb/s. At 100 Mb/s per producer, the peak requires 10 producer instances. At 10 Mb/s per consumer, a full-stream consumer group requires up to 100 instances. The payment and fraud streams therefore use 100 partitions. All related events use `paymentId` as the record key.

## Topics

| Topic | Partitions | RF | Retention | Purpose |
|---|---:|---:|---|---|
| `payments.events.v1` | 100 | 3 | 7 years | Payment lifecycle |
| `fraud.events.v1` | 100 | 3 | 7 years | Fraud scores |
| `notifications.events.v1` | 100 | 3 | 30 days | Notification outcomes |

## Consumer groups

| Group | Input | SLA | Purpose |
|---|---|---|---|
| `fraud-scoring-v1` | payments | <50 ms | Score initiated payments |
| `customer-notifications-v1` | payments | <2 s | Notify on validated/failed payments |
| `payment-reconciliation-v1` | payments | <1 hour | Reconciliation |
| `regulatory-audit-v1` | payment + fraud | <1 hour | Audit export/verification |

## Configuration and commands

See [`payment.md`](payment.md), [`fraud.md`](fraud.md), and [`notification.md`](notification.md) for executable Kubernetes Kafka CLI commands and examples.

## Payment lifecycle

```text
PAYMENT_INITIATED
  -> PAYMENT_AUTHORISED -> PAYMENT_VALIDATED -> PAYMENT_COMPLETED
  -> PAYMENT_NOT_AUTHORISED
  -> PAYMENT_AUTHORISED -> PAYMENT_INVALIDATED
  -> PAYMENT_AUTHORISED -> PAYMENT_VALIDATED -> PAYMENT_NOT_COMPLETED
```

## Durability and idempotency

Producers use `acks=all`, `enable.idempotence=true`, unlimited logical retries within a 120-second delivery timeout, and `max.in.flight.requests.per.connection=5`. Topics use RF=3 and `min.insync.replicas=2`. Application consumers must deduplicate on `eventId` and commit offsets only after successful processing.

## Serialization

JSON is used for this POC for readability. A production implementation should use a governed Avro or Protobuf schema and compatibility controls. Sensitive payment credentials must not be included in event payloads.

## Verification

```bash
kubectl exec -it kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --describe --topic payments.events.v1
kubectl exec -it kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group fraud-scoring-v1
kubectl exec -it kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group customer-notifications-v1
kubectl exec -it kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group payment-reconciliation-v1
```

To verify ordering, consume with key, partition and offset printing enabled. A single `paymentId` must remain on the same partition and its offsets must increase in lifecycle order. Verify consumer lag with `kafka-consumer-groups --describe`.

## Trade-offs

- 100 partitions provide the parallelism implied by the brief but increase broker and rebalance overhead.
- RF=3 plus `acks=all` improves durability but can reject writes when too few replicas are healthy.
- Seven-year Kafka retention supports replay but has substantial storage cost; production should evaluate compliant archival/tiered storage.
- Manual offset control reduces the chance of missed processing but means consumers must be idempotent.

## Error handling

Retry transient failures within the delivery window. Application consumers should route poison/unprocessable messages to dedicated DLQ topics and retain the original event plus failure metadata. Never silently discard a payment event.

## Issues and troubleshooting

- Invalid replication factor: confirm at least three brokers for RF=3.
- No consumer records: verify topic, offsets and producer success.
- High lag: scale consumers up to the partition count and inspect slow downstream calls or partition skew.
- Out-of-order lifecycle: confirm every producer call uses the same `paymentId` key.
- Duplicate side effects: deduplicate by `eventId` and commit only after successful processing.

## Execution evidence

The attached source files contain templates rather than actual cluster output. Run the commands against the provided cluster and paste the real topic descriptions, consumed records, partition/offset evidence and consumer-lag output here. Sample output is deliberately not represented as executed evidence.

## Acceptance checklist

- [x] Topic design and rationale
- [x] Payment producer design
- [x] Fraud consumer and producer design
- [x] Notification consumer and producer design
- [x] At least three independent consumer groups
- [x] Ordering strategy
- [x] Durability/idempotency settings
- [x] Verification and lag commands
- [x] Trade-offs and troubleshooting
- [ ] Paste real execution evidence from the local Kafka cluster

## Conclusion

The design decouples payment processing, fraud scoring and notifications while retaining per-payment ordering through the `paymentId` key. The configuration prioritises durability for financial events and scales according to the throughput assumptions supplied in the project brief.
