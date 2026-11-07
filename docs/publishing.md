# Publishing

## Delivery model

An idempotent producer with `acks=all` gives durable, ordered delivery within a producer session. It does not give exactly-once delivery to your database. A client retry, a Cloud Run restart, or a regional failover can still produce duplicates.

Design for at-least-once:

- Give every event a stable `eventId`.
- Let downstream consumers deduplicate by `eventId` or a natural key.
- Use the message key for partition ordering, not for deduplication alone.
- Do not retry a publish indefinitely from the request handler. Return a retriable error and let the caller retry with the same event ID.

## Recommended producer configuration

HTTP publishes enter through Cloudflare Load Balancing and a regional external ALB. A Cloud Run app that embeds the Kafka client bypasses that HTTP edge and writes through Direct VPC egress and PSC instead.

The reference producer is in [`src/kafka.ts`](../src/kafka.ts) and is used by [`src/producer-service.ts`](../src/producer-service.ts).

| Setting | Value | Why |
| --- | --- | --- |
| `security.protocol` | `sasl_ssl` | Encrypted transport and SASL authentication |
| `sasl.mechanisms` | `PLAIN` | Confluent Cloud API key and secret |
| `client.dns.lookup` | `use_all_dns_ips` | Use every broker/PSC address instead of only the first |
| `socket.keepalive.enable` | `true` | Detect dead connections |
| `enable.idempotence` | `true` | Avoid duplicates and reordering from internal retries |
| `acks` | `-1` | Wait for all in-sync replicas |
| `delivery.timeout.ms` | `120000` | Bound the total time a record can sit in the producer |
| `linger.ms` | `10` | Small batching window without noticeable latency |
| `compression.codec` | `zstd` | Good compression ratio and CPU efficiency |
| `max.in.flight.requests.per.connection` | `5` | Safe upper bound with idempotence |
| `queue.buffering.max.messages` | `100000` | Absorb bursts; tune for memory |

`enable.idempotence=true` automatically adjusts `acks`, retries, and in-flight limits if they are not explicitly set. The reference configuration sets compatible values explicitly so the intent is visible.

## Message shape

The HTTP producer accepts:

```json
{
  "key": "order-1",
  "value": {
    "eventId": "evt-1",
    "type": "order.created",
    "payload": {
      "orderId": "order-1",
      "amount": 42.5
    }
  },
  "headers": {
    "traceparent": "00-..."
  }
}
```

The service adds:

- `event-id`: a generated UUID when the caller does not provide one.
- `content-type`: `application/json`.
- `source`: the Cloud Run service name.

Use a business key such as `orderId` as the Kafka key when per-order ordering matters. All records with the same key go to the same partition while the partition count is unchanged.

## HTTP contract

- Return `202 Accepted` after Kafka acknowledges the write.
- Return the topic, partition, offset, and event ID.
- Return `400` for invalid JSON or invalid fields.
- Return `413` for payloads over the configured limit.
- Return `503` with `Retry-After` when Kafka cannot accept the write.
- Do not return `200` before the broker acknowledges the write unless you intentionally accept the durability trade-off.

The caller should retry `503` with exponential backoff and the same event ID. If the caller cannot tolerate duplicates, it should write to a local outbox first and publish from an outbox worker.

## Why not wait for a client retry inside the handler

The producer already retries retriable broker errors until `delivery.timeout.ms`. Adding an unbounded application retry loop in the HTTP handler:

- Holds the Cloud Run request open.
- Consumes concurrency and memory.
- Makes the client timeout ambiguous.
- Increases the chance of duplicate writes after a timeout.

Let the producer exhaust its delivery budget, then return a retriable error.

## Error handling

| Error class | Examples | Action |
| --- | --- | --- |
| Retriable | Broker transport failure, leader election, queue full, timeout | Log, return `503`, let the caller retry |
| Authentication | Invalid API key, expired credentials | Fail fast, page the owner, do not retry forever |
| Authorization | Topic ACL missing | Fail fast, fix the ACL |
| Invalid record | Message too large, invalid topic | Return `400`, do not retry |
| Fatal producer state | Idempotence gap, fatal configuration | Restart the instance after logging |

The reference service logs the event ID and error details and returns `503` for all send failures. In production, classify the error code and avoid returning a retriable status for permanent failures.

## Cloud Run lifecycle

- Run the producer as a Cloud Run service with `min-instances` at 1 for latency-sensitive traffic.
- Use instance-based billing (`--no-cpu-throttling`) if the instance must flush or reconnect outside a request.
- Set a startup probe. Direct VPC egress can delay the first packet, and the producer connection may need a few seconds.
- On SIGTERM, stop accepting new requests, flush the producer, and disconnect.
- Keep the graceful shutdown budget below the Cloud Run termination grace period. The reference uses 8 seconds by default.

The handler closes the HTTP server first, then flushes, then disconnects:

```text
SIGTERM
  -> mark not ready
  -> server.close() and close idle connections
  -> producer.flush(timeout)
  -> producer.disconnect()
  -> exit 0
```

## Multi-region publishing

- Produce to the local regional cluster in steady state.
- Do not produce to a Cluster Linking mirror topic. Mirror topics are read-only.
- On failover, switch the producer's bootstrap endpoint to the secondary regional cluster.
- Keep the same event ID scheme so consumers can deduplicate across clusters.
- Test the failover path. A producer that reconnects to a new cluster may produce duplicates for in-flight records.
- Use an explicit runbook or automated health check for the switch. Do not rely on a global bootstrap endpoint that silently points at another region.

If you need stronger cross-region guarantees, consider a transactional outbox and a consumer-side deduplication store. Kafka transactions are local to a cluster and do not automatically span Cluster Linking.

## Schema Registry

For structured events:

- Use Confluent Schema Registry rather than ad hoc JSON.
- Pick Avro, Protobuf, or JSON Schema and enforce a compatibility mode.
- Register schemas in the same region as the cluster, or use a global Schema Registry strategy.
- Include the schema ID in the serialized payload, not only in headers.
- Test schema evolution in CI with the same compatibility policy as production.

The sample code sends strings and JSON for clarity. In production, add a serializer and deserializer at the client boundary.

## Throughput and latency

- Increase `linger.ms` for throughput, decrease it for latency.
- Use `zstd` unless the consumer ecosystem requires another codec.
- Size partitions for peak consumer parallelism, not just current volume.
- Watch producer queue depth and `delivery.timeout.ms` failures.
- Keep message sizes below the topic `max.message.bytes` and the broker limit.
- Use compression for large payloads; it reduces network and storage cost but adds CPU.

## Anti-patterns

- Producing to a mirror topic.
- Using a custom DNS name that does not match the Confluent TLS certificate.
- Setting `acks=0` or `acks=1` for business-critical events.
- Turning off idempotence to "improve throughput" without measuring.
- Retrying a failed publish forever inside the HTTP request.
- Generating a new event ID on every retry.
- Assuming Kafka transactions span regions.
- Using one producer instance per request instead of a shared singleton.

## References

- [Confluent Cloud producer configuration](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
- [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [Cloud Run services](https://cloud.google.com/run/docs/deploying)
