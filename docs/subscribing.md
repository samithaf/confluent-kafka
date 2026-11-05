# Subscribing

## Delivery model

The reference consumer uses manual commits and batch processing. It gives at-least-once delivery:

- A record is processed before its offset is resolved.
- Resolved offsets are committed periodically and at the end of a batch.
- A crash or rebalance after processing but before a commit causes reprocessing.
- A partition revoke commits resolved offsets so the next owner does not repeat work unnecessarily.

The consumer handler must be idempotent.

## Consumer group protocol

Confluent recommends the new consumer group protocol (`group.protocol=consumer`, KIP-848). It is the default in the reference code.

With `group.protocol=consumer`:

- Do not set `partition.assignment.strategy`, `session.timeout.ms`, or `heartbeat.interval.ms`. These classic options are not supported.
- The broker manages group membership and heartbeats.
- Assignment behavior is controlled by the broker and the group consumer configuration.

With `group.protocol=classic`:

- Use `partition.assignment.strategy=cooperative-sticky` for incremental rebalances.
- Set `session.timeout.ms` and `heartbeat.interval.ms` explicitly.
- Keep the cooperative protocol across all group members. Do not mix eager and cooperative assignors.

The code in [`src/kafka.ts`](../src/kafka.ts) applies these settings conditionally based on `KAFKA_GROUP_PROTOCOL`.

Verify that your Confluent cluster and client version support `group.protocol=consumer` before making it the default in production. If they do not, set `KAFKA_GROUP_PROTOCOL=classic`.

## Recommended consumer configuration

| Setting | Value | Why |
| --- | --- | --- |
| `group.id` | required | Consumer group identity |
| `group.protocol` | `consumer` | New group protocol; use `classic` only for compatibility |
| `enable.auto.commit` | `false` | Commit only after processing |
| `auto.offset.reset` | `earliest` | Do not silently skip records for a new group |
| `fetch.wait.max.ms` | `500` | Balance latency and batching |
| `max.poll.interval.ms` | `300000` | Allow long batches; keep processing below this bound |
| `client.dns.lookup` | `use_all_dns_ips` | Use all PSC endpoint addresses |
| `socket.keepalive.enable` | `true` | Detect dead connections |

Classic-only additions:

| Setting | Value |
| --- | --- |
| `partition.assignment.strategy` | `cooperative-sticky` |
| `session.timeout.ms` | `45000` |
| `heartbeat.interval.ms` | `3000` |

## Offset management in `@confluentinc/kafka-javascript`

The library's KafkaJS-compatible consumer:

- Forces `enable.auto.offset.store=false`.
- Stores offsets when you call `resolveOffset(offset)`.
- Commits stored offsets when you call `commitOffsets()` or `commitOffsetsIfNecessary()`.
- Handles the default partition assign/unassign behavior internally.

This means the correct order is:

```text
process record
  -> resolveOffset(record.offset)
  -> commitOffsetsIfNecessary() periodically
```

If processing throws before `resolveOffset`, the library seeks back to the first unresolved offset. The batch is reprocessed.

### Rebalance callback

The library wraps the user `rebalance_cb`. The callback receives:

```text
(error, assignment, { assign, unassign, assignmentLost })
```

The library awaits the callback and applies the default assignment behavior unless you call the assignment functions yourself.

The reference callback commits resolved offsets on `ERR__REVOKE_PARTITIONS` and lets the library handle assign/unassign:

```text
if error is ERR__REVOKE_PARTITIONS:
  commitOffsets()
```

This reduces duplicate work when partitions move. It does not guarantee exactly-once processing.

Do not call `assign` or `unassign` in the callback unless you are intentionally implementing custom assignment. Mixing custom assignment with the library's default behavior can cause duplicate or missing partitions.

## Batch processing

The reference worker in [`src/consumer-worker.ts`](../src/consumer-worker.ts) uses:

```text
eachBatchAutoResolve: false
```

and resolves each record after it is processed. This prevents one failed record from forcing the whole batch to be reprocessed. Only the unresolved suffix is retried.

Key points:

- Check `isRunning()` and `isStale()` before processing each record.
- Call `heartbeat()` during long batches.
- Commit every `KAFKA_COMMIT_INTERVAL_MESSAGES` records and once at the end.
- If the batch becomes stale, stop processing and let the new owner take over.
- Keep total batch processing time below `max.poll.interval.ms`.

## Error handling

### Retriable processing errors

Throw from the batch handler. The library seeks back to the first unresolved offset and the batch is retried. Add a bounded retry or backoff in your business logic; do not spin on a permanent failure forever.

### Poison messages

Use one of:

- A retry topic with a delay and an attempt counter.
- A dead-letter topic after N attempts.
- A database record of failed event IDs with manual replay.

If you route a poison message to a DLQ, resolve its offset only after the DLQ write succeeds. Then continue the batch.

### Backpressure

Use `pause()` when a downstream dependency is unavailable. The batch payload exposes `pause()` in both `eachMessage` and `eachBatch`. Resume when the dependency recovers.

### Consumer lag

Lag is normal during bursts. Sustained lag means:

- Not enough instances or partitions.
- A slow downstream dependency.
- Large batches or long processing times.
- Repeated reprocessing from uncommitted offsets.
- A hot partition caused by a poor message key.

## Scaling

Consumer parallelism is bounded by the number of partitions. Within an instance, `partitionsConsumedConcurrently` controls how many partitions are processed in parallel. Across instances, the group protocol assigns partitions.

Rules of thumb:

- Set `partitionsConsumedConcurrently` to the number of CPUs available for processing, capped by partitions per instance.
- Scale instances up to the partition count. Extra instances sit idle.
- Increase partitions only by adding them; Kafka does not support reducing partition count.
- Adding partitions changes key-to-partition mapping. Plan for ordering and migration impacts.
- Use worker pools for background consumers. They have no request concurrency or HTTP port.
- Use CREMA or a KEDA Kafka scaler for lag-based autoscaling. Scale on lag, not CPU alone.

## Graceful shutdown

Cloud Run sends SIGTERM before terminating an instance. The reference worker:

1. Stops fetching with `consumer.stop()`.
2. Waits for the run loop to exit.
3. Commits final offsets.
4. Disconnects.
5. Exits within `SHUTDOWN_TIMEOUT_MS`.

Rebalance commits and shutdown commits are complementary. The rebalance callback handles partitions moving to another instance while the process is still running. The shutdown path handles a normal scale-in or revision replacement.

## Multi-region consuming

### Active-passive

- Consume from the local cluster in steady state.
- On failover, point the consumer at the secondary cluster.
- Decide whether to resume from the committed offset, rewind, or replay from a timestamp.
- Test consumer group offset translation before relying on it. Do not assume offsets map one-to-one across clusters.
- Do not run the same consumer group against two clusters at the same time unless you have a fencing strategy. Split-brain consumers can process conflicting partitions.

### Active-active

- Produce to the local cluster in each region.
- Consume local and linked data depending on the aggregation requirement.
- Deduplicate by `eventId`.
- Do not assume global ordering across regions. Ordering is per partition per cluster.
- Define conflict resolution for updates to the same entity.
- Account for replication lag in read models and projections.

## Observability

Track:

- Consumer lag per topic and partition.
- Commit failures and rebalance count.
- Batch processing latency and records per batch.
- Handler error rate and DLQ volume.
- Instance count versus partition count.
- `max.poll.interval.ms` violations.

Log:

- Topic, partition, offset, event ID, and consumer group.
- Rebalance events with the number of assigned or revoked partitions.
- Shutdown progress.

Do not log full payloads by default. They may contain sensitive data.

## Anti-patterns

- Auto-committing before processing.
- Committing every record in a high-throughput topic without measuring cost.
- Ignoring `isStale()` and processing records after a revoke.
- Processing a batch longer than `max.poll.interval.ms`.
- Setting `auto.offset.reset=latest` for a new business consumer and silently skipping history.
- Scaling consumers beyond the partition count and expecting more throughput.
- Running the same group ID against multiple clusters without fencing.
- Treating Kafka as exactly-once without idempotent writes.

## References

- [Confluent Cloud consumer configuration](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [Cloud Run worker pools](https://cloud.google.com/run/docs/configuring/workerpools)
