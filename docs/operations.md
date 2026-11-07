# Operations and security

## Service level indicators

Define SLIs around user-visible behavior, not only process health.

| SLI | Measurement |
| --- | --- |
| Publish availability | Successful `POST /publish` responses divided by total requests |
| Publish latency | p50, p95, and p99 from request start to Kafka acknowledgment |
| Publish durability | Records acknowledged by Kafka and later observed by a consumer |
| Consumer freshness | Time from event creation to successful processing |
| Consumer lag | Records behind the high watermark per partition |
| Processing error rate | Handler failures divided by records processed |
| Edge routing availability | Healthy Cloudflare origin pools and successful origin health checks |
| Failover readiness | Cluster Linking lag, Cloudflare pool health, and last successful promotion test |

## Alerts

Alert on symptoms first:

- Producer 5xx rate above a small threshold for five minutes.
- Producer p99 latency above the budget.
- Cloudflare origin pool unhealthy or health checks failing.
- Cloudflare or regional ALB 5xx rate above a small threshold.
- Consumer lag growing for fifteen minutes and not recovering.
- No consumer progress for a topic that should be active.
- Cluster Linking lag above the RPO target.
- PSC endpoint or DNS resolution failures.
- Authentication failures, which usually indicate a bad or expired credential.

Add cause-based alerts for capacity, but do not page on every CPU spike.

## Structured logging

The reference logger emits JSON with:

- `timestamp`
- `severity`
- `message`
- `service`
- `revision`
- `component`
- `eventId`, `topic`, `partition`, `offset` where relevant

Log correlation IDs at the HTTP boundary and propagate them through Kafka headers. Avoid logging full payloads; they may contain personal or regulated data.

## Tracing

- Accept a `traceparent` header at the producer service.
- Store trace context in Kafka message headers.
- Continue the trace in the consumer.
- Sample at a rate that keeps cost reasonable while retaining error traces.
- Correlate Kafka offsets with trace IDs for debugging.

## Security

### Credentials

- Use a separate Confluent API key per workload and environment.
- Store keys in Secret Manager.
- Grant the Cloud Run runtime service account access only to the secrets it needs.
- Rotate keys on a schedule and test overlapping validity.
- Never commit `.env` files, keys, or service account JSON.

### Authorization

- Use Confluent ACLs to restrict each producer and consumer to its topics and consumer group.
- Give admin credentials only to provisioning jobs, not application services.
- Use separate service accounts for the producer and consumer if their permissions differ.
- Review ACLs after topic or team changes.

### Network

- Keep Kafka traffic private through PSC.
- Restrict egress with firewall rules.
- Terminate public HTTP traffic at Cloudflare with WAF, DDoS protection, and Load Balancing.
- Put a regional external ALB and serverless NEG in front of each regional Cloud Run producer service.
- Use Cloud Run ingress `internal-and-cloud-load-balancing`; do not expose the default Cloud Run URL.
- If the origin allows unauthenticated requests, that is only acceptable because ingress is restricted to the ALB. Enforce end-user authentication at Cloudflare or an identity-aware proxy if needed.
- Restrict the regional ALBs to Cloudflare IP ranges or Authenticated Origin Pulls. If you use Cloud Armor, use it only as an origin allowlist; Cloudflare is the primary WAF.
- Do not put Kafka bootstrap traffic behind Cloudflare or the HTTP load balancers.
- Consider VPC Service Controls for data exfiltration controls.
- Do not disable TLS verification.

### Supply chain

- Pin dependencies and commit the lockfile.
- Run `npm ci` in CI and in the image build.
- Scan images and dependencies.
- Use a non-root container user.
- Keep the Node.js base image patched.

## Topic governance

| Setting | Recommendation |
| --- | --- |
| Replication factor | 3 for production |
| `min.insync.replicas` | 2 |
| Partitions | Size for peak consumer parallelism; increase only |
| Retention | Match replay and compliance requirements |
| Cleanup policy | `delete` for event streams; `compact` for state topics |
| Key | Business key for ordering; globally unique event ID for deduplication |
| Naming | Environment and domain prefixes, version suffix for breaking schema changes |

Create topics through the provisioning script or infrastructure as code, not with `allowAutoTopicCreation` in production.

## Capacity planning

- Estimate peak records per second and average record size.
- Multiply by replication factor for broker-side storage and network.
- Account for Cluster Linking replication traffic.
- Size partitions for the target consumer parallelism.
- Size Cloud Run egress subnets for the maximum instance count.
- Size the regional proxy-only subnets for normal plus failover traffic.
- Check PSC endpoint throughput limits.
- Check Cloudflare Load Balancing pool and health-check limits.
- Keep at least N+1 capacity in each active region and leave headroom for failover, replay, and schema migrations.
- Load test with realistic keys and partition distribution.

## Testing

- Unit test serialization, validation, and idempotency keys.
- Integration test against a real Kafka cluster or a compatible local stack.
- Test consumer rebalances by adding and removing instances.
- Test poison-message handling and DLQ routing.
- Test graceful shutdown under load.
- Test PSC connectivity from a Cloud Run job in each region.
- Test Cloudflare origin pool failover and failback.
- Test Kafka-only failure and confirm `/readyz` turns unhealthy.
- Test direct access to the regional ALB from outside Cloudflare and confirm it is blocked.
- Run a multi-region failover game day.
- Test credential rotation.

## Incident runbook

### Producer cannot publish

1. Check Cloudflare Load Balancing pool health and origin health-check status.
2. Check the regional ALB 5xx rate and Cloud Run 5xx rate and logs.
3. Check DNS resolution of the bootstrap and broker hostnames.
4. Check PSC forwarding rule state and firewall rules.
5. Check Confluent API key validity and topic ACLs.
6. Check producer queue depth and delivery timeouts.
7. If the region is unhealthy, follow the failover runbook.

### Kafka-only failure

1. Confirm whether `/readyz` on the primary origin is failing.
2. If `/readyz` remains healthy, the readiness check is not Kafka-aware. Disable the primary Cloudflare pool manually and fix the probe.
3. If `/readyz` fails, confirm Cloudflare moves HTTP traffic to the secondary pool.
4. Promote the secondary mirror topics and switch Kafka clients to the secondary bootstrap endpoint.
5. Do not assume Cloudflare failover moved Kafka clients or promoted topics.

### Cloudflare edge or origin failure

1. Check the Cloudflare Load Balancer status and each origin pool.
2. Check the health-check path (`/readyz`), expected status, Host header, and WAF exemptions.
3. Check the regional ALB forwarding rule, HTTPS proxy, certificate, serverless NEG, and Cloud Run service.
4. Confirm the origin is not blocked by Cloudflare IP allowlisting or Authenticated Origin Pulls.
5. If one origin is unhealthy, confirm traffic is served by the other pool.
6. If Cloudflare itself is degraded, follow the documented break-glass path.
7. Do not change Kafka bootstrap settings in response to an HTTP edge failure unless the Kafka region is also failed.

### Consumer lag is growing

1. Check instance count, partition count, and `partitionsConsumedConcurrently`.
2. Check handler latency and downstream dependency health.
3. Check rebalance count and commit failures.
4. Check for a hot partition or uneven key distribution.
5. Check for repeated reprocessing from unresolved offsets.
6. Scale instances up to the partition count, or fix the bottleneck.

### Rebalance storm

1. Check for instances restarting or failing liveness probes.
2. Check batch processing time against `max.poll.interval.ms`.
3. Check network instability and broker health.
4. Check for mixed classic and consumer group protocols.
5. Reduce batch size or processing time, then stabilize the group.

### Authentication failure

1. Verify the secret version referenced by the service.
2. Verify the API key is active and has the required ACLs.
3. Check for clock skew if using time-based credentials.
4. Rotate the key if it is compromised.
5. Redeploy with the new secret version.

### DNS or PSC failure

1. Resolve the Confluent hostname from a VPC VM or Cloud Run job.
2. Verify the private zone is attached to the correct VPC.
3. Verify the A records point at the PSC endpoint IPs.
4. Verify the forwarding rules and service attachments are healthy.
5. Verify Global Access and firewall rules for cross-region paths.
6. Fail over to the secondary region if the primary path is unavailable.

## Cost control

- Set `min-instances` from the availability target, not just cost. Resilience-first uses at least two warm instances per active region.
- Use instance-based billing only where background work requires it.
- Keep both regions warm when you need automatic failover. Remove warm capacity only with explicit risk acceptance.
- Avoid cross-region Kafka traffic in steady state.
- Compress events.
- Scale consumers to partition count, not beyond.
- Set retention based on replay needs, not indefinitely.
- Review idle clusters, topics, PSC endpoints, proxy-only subnets, and unused Cloudflare origin pools.

## Change management

Changes that need a game day or a rollback plan:

- Consumer group protocol changes.
- Partition count changes.
- Schema compatibility changes.
- Cluster Linking topology changes.
- Cloudflare Load Balancing pool, steering, health-check, or WAF changes.
- Regional external ALB, serverless NEG, certificate, or proxy-only subnet changes.
- Cloud Run ingress or readiness-probe changes.
- DNS or PSC changes.
- Producer or consumer upgrade to a new client version.
- Changes to commit strategy or batch size.

Deploy configuration changes with a canary and watch lag, error rate, and latency before rolling out to all instances.
