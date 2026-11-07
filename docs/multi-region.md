# Multi-region and failover

## Options

| Option | Data path | Complexity | Use when |
| --- | --- | --- | --- |
| Single cluster, global access | Every region connects to one cluster | Low | Small workloads, strict single-writer requirements, latency is acceptable |
| One cluster per region, active-passive | Local cluster in primary; linked standby | Medium | Regional failover with a clear primary |
| One cluster per region, active-active | Local cluster in every region; bidirectional links | High | Regional writes must stay local; global aggregation is required |

The resilience-first default is one cluster per region with Cluster Linking and both regions warm. Prefer active-active where the domain can handle multi-writer deduplication and fencing. Otherwise use warm active-passive with pre-created secondary resources; do not rely on cold standby.

For the HTTP routing layer, concrete Cloudflare and regional ALB names, and the Cloud Run readiness pattern, see [Regional failover and Cloudflare edge routing](failover-routing.md).

## Resilience-first default

- Run at least two active regions. For tier-0, add a third region.
- Size each region for full peak load, not a fraction of peak. Keep N+1 capacity at minimum.
- Use Confluent Dedicated or Enterprise multi-zone clusters. Confirm the current SLA for your cluster type; multi-zone Dedicated is 99.99%, while Basic is 99.5%.
- Use bidirectional Cluster Linking for active-active, or source-to-secondary linking plus a reverse link for failback in active-passive.
- Enable consumer offset and ACL synchronization where supported, and verify it in a game day.
- Keep Cloud Run minimum instances at 2 or more in each active region. Do not scale the producer to zero.
- Keep PSC local in every region across all three zones. Use PSC Global Access only as a cross-region fallback.
- Put both regional origins in Cloudflare Load Balancing and keep both pools healthy. Do not use DNS-only failover.
- Test the Cloudflare edge dependency. For tier-0, document a break-glass path that does not depend on the same Cloudflare load balancer.

## Cluster Linking

Cluster Linking replicates topics from a source cluster to a destination cluster.

Key properties:

- A link is directional.
- Replicated topics are mirror topics.
- Mirror topics are read-only on the destination.
- Promoting a mirror topic makes it writable for failover.
- Replication is asynchronous, so there is a non-zero replication lag and RPO.
- Schema Registry is separate. Plan schema compatibility across regions.
- Consumer group offset synchronization depends on the feature and configuration. Verify it for your Confluent clusters; do not assume offsets translate automatically.

### Do not write to mirror topics

Producers must write to the source cluster. Writing to a mirror topic is not supported and can create loops or data loss.

### Topic naming

For active-passive, the same topic name on both clusters is convenient because the application can switch clusters without changing topic names. The mirror topic on the destination has the same name.

For active-active, use a deliberate naming strategy:

- Separate mirror topics per direction, or
- Separate logical topic namespaces per region, or
- A single topic with regional keys and deduplication.

Bidirectional links with the same topic names can create replication loops unless the linking topology is designed to prevent them. Test the topology before production.

## Active-passive

### Steady state

- Producers and consumers use the local cluster in the primary region.
- Cluster Linking replicates topics to the secondary region.
- Schema Registry is available in both regions or globally.
- Monitoring tracks link lag continuously.

### Failover

1. Declare the failure and stop producers in the primary region if they are still reachable. Fencing the old producers is essential; otherwise they may continue writing to the old primary during a network partition.
2. Check the Cluster Linking lag. Record the last replicated offset for each topic. This is the RPO.
3. Promote the mirror topics in the secondary cluster.
4. Switch the application configuration to the secondary bootstrap endpoint. This can be an environment variable, a secret version, a ConfigMap, or a redeploy.
5. Confirm Cloudflare has moved HTTP traffic to the secondary origin pool. If it has not, disable the primary pool manually.
6. Restart or redeploy producers and consumers. Kafka clients cache metadata and existing connections; a DNS change alone does not move them immediately.
7. Validate consumer startup position. If consumer offsets were not synchronized, choose a rewind policy or replay from a timestamp.
8. Monitor duplicates, lag, and error rates.
9. Keep the old primary isolated until it is safe to rebuild.

### Failback

1. Re-establish a link in the reverse direction.
2. Wait for replication to catch up.
3. Quiesce writes in the secondary.
4. Promote the reverse mirror topics in the original primary.
5. Switch clients back.
6. Validate offsets and duplicates again.

Failback is a second failover. Test it, not just the first direction.

## Active-active

### Steady state

- Each region produces to its local cluster.
- Links replicate data in both directions.
- Consumers process local and replicated data according to the aggregation design.
- Every event has a globally unique `eventId`.
- Downstream stores are idempotent and can resolve conflicts.

### Ordering

Kafka ordering is per partition per cluster. There is no global ordering across regions. If an entity can be updated in two regions, define:

- The authoritative region for that entity.
- A conflict resolution rule (last write wins with a version, region priority, or domain-specific merge).
- How consumers handle out-of-order updates.

### Consumer groups

Do not run the same consumer group ID against two clusters and expect one global group. Consumer groups are cluster-scoped.

Options:

- One group per region, with a shared deduplication store.
- A single active consumer region for a given aggregate.
- A stream processor that merges linked topics with event-time semantics.

### Producer fencing

Active-active does not remove the need for fencing during a partition. If both regions can write the same entity, define which region wins and how the losing write is rejected or merged.

## DNS and client behavior

- Use the real Confluent bootstrap hostname for each region.
- Override DNS privately to the regional PSC endpoints.
- Keep regional hostnames separate. Do not point both regions at the same cluster unless that is the design.
- Keep the Kafka DNS path separate from the public HTTP path. `producer.example.com` is a Cloudflare HTTP hostname; it is not a Kafka bootstrap endpoint.
- Kafka clients resolve bootstrap and broker hostnames, cache metadata, and keep long-lived connections. A DNS change does not immediately move an existing producer or consumer.
- For failover, update the application configuration and restart or redeploy the clients. DNS-based failover can reduce the config change, but it still needs client reconnection and testing.

Cloudflare Load Balancing handles HTTP origins only. There is no Cloudflare Kafka proxy in this design. Do not put Kafka bootstrap traffic behind Cloudflare Load Balancing.

If you use a global DNS name with health-checked failover for a non-Kafka endpoint, verify:

- The TTL is low enough for your RTO.
- The client re-resolves after a connection failure.
- Broker metadata points at the new cluster.
- TLS hostnames still match.

## RPO and RTO

| Metric | Driver |
| --- | --- |
| RPO | Cluster Linking replication lag at the moment of failure |
| RTO | Detection + decision + mirror promotion + client restart + validation |

Reduce RPO with:

- Healthy links and sufficient link throughput.
- Monitoring link lag and alerting before it grows.
- Avoiding cross-region dependencies in the primary path.

Reduce RTO with:

- Runbooks and automation for promotion, Cloudflare pool failover, and client restart.
- Pre-created secondary resources, secrets, Cloudflare origin pools, and regional ALBs.
- Regular game days.
- Warm consumers and producers in the secondary. Keep both Cloudflare origin pools healthy.

## Consumer offsets on failover

Consumer group offsets are a common failure point. Decide in advance:

- Are offsets synchronized by Cluster Linking? Verify the feature and configuration.
- If not, do you start at the earliest offset, the latest offset, or a timestamp?
- Do you accept duplicates, or do you need a deduplication store?
- How do you prevent the old consumer group from processing after failover?

A consumer that starts at `latest` after failover may skip events. A consumer that starts at `earliest` may replay a large history. Neither is universally correct; choose based on the business impact.

## Security and networking parity

- Create the same PSC, DNS, firewall, Cloudflare, and Cloud Run configuration in every region.
- Create one Cloudflare origin pool and one regional external ALB per active region.
- Restrict regional ALB origins to Cloudflare IP ranges or Authenticated Origin Pulls. Do not expose the Cloud Run default URL.
- Keep the same WAF and DDoS policy on the public hostname in every region.
- Enable PSC Global Access only where cross-region access is required; do not use it as the primary failover mechanism.
- Use separate API keys per region and environment.
- Keep secrets replicated to the regions that need them.
- Ensure the Schema Registry endpoint is reachable from each region.
- Test IAM and ACLs in the secondary before an incident.

## Testing

Run an edge and Kafka failover game day:

1. Produce a known stream with sequence numbers.
2. Measure link lag under load.
3. Disable the primary Cloudflare origin pool and verify HTTP traffic moves to the secondary ALB.
4. Simulate a Kafka-only failure in the primary region and verify `/readyz` fails and Cloudflare moves traffic.
5. Call a regional ALB IP directly from outside Cloudflare and verify the origin blocks it.
6. Simulate a full regional failure.
7. Execute the runbook with a timer.
8. Measure actual RPO and RTO.
9. Verify duplicates, ordering, and consumer position.
10. Fail back.
11. Record gaps and fix the runbook.

Test during peak load, not only in a quiet environment.

## Anti-patterns

- Producing to a mirror topic.
- Assuming Cluster Linking is synchronous.
- Assuming consumer offsets are globally consistent.
- Running the same group ID against two clusters without fencing.
- Switching DNS and expecting existing clients to move immediately.
- Building an active-active topology without a conflict resolution rule.
- Forgetting Schema Registry in the failover plan.
- Testing failover only in one direction.
- Using cold standby or scale-to-zero for the producer when you need automatic failover.
- Using `/healthz` or a process-only `/readyz` as the Cloudflare health check.
- Putting Kafka bootstrap traffic behind Cloudflare or an HTTP load balancer.
- Leaving regional ALB IPs reachable from the public internet without Cloudflare origin protection.
- Assuming Cloudflare Load Balancing promotes topics or moves consumer offsets.

## References

- [Cloudflare Load Balancing](https://developers.cloudflare.com/load-balancing/)
- [Regional external Application Load Balancer with Cloud Run](https://docs.cloud.google.com/load-balancing/docs/https/setting-up-reg-ext-https-serverless)
- [Confluent Cloud Cluster Linking](https://docs.confluent.io/cloud/current/clusters/cluster-linking/index.html)
- [Confluent multi-region clusters](https://docs.confluent.io/cloud/current/multi-zone/multiregion-cluster.html)
- [Cloud DNS private zones](https://cloud.google.com/dns/docs/zones)
- [PSC global access](https://cloud.google.com/vpc/docs/about-accessing-vpc-hosted-services-endpoints)
