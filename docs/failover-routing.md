# Regional failover and Cloudflare edge routing

This document describes the HTTP publish path and the regional failover pattern. Cloudflare is the public edge for WAF, DDoS protection, TLS, and global load balancing. GCP runs a regional external Application Load Balancer with a serverless NEG in front of each regional Cloud Run producer service.

This reference assumes Cloudflare Load Balancing is available. If you only have Cloudflare proxied DNS and WAF, keep a GCP global external Application Load Balancer behind Cloudflare for regional failover, or add Cloudflare Load Balancing before relying on this pattern.

The edge only handles HTTP producer traffic. It does not route the Kafka protocol and it does not promote mirror topics. Kafka failover is a separate operation.

## Concrete names

| Resource | Name |
| --- | --- |
| Cloudflare Load Balancer | `kafka-producer-edge-lb` |
| Cloudflare origin pool, primary | `kafka-producer-pool-us-central1` |
| Cloudflare origin pool, secondary | `kafka-producer-pool-us-east1` |
| Public hostname | `producer.example.com` |
| Origin hostname, primary | `origin-us-central1.example.com` |
| Origin hostname, secondary | `origin-us-east1.example.com` |
| Cloud Run producer service, both regions | `kafka-producer-api` |
| Regional external ALB, primary | `kafka-producer-lb-us-central1` |
| Regional external ALB, secondary | `kafka-producer-lb-us-east1` |
| Regional static IP, primary | `kafka-producer-ip-us-central1` |
| Regional static IP, secondary | `kafka-producer-ip-us-east1` |
| Backend service, primary | `kafka-producer-backend-us-central1` |
| Backend service, secondary | `kafka-producer-backend-us-east1` |
| URL map, primary | `kafka-producer-url-map-us-central1` |
| URL map, secondary | `kafka-producer-url-map-us-east1` |
| HTTPS proxy, primary | `kafka-producer-https-proxy-us-central1` |
| HTTPS proxy, secondary | `kafka-producer-https-proxy-us-east1` |
| TLS certificate, primary | `kafka-producer-cert-us-central1` |
| TLS certificate, secondary | `kafka-producer-cert-us-east1` |
| Forwarding rule, primary | `kafka-producer-fr-us-central1` |
| Forwarding rule, secondary | `kafka-producer-fr-us-east1` |
| Serverless NEG, primary | `kafka-producer-neg-us-central1` |
| Serverless NEG, secondary | `kafka-producer-neg-us-east1` |
| Proxy-only subnet, primary | `kafka-proxy-us-central1` |
| Proxy-only subnet, secondary | `kafka-proxy-us-east1` |
| Cloud Run egress subnet, primary | `kafka-run-us-central1` |
| Cloud Run egress subnet, secondary | `kafka-run-us-east1` |
| PSC subnet, primary | `kafka-psc-us-central1` |
| PSC subnet, secondary | `kafka-psc-us-east1` |
| Consumer worker pool, primary | `kafka-consumer-worker` in `us-central1` |
| Consumer worker pool, secondary | `kafka-consumer-worker` in `us-east1` |

The Cloudflare Load Balancer is **`kafka-producer-edge-lb`**; clients use **`producer.example.com`**. Cloudflare terminates the public TLS connection and selects a healthy origin pool. Each origin pool points at a regional external ALB, which forwards to the regional Cloud Run service through a serverless NEG.

## Routing layers

| Layer | Primary | Secondary | Failover mechanism |
| --- | --- | --- | --- |
| Client to API | `producer.example.com` at Cloudflare | Same hostname | Cloudflare Load Balancing |
| Edge security | Cloudflare WAF and DDoS | Same | Cloudflare |
| Regional ingress | `kafka-producer-lb-us-central1` | `kafka-producer-lb-us-east1` | Cloudflare origin health checks |
| HTTP backend | `kafka-producer-neg-us-central1` | `kafka-producer-neg-us-east1` | Serverless NEG |
| Cloud Run | `kafka-producer-api` in `us-central1` | `kafka-producer-api` in `us-east1` | Cloudflare origin health check on `/readyz` |
| VPC egress | `kafka-run-us-central1` | `kafka-run-us-east1` | Region-local Direct VPC egress |
| PSC | `kafka-psc-us-central1` endpoints | `kafka-psc-us-east1` endpoints | Not global; use the surviving region |
| Kafka | Primary Confluent cluster | Secondary Confluent cluster | Cluster Linking promotion and client switch |

## What happens during a regional failure

1. The primary region becomes unavailable, or `kafka-producer-api` fails its readiness probe.
2. Cloudflare health checks mark the primary origin pool unhealthy.
3. Cloudflare Load Balancing stops sending new requests to `kafka-producer-pool-us-central1` and sends them to `kafka-producer-pool-us-east1`.
4. The secondary regional external ALB forwards the request to `kafka-producer-neg-us-east1`.
5. The secondary `kafka-producer-api` handles the request using its own environment:
   - `KAFKA_BOOTSTRAP_SERVERS` points at the secondary Confluent cluster.
   - Direct VPC egress uses `kafka-run-us-east1`.
   - Traffic goes to the secondary PSC endpoints in `kafka-psc-us-east1`.
6. The Kafka data plane is failed over separately: promote the secondary mirror topics and start consumers against the secondary cluster.

Cloudflare does not send Kafka traffic. Kafka clients use the regional Confluent bootstrap hostnames, not `producer.example.com`.

## Deploy the producer API in both regions

Each region needs its own bootstrap server and credentials. Deploy with region-specific environment variables rather than one multi-region command.

Primary:

```bash
gcloud run deploy kafka-producer-api \
  --image us-central1-docker.pkg.dev/PROJECT_ID/containers/kafka-app:latest \
  --region us-central1 \
  --network kafka-vpc \
  --subnet kafka-run-us-central1 \
  --vpc-egress private-ranges-only \
  --allow-unauthenticated \
  --ingress internal-and-cloud-load-balancing \
  --min-instances 2 \
  --max-instances 50 \
  --no-cpu-throttling \
  --readiness-probe httpGet.path=/readyz,httpGet.port=8080,successThreshold=2,failureThreshold=3,timeoutSeconds=2,periodSeconds=10 \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=pkc-primary.us-central1.gcp.confluent.cloud:9092,KAFKA_TOPIC=orders.v1 \
  --set-secrets KAFKA_API_KEY=kafka-primary-api-key:latest,KAFKA_API_SECRET=kafka-primary-api-secret:latest
```

Secondary:

```bash
gcloud run deploy kafka-producer-api \
  --image us-central1-docker.pkg.dev/PROJECT_ID/containers/kafka-app:latest \
  --region us-east1 \
  --network kafka-vpc \
  --subnet kafka-run-us-east1 \
  --vpc-egress private-ranges-only \
  --allow-unauthenticated \
  --ingress internal-and-cloud-load-balancing \
  --min-instances 2 \
  --max-instances 50 \
  --no-cpu-throttling \
  --readiness-probe httpGet.path=/readyz,httpGet.port=8080,successThreshold=2,failureThreshold=3,timeoutSeconds=2,periodSeconds=10 \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=pkc-secondary.us-east1.gcp.confluent.cloud:9092,KAFKA_TOPIC=orders.v1 \
  --set-secrets KAFKA_API_KEY=kafka-secondary-api-key:latest,KAFKA_API_SECRET=kafka-secondary-api-secret:latest
```

For resilience-first operation, keep both regions warm and size each region for the full peak load. Do not rely on scale-to-zero in the secondary region. Keep `--min-instances` at 2 or more so a single instance failure does not make the origin pool unhealthy.

The Cloud Run service allows unauthenticated requests at the origin because ingress is restricted to `internal-and-cloud-load-balancing` and the public edge is Cloudflare. If you need end-user authentication, enforce it at Cloudflare or add an identity-aware proxy in front of the ALB. Do not expose the default Cloud Run URL.

## Readiness must reflect Kafka, not just the process

Cloudflare Load Balancing cannot read Cloud Run service health or serverless NEG health directly. It only sees the HTTP response from the readiness endpoint. Keep the readiness endpoint unauthenticated at the origin and return `200` only when the instance can publish.

- `/healthz` should answer whether the process is alive.
- `/readyz` should answer whether the instance can publish to Kafka.

A readiness endpoint that only checks that the HTTP server started will keep the primary origin pool "healthy" even when its Kafka cluster is unreachable. That prevents automatic failover for a Kafka-only regional failure.

In production, make `/readyz` perform a bounded Kafka metadata check, for example a short-timeout `fetchTopicMetadata` or an equivalent producer connectivity check. Cache the result briefly so every probe does not create a broker round trip.

The sample service exposes `/readyz` for the initial connection. Add a live Kafka check before relying on it as the sole automated failover signal.

## Create the proxy-only subnets

Regional external Application Load Balancers use a regional managed proxy subnet. Create one in each region and size it for normal traffic plus failover traffic:

```bash
gcloud compute networks subnets create kafka-proxy-us-central1 \
  --purpose REGIONAL_MANAGED_PROXY \
  --role ACTIVE \
  --region us-central1 \
  --network kafka-vpc \
  --range 10.10.32.0/23

gcloud compute networks subnets create kafka-proxy-us-east1 \
  --purpose REGIONAL_MANAGED_PROXY \
  --role ACTIVE \
  --region us-east1 \
  --network kafka-vpc \
  --range 10.11.32.0/23
```

The proxy-only subnet is separate from the Cloud Run egress subnet and the PSC endpoint subnet.

## Create the serverless NEGs

```bash
gcloud compute network-endpoint-groups create kafka-producer-neg-us-central1 \
  --region us-central1 \
  --network-endpoint-type serverless \
  --cloud-run-service kafka-producer-api

gcloud compute network-endpoint-groups create kafka-producer-neg-us-east1 \
  --region us-east1 \
  --network-endpoint-type serverless \
  --cloud-run-service kafka-producer-api
```

A serverless NEG points to a Cloud Run service in the same region. The pattern uses one serverless NEG per regional service.

## Create the regional external ALBs

Repeat these commands for `us-east1` with the corresponding names and ranges.

Reserve the regional IP:

```bash
gcloud compute addresses create kafka-producer-ip-us-central1 \
  --region us-central1 \
  --network-tier PREMIUM \
  --ip-version IPV4
```

Create the backend service and attach the NEG:

```bash
gcloud compute backend-services create kafka-producer-backend-us-central1 \
  --load-balancing-scheme EXTERNAL_MANAGED \
  --protocol HTTP \
  --region us-central1

gcloud compute backend-services add-backend kafka-producer-backend-us-central1 \
  --region us-central1 \
  --network-endpoint-group kafka-producer-neg-us-central1 \
  --network-endpoint-group-region us-central1
```

Create the URL map:

```bash
gcloud compute url-maps create kafka-producer-url-map-us-central1 \
  --default-service kafka-producer-backend-us-central1 \
  --region us-central1
```

Create a regional certificate for the origin hostname. A Cloudflare Origin CA certificate is a good fit because Cloudflare is the only origin client; alternatively use a Certificate Manager certificate:

```bash
gcloud compute ssl-certificates create kafka-producer-cert-us-central1 \
  --certificate /path/to/origin-us-central1.crt \
  --private-key /path/to/origin-us-central1.key \
  --region us-central1
```

Create the HTTPS proxy and forwarding rule:

```bash
gcloud compute target-https-proxies create kafka-producer-https-proxy-us-central1 \
  --url-map kafka-producer-url-map-us-central1 \
  --ssl-certificates kafka-producer-cert-us-central1 \
  --region us-central1

gcloud compute forwarding-rules create kafka-producer-fr-us-central1 \
  --load-balancing-scheme EXTERNAL_MANAGED \
  --network-tier PREMIUM \
  --network kafka-vpc \
  --address kafka-producer-ip-us-central1 \
  --target-https-proxy kafka-producer-https-proxy-us-central1 \
  --target-https-proxy-region us-central1 \
  --region us-central1 \
  --ports 443
```

Read the regional IP addresses for Cloudflare origin records:

```bash
gcloud compute addresses describe kafka-producer-ip-us-central1 \
  --region us-central1 --format='value(address)'

gcloud compute addresses describe kafka-producer-ip-us-east1 \
  --region us-east1 --format='value(address)'
```

## Configure Cloudflare

1. Create DNS-only records:
   - `origin-us-central1.example.com` -> `kafka-producer-ip-us-central1`
   - `origin-us-east1.example.com` -> `kafka-producer-ip-us-east1`
2. Create origin pools:
   - `kafka-producer-pool-us-central1` with origin `origin-us-central1.example.com`
   - `kafka-producer-pool-us-east1` with origin `origin-us-east1.example.com`
3. Configure an HTTPS health check for each pool:
   - Path: `/readyz`
   - Host header: the origin hostname
   - Expected response: `200`
   - Use the shortest interval and lowest unhealthy threshold your Cloudflare plan supports.
4. Create the load balancer `kafka-producer-edge-lb` on `producer.example.com`.
5. Attach both origin pools. When both regions are warm, use a steering policy that serves both healthy pools (for example, dynamic or geo). For strict primary/secondary, use failover steering with the secondary as a warm standby.
6. Keep Cloudflare WAF, DDoS, and TLS settings enabled for `producer.example.com`.
7. Set SSL/TLS mode to **Full (strict)** so Cloudflare validates the regional ALB certificate.

If you use failover steering, send a small amount of trickle traffic to the secondary pool so the standby path is continuously validated.

## Protect the origins

Cloudflare is the public edge, not the origin.

- Do not expose the Cloud Run default URL to clients. Use `--ingress internal-and-cloud-load-balancing`.
- Restrict the regional ALBs to Cloudflare IP ranges. If you use Cloud Armor for this, use it only as an origin allowlist, not as the primary WAF.
- Prefer Cloudflare Authenticated Origin Pulls or mTLS where your GCP ALB configuration supports it.
- Do not put the regional ALB IPs in public documentation or client configuration.
- Keep Cloudflare health checks allowed through WAF rules.

For a private-origin design, use Cloudflare Tunnel with multiple connectors in each region and point the tunnel at an internal regional ALB. That adds connectors and another control plane, so use it only when public origin IPs are not acceptable.

## Test failover

Test the HTTP path:

1. Disable or mark `kafka-producer-pool-us-central1` unhealthy in Cloudflare.
2. Send requests to `https://producer.example.com`.
3. Confirm responses are served by the `us-east1` origin.
4. Restore the primary pool and confirm failback.

Test the Kafka-only path separately:

1. Make the primary Confluent cluster unreachable while `kafka-producer-api` in `us-central1` remains running.
2. Confirm `GET /readyz` on the primary origin returns a failure.
3. Confirm Cloudflare moves traffic to the secondary pool.

If `/readyz` stays healthy, Cloudflare will keep sending traffic to a producer that cannot publish.

Test origin bypass protection by calling the regional ALB IP directly with a non-Cloudflare source and confirming it is blocked.

## Consumer routing

Consumer worker pools have no HTTP endpoint and are not behind Cloudflare or the ALBs. Routing for consumers means:

1. Run `kafka-consumer-worker` in both regions.
2. Keep the secondary pool warm in active-passive, or run active-active with fencing and deduplication.
3. On failover, scale the secondary pool up and the primary pool down or isolate it.
4. Promote the secondary mirror topics.
5. Point the secondary workers at the secondary bootstrap endpoint.
6. Resume from synced consumer offsets if offset sync was enabled.

Do not run the same consumer group against both clusters without fencing. That creates split-brain processing.

## Failure matrix

| Failure | HTTP routing | Kafka routing |
| --- | --- | --- |
| One zone in a region | Cloudflare continues to the region; Cloud Run and Kafka use other zones/PSC endpoints | No cluster failover |
| GCP region only | Cloudflare sends to the healthy origin pool | Can stay on primary Kafka through Global Access, or fail over to secondary |
| Confluent region only | Cloudflare sends to the secondary pool only if `/readyz` fails | Promote secondary cluster |
| Whole region | Cloudflare sends to the secondary pool | Promote secondary cluster; resume consumers |
| Cloudflare origin pool error | Other healthy pool if active-active; outage if only one pool is healthy | Unchanged |
| Cloudflare global outage | No public edge; use a tested break-glass path | Unchanged |

## Resilience-first additions

- Keep both origin pools warm and size each region for full peak load.
- Use at least two active regions. For tier-0, add a third region and a third origin pool.
- Use Confluent Enterprise or Dedicated multi-zone clusters with a 99.99% SLA.
- Use bidirectional Cluster Linking and enable consumer offset and ACL sync.
- Use Cloudflare steering that serves both healthy pools (for example, dynamic or geo), not DNS-only failover.
- Monitor Cloudflare pool health, origin 5xx rate, Cluster Linking lag, and PSC endpoint health.
- Run failover game days for Cloudflare pool loss, regional loss, Kafka-only loss, and origin bypass.
- Cloudflare is now the global ingress dependency. For tier-0, document and test a break-glass path that does not depend on the same Cloudflare load balancer.

## Important boundaries

- Cloudflare and the regional ALBs are for HTTP(S). They do not proxy the Kafka protocol.
- Cloudflare health checks use `/readyz`; they do not use Cloud Run service health aggregation.
- PSC endpoints are regional. If their region is down, they cannot be used, even with PSC Global Access.
- PSC Global Access is not a failover mechanism. It allows cross-region access while the endpoint's region is healthy.
- Kafka clients cache metadata. A DNS change alone does not move an existing client; restart or redeploy after switching bootstrap servers.
- Promotions, offset sync, and producer fencing are Kafka operations, not edge operations.
- A regional external ALB backend service can use one serverless NEG for Cloud Run.
- If a Cloud Run service is deleted, Cloud Run does not report an unhealthy status to a GCP load balancer. Cloudflare will still rely on its own health check.

## Related documents

- [Networking and PSC](networking.md)
- [Cloud Run lifecycle and scaling](cloud-run.md)
- [Multi-region and failover](multi-region.md)
- [Operations and security](operations.md)

## References

- [Cloudflare Load Balancing](https://developers.cloudflare.com/load-balancing/)
- [Cloudflare health checks](https://developers.cloudflare.com/load-balancing/understand-basics/health-details/)
- [Regional external Application Load Balancer with Cloud Run](https://docs.cloud.google.com/load-balancing/docs/https/setting-up-reg-ext-https-serverless)
- [Serverless NEGs](https://docs.cloud.google.com/load-balancing/docs/negs/serverless-neg-concepts)
- [Cloud Run Direct VPC egress](https://docs.cloud.google.com/run/docs/configuring/private-networking)
