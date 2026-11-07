# Regional failover and global routing

This document describes what happens when a whole region fails and how HTTP traffic reaches the surviving region. It uses concrete GCP resource names so the pattern can be deployed without inventing a naming scheme.

The load balancer only handles HTTP producer traffic. It does not route the Kafka protocol and it does not promote mirror topics. Kafka failover is a separate step.

## Concrete names

Use these names throughout the deployment and runbook:

| Resource | Name |
| --- | --- |
| Cloud Run producer service (both regions) | `kafka-producer-api` |
| Global external Application Load Balancer | `kafka-producer-global-lb` |
| Global static IP | `kafka-producer-global-ip` |
| Backend service | `kafka-producer-backend` |
| URL map | `kafka-producer-global-lb-url-map` |
| HTTPS proxy | `kafka-producer-https-proxy` |
| HTTPS forwarding rule | `kafka-producer-global-lb-fr` |
| Managed certificate | `kafka-producer-cert` |
| Serverless NEG, primary | `kafka-producer-neg-us-central1` |
| Serverless NEG, secondary | `kafka-producer-neg-us-east1` |
| Public app domain | `producer.example.com` |
| Primary VPC egress subnet | `kafka-run-us-central1` |
| Secondary VPC egress subnet | `kafka-run-us-east1` |
| Primary PSC subnet | `kafka-psc-us-central1` |
| Secondary PSC subnet | `kafka-psc-us-east1` |
| Consumer worker pool, primary | `kafka-consumer-worker` in `us-central1` |
| Consumer worker pool, secondary | `kafka-consumer-worker` in `us-east1` |

The name to use when referring to the load balancer is **`kafka-producer-global-lb`**. In GCP, the actual frontend is the forwarding rule `kafka-producer-global-lb-fr`, backed by the URL map `kafka-producer-global-lb-url-map` and the global IP `kafka-producer-global-ip`.

For private/internal traffic, use a **cross-region internal Application Load Balancer** named `kafka-producer-internal-lb` instead. The same serverless NEG pattern applies.

## Routing layers

| Layer | Primary | Secondary | Failover mechanism |
| --- | --- | --- | --- |
| Client to API | `producer.example.com` resolves to `kafka-producer-global-ip` | Same global IP | Global load balancer |
| HTTP backend | `kafka-producer-neg-us-central1` | `kafka-producer-neg-us-east1` | Cloud Run service health |
| Cloud Run | `kafka-producer-api` in `us-central1` | `kafka-producer-api` in `us-east1` | Readiness probes and NEG health |
| VPC egress | `kafka-run-us-central1` | `kafka-run-us-east1` | Region-local Direct VPC egress |
| PSC | `kafka-psc-us-central1` endpoints | `kafka-psc-us-east1` endpoints | Not global; use the surviving region |
| Kafka | Primary Confluent cluster | Secondary Confluent cluster | Cluster Linking promotion and client switch |

## What happens during a regional failure

1. The primary region becomes unavailable, or its Cloud Run readiness probes fail.
2. Cloud Run service health marks the primary regional service unhealthy. A service is healthy only when at least 60 percent of its instances pass readiness probes.
3. The global external Application Load Balancer stops sending new requests to `kafka-producer-neg-us-central1` and sends them to `kafka-producer-neg-us-east1`.
4. The secondary `kafka-producer-api` handles the request using its own environment:
   - `KAFKA_BOOTSTRAP_SERVERS` points at the secondary Confluent cluster.
   - Direct VPC egress uses `kafka-run-us-east1`.
   - Traffic goes to the secondary PSC endpoints in `kafka-psc-us-east1`.
5. The Kafka data plane is failed over separately: promote the secondary mirror topics and start consumers against the secondary cluster.

The load balancer does not send Kafka traffic. Kafka clients use the regional Confluent bootstrap hostnames, not `producer.example.com`.

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
  --min-instances 2 \
  --max-instances 50 \
  --no-cpu-throttling \
  --readiness-probe httpGet.path=/readyz,httpGet.port=8080,successThreshold=2,failureThreshold=3,timeoutSeconds=2,periodSeconds=10 \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=pkc-secondary.us-east1.gcp.confluent.cloud:9092,KAFKA_TOPIC=orders.v1 \
  --set-secrets KAFKA_API_KEY=kafka-secondary-api-key:latest,KAFKA_API_SECRET=kafka-secondary-api-secret:latest
```

`--min-instances` must be at least 1 for Cloud Run service health to calculate a regional health signal.

## Readiness must reflect Kafka, not just the process

The global load balancer only moves traffic if the primary region is considered unhealthy.

- `/healthz` should answer whether the process is alive.
- `/readyz` should answer whether the instance can publish to Kafka.

A readiness endpoint that only checks that the HTTP server started will keep the primary region "healthy" even when its Kafka cluster is unreachable. That prevents automatic failover for a Kafka-only regional failure.

In production, make `/readyz` perform a bounded Kafka metadata check, for example a short-timeout `fetchTopicMetadata` or an equivalent producer connectivity check. Cache the result briefly so every probe does not create a broker round trip.

The sample service exposes `/readyz` for the initial connection. Add a live Kafka check before using it as the sole automated failover signal.

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

A serverless NEG points to a Cloud Run service in the same region. The pattern uses one serverless NEG per regional service, and the global backend service combines them.

## Create the backend service

```bash
gcloud compute backend-services create kafka-producer-backend \
  --load-balancing-scheme EXTERNAL_MANAGED \
  --global

gcloud compute backend-services add-backend kafka-producer-backend \
  --global \
  --network-endpoint-group kafka-producer-neg-us-central1 \
  --network-endpoint-group-region us-central1

gcloud compute backend-services add-backend kafka-producer-backend \
  --global \
  --network-endpoint-group kafka-producer-neg-us-east1 \
  --network-endpoint-group-region us-east1
```

Without Cloud Run service health or outlier detection, a multi-region serverless backend normally routes to the closest region. It does not automatically fail over just because a service becomes unresponsive. The readiness probe plus Cloud Run service health provides the failover signal.

## Create the global load balancer

```bash
gcloud compute addresses create kafka-producer-global-ip \
  --network-tier PREMIUM \
  --ip-version IPV4 \
  --global

gcloud compute ssl-certificates create kafka-producer-cert \
  --domains producer.example.com \
  --global

gcloud compute url-maps create kafka-producer-global-lb-url-map \
  --default-service kafka-producer-backend

gcloud compute target-https-proxies create kafka-producer-https-proxy \
  --url-map kafka-producer-global-lb-url-map \
  --ssl-certificates kafka-producer-cert

gcloud compute forwarding-rules create kafka-producer-global-lb-fr \
  --load-balancing-scheme EXTERNAL_MANAGED \
  --network-tier PREMIUM \
  --address kafka-producer-global-ip \
  --target-https-proxy kafka-producer-https-proxy \
  --global \
  --ports 443
```

Point the public DNS record at the global IP:

```bash
gcloud dns record-sets create producer.example.com. \
  --zone example-com \
  --type A \
  --ttl 300 \
  --rrdatas "$(gcloud compute addresses describe kafka-producer-global-ip --global --format='value(address)')"
```

The managed certificate becomes active after DNS resolves `producer.example.com` to `kafka-producer-global-ip`.

For an internal API, use a cross-region internal Application Load Balancer named `kafka-producer-internal-lb` and a private DNS record instead of a public IP.

## Restrict Cloud Run ingress

If the producer API should only accept traffic from the load balancer:

```bash
gcloud run services update kafka-producer-api \
  --region us-central1 \
  --ingress internal-and-cloud-load-balancing

gcloud run services update kafka-producer-api \
  --region us-east1 \
  --ingress internal-and-cloud-load-balancing
```

Add authentication at the load balancer or application layer if the API is not meant to be public.

## Test automatic failover

Remove the primary backend to simulate a regional failure:

```bash
gcloud compute backend-services remove-backend kafka-producer-backend \
  --global \
  --network-endpoint-group kafka-producer-neg-us-central1 \
  --network-endpoint-group-region us-central1
```

Send requests to `https://producer.example.com` and confirm they are served by the secondary region. Then add the backend back:

```bash
gcloud compute backend-services add-backend kafka-producer-backend \
  --global \
  --network-endpoint-group kafka-producer-neg-us-central1 \
  --network-endpoint-group-region us-central1
```

Test the Kafka-only failure case separately: make the primary Confluent cluster unreachable while the primary Cloud Run service is still running. The primary `/readyz` must fail, or the load balancer will keep sending traffic to a service that cannot publish.

## Consumer routing

Consumer worker pools have no HTTP endpoint and no global load balancer. Routing for consumers means:

1. Run `kafka-consumer-worker` in both regions.
2. Keep the secondary pool at `min-instances=0` or a small warm size in active-passive.
3. On failover, scale the secondary pool up and the primary pool down or isolate it.
4. Promote the secondary mirror topics.
5. Point the secondary workers at the secondary bootstrap endpoint.
6. Resume from synced consumer offsets if offset sync was enabled.

Do not run the same consumer group against both clusters without fencing. That creates split-brain processing.

## Failure matrix

| Failure | HTTP routing | Kafka routing |
| --- | --- | --- |
| One zone in a region | Global LB continues to the region; Kafka uses other zones/PSC endpoints | No cluster failover |
| GCP region only | Global LB sends to secondary Cloud Run | Can stay on primary Kafka through Global Access, or fail over to secondary |
| Confluent region only | Global LB should send to secondary if `/readyz` fails | Promote secondary cluster |
| Whole region | Global LB sends to secondary Cloud Run | Promote secondary cluster; resume consumers |
| Global LB failure | DNS or a second frontend; see the GCP failover guidance | Unchanged |

## Important boundaries

- The global load balancer is for HTTP(S). It does not proxy the Kafka protocol.
- PSC endpoints are regional. If their region is down, they cannot be used, even with PSC Global Access.
- PSC Global Access is not a failover mechanism. It allows cross-region access while the endpoint's region is healthy.
- Kafka clients cache metadata. A DNS change alone does not move an existing client; restart or redeploy after switching bootstrap servers.
- Promotions, offset sync, and producer fencing are Kafka operations, not load balancer operations.
- Cloud Run service health requires readiness probes and at least one minimum instance per region.
- The pattern uses one serverless NEG per regional Cloud Run service.
- If a Cloud Run service is deleted, Cloud Run does not report an unhealthy status to the load balancer.
- Unknown health is treated as healthy. A newly started instance or a revision without probes does not immediately trigger failover.

## Related documents

- [Networking and PSC](networking.md)
- [Cloud Run lifecycle and scaling](cloud-run.md)
- [Multi-region and failover](multi-region.md)
- [Operations and security](operations.md)

## References

- [Automate cross-region failover with Cloud Run service health](https://docs.cloud.google.com/run/docs/configuring/configure-service-health)
- [Serve traffic from multiple regions](https://docs.cloud.google.com/run/docs/multiple-regions)
- [Global external Application Load Balancer with serverless NEGs](https://docs.cloud.google.com/load-balancing/docs/https/setup-global-ext-https-serverless)
- [Cloud Run health checks](https://docs.cloud.google.com/run/docs/configuring/healthchecks)
