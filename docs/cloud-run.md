# Cloud Run lifecycle and scaling

## Choose the right resource

| Requirement | Cloud Run service | Cloud Run worker pool | Cloud Run job |
| --- | --- | --- | --- |
| HTTP ingress | Yes | No | No |
| Request concurrency | Yes | No | No |
| Background run loop | Possible, but awkward | Yes | Runs to completion |
| Scale on CPU/requests | Yes | No request signal | No |
| Scale on Kafka lag | Not native | Via CREMA/KEDA | Not applicable |
| Billing | Request-based or instance-based | Instance-based | Per execution |
| Probes | Startup and liveness | Not applicable | Not applicable |

Use a service for the producer API. Use a worker pool for consumers. Use a job for topic creation and connectivity checks.

## CPU and billing

A Cloud Run service with request-based billing throttles CPU outside request processing. That is fine for a producer handler, but it can delay background flush, reconnect, and metrics work.

Options:

- Set `--min-instances=1` to keep a warm instance.
- Use `--no-cpu-throttling` (instance-based billing) if the service must do meaningful work outside requests.
- For consumer worker pools, CPU is allocated to the instance for its lifetime. There is no request-based billing.

Cost is driven by allocated CPU and memory, minimum instances, and time. A warm `min-instances=1` instance costs money even with no traffic. Use it deliberately for latency-sensitive producers.

## Probes

Direct VPC egress can delay the first outbound packet while the instance network is attached. A startup probe prevents Cloud Run from sending traffic to an instance that is not yet connected to Kafka.

The reference producer exposes:

- `GET /healthz`: process is up and the producer has connected.
- `GET /readyz`: same signal for external load balancers.

Cloud Run uses the startup probe to gate traffic. It does not have a separate readiness probe. Do not use Kafka broker availability as a liveness signal: a broker outage should not restart every producer instance.

Example startup probe:

```bash
--startup-probe httpGet.path=/healthz,initialDelaySeconds=10,failureThreshold=30,periodSeconds=5,timeoutSeconds=3
```

This allows up to roughly 160 seconds for Direct VPC egress, DNS, and the initial Kafka connection. Tune it to your environment.

For a liveness probe, use a shallow check with a generous failure threshold. A process that is alive but temporarily disconnected from Kafka should reconnect rather than restart.

## SIGTERM and graceful shutdown

Cloud Run sends SIGTERM when it scales in or replaces a revision. The default termination grace period is short. Keep the application shutdown budget below it; the reference uses `SHUTDOWN_TIMEOUT_MS=8000`.

Producer shutdown:

```text
SIGTERM
  -> /healthz returns not ready
  -> stop accepting new HTTP requests
  -> close idle keep-alive connections
  -> flush producer
  -> disconnect producer
  -> exit 0
```

Consumer shutdown:

```text
SIGTERM
  -> consumer.stop()
  -> wait for the batch loop to exit
  -> commit final offsets
  -> disconnect
  -> exit 0
```

If shutdown exceeds the budget, the lifecycle helper forces a non-zero exit. That is better than hanging until the platform kills the process without a final commit, but it still risks reprocessing. Keep batch sizes and processing times bounded.

## Networking flags

Direct VPC egress:

```bash
gcloud run deploy SERVICE \
  --network=kafka-vpc \
  --subnet=kafka-run-us-central1 \
  --vpc-egress=private-ranges-only
```

The subnet must be in the same region as the Cloud Run resource. The PSC endpoint can be in the same region, or in another region with PSC Global Access.

If private DNS or cross-region routing does not work with `private-ranges-only`, test `all-traffic` before changing the architecture. `all-traffic` sends all egress through the VPC and may affect other dependencies.

## Deploying the producer service

```bash
gcloud run deploy producer-service \
  --image us-central1-docker.pkg.dev/PROJECT_ID/REPO/kafka-app:latest \
  --region us-central1 \
  --network kafka-vpc \
  --subnet kafka-run-us-central1 \
  --vpc-egress private-ranges-only \
  --min-instances 1 \
  --max-instances 50 \
  --cpu 1 \
  --memory 512Mi \
  --concurrency 80 \
  --timeout 30 \
  --no-cpu-throttling \
  --startup-probe httpGet.path=/healthz,initialDelaySeconds=10,failureThreshold=30,periodSeconds=5,timeoutSeconds=3 \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092,KAFKA_TOPIC=orders.v1 \
  --set-secrets KAFKA_API_KEY=kafka-api-key:latest,KAFKA_API_SECRET=kafka-api-secret:latest
```

Tune `--concurrency` against producer throughput and memory. A single producer instance multiplexes many requests, but the producer queue and event loop still have limits.

## Deploying the consumer worker pool

```bash
gcloud run worker-pools deploy consumer-worker \
  --image us-central1-docker.pkg.dev/PROJECT_ID/REPO/kafka-app:latest \
  --region us-central1 \
  --network kafka-vpc \
  --subnet kafka-run-us-central1 \
  --vpc-egress private-ranges-only \
  --min-instances 1 \
  --max-instances 20 \
  --cpu 1 \
  --memory 512Mi \
  --command node \
  --args dist/consumer-worker.js \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092,KAFKA_TOPIC=orders.v1,KAFKA_CONSUMER_GROUP=orders-service,KAFKA_PARTITIONS_CONSUMED_CONCURRENTLY=4 \
  --set-secrets KAFKA_API_KEY=kafka-api-key:latest,KAFKA_API_SECRET=kafka-api-secret:latest
```

Worker pools do not expose an HTTP port. Do not add a startup probe expecting HTTP. The consumer retry loop in `connectWithRetry` handles a slow initial connection. If the initial connection never succeeds, the instance stays alive and retries; use logs and alerting rather than a platform probe.

## Autoscaling consumers

Worker pools do not have a request concurrency signal. Options:

1. Manual instance count for predictable load.
2. Schedule-based scaling for known peaks.
3. CREMA (Cloud Run Metrics Autoscaler) with Kafka lag metrics.
4. KEDA with a Kafka scaler, if your platform integrates it.

Lag-based scaling needs a metrics source and a control loop. The scale target must account for:

- Rebalance time when instances join or leave.
- Partition count as the upper bound.
- Per-instance processing throughput.
- Batch size and commit interval.
- Downstream dependency capacity.

Do not scale purely on CPU. A consumer can be CPU-idle while lag grows because of a slow downstream call.

## Container image

The `@confluentinc/kafka-javascript` package ships prebuilt librdkafka binaries linked against glibc. Use a Debian-based image:

```dockerfile
FROM node:22-bookworm-slim
```

Do not use Alpine or another musl-based image unless you build librdkafka from source for that platform. The reference Dockerfile uses a multi-stage build, `npm ci`, a non-root user, and a runtime image with production dependencies only.

## Jobs for setup and verification

Use Cloud Run jobs for:

- Creating topics.
- Checking DNS, PSC, TLS, and authentication.
- Running a one-off replay or backfill.

```bash
gcloud run jobs deploy kafka-check \
  --image us-central1-docker.pkg.dev/PROJECT_ID/REPO/kafka-app:latest \
  --region us-central1 \
  --network kafka-vpc \
  --subnet kafka-run-us-central1 \
  --vpc-egress private-ranges-only \
  --command node \
  --args dist/check-connectivity.js \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092,KAFKA_TOPIC=orders.v1 \
  --set-secrets KAFKA_API_KEY=kafka-api-key:latest,KAFKA_API_SECRET=kafka-api-secret:latest

gcloud run jobs execute kafka-check --region us-central1 --wait
```

## Secrets

- Store `KAFKA_API_KEY` and `KAFKA_API_SECRET` in Secret Manager.
- Reference secret versions with `--set-secrets`.
- Grant the Cloud Run runtime service account access only to the required secrets.
- Rotate keys on a schedule and support overlapping validity during rotation.
- Do not bake credentials into the image or commit `.env`.

## Observability

The reference logger emits one JSON object per line with severity, message, service, revision, and structured fields. Cloud Logging parses the severity field and indexes the rest.

Track:

- Cloud Run request count, latency, and 5xx rate for the producer.
- Instance count and startup latency.
- Consumer lag per topic and partition.
- Rebalance count and commit failures.
- Producer send latency and error codes.
- PSC endpoint health and throughput.
- DNS resolution failures.

Use Cloud Monitoring dashboards and alerts. Add OpenTelemetry tracing at the HTTP boundary and propagate `traceparent` in Kafka headers if you need end-to-end traces.

## Cost notes

- Minimum instances are the main fixed cost.
- Cross-region Global Access traffic adds egress cost and latency.
- Long-lived consumer instances cost for their lifetime.
- Compression reduces storage and network cost but increases CPU.
- Over-provisioning consumers beyond the partition count adds cost with no throughput gain.
- Keep the PSC and Cloud Run subnets sized correctly; IP exhaustion causes deployment failures.

## References

- [Cloud Run services](https://cloud.google.com/run/docs/deploying)
- [Cloud Run worker pools](https://cloud.google.com/run/docs/configuring/workerpools)
- [Cloud Run health checks](https://cloud.google.com/run/docs/configuring/healthchecks)
- [Cloud Run Direct VPC egress](https://cloud.google.com/run/docs/configuring/private-networking)
