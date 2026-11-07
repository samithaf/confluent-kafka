# Networking and Private Service Connect

## Target path

```text
Cloud Run service or worker pool
  -> Direct VPC egress (subnet in the same region)
  -> PSC forwarding rule private IP (endpoint in the same or another region)
  -> PSC service attachment (Confluent)
  -> Confluent Kafka cluster
```

Do not use the public Confluent bootstrap endpoint from Cloud Run. Do not invent a Kafka DNS name unless you control the TLS certificate.

## Prerequisites

- A Confluent Cloud network and a Kafka cluster in each target region.
- A VPC network and a subnet for Cloud Run egress in each region.
- A dedicated `/26` subnet for the Confluent PSC endpoints in each region.
- Permission to create PSC endpoints, Cloud DNS private zones, firewall rules, and Cloud Run resources.
- The Confluent cluster's bootstrap endpoint and broker hostnames.

## Step 1: Create the Confluent PSC service attachment

Create a private network and PSC service attachment in Confluent Cloud for each region. Confluent creates one service attachment per availability zone. Record the three service attachment resource names.

Using the Confluent CLI or Console is usually simpler than scripting the network API. The important outputs are:

- Service attachment names, one per zone.
- The region of each attachment.
- The Kafka bootstrap endpoint and broker hostnames.

The service attachment is the Confluent side of the connection. You do not create a peering or a VPN.

## Step 2: Reserve a PSC address range

Use a dedicated `/26` in each region:

```bash
gcloud compute networks create kafka-vpc --subnet-mode=custom

gcloud compute networks subnets create kafka-psc-us-central1 \
  --network=kafka-vpc \
  --region=us-central1 \
  --range=10.10.0.0/26

gcloud compute networks subnets create kafka-run-us-central1 \
  --network=kafka-vpc \
  --region=us-central1 \
  --range=10.10.16.0/24
```

`kafka-psc-us-central1` holds only the PSC endpoint addresses. `kafka-run-us-central1` holds Cloud Run Direct VPC egress addresses.

Repeat for each region with non-overlapping ranges.

## Step 3: Create one PSC endpoint per zone

Reserve one address per endpoint:

```bash
gcloud compute addresses create kafka-psc-1 \
  --region=us-central1 \
  --subnet=kafka-psc-us-central1

gcloud compute addresses create kafka-psc-2 \
  --region=us-central1 \
  --subnet=kafka-psc-us-central1

gcloud compute addresses create kafka-psc-3 \
  --region=us-central1 \
  --subnet=kafka-psc-us-central1
```

Create the forwarding rules:

```bash
gcloud compute forwarding-rules create kafka-psc-endpoint-1 \
  --region=us-central1 \
  --network=kafka-vpc \
  --address=kafka-psc-1 \
  --target-service-attachment=projects/PROJECT/regions/us-central1/serviceAttachments/ATTACHMENT_1 \
  --allow-psc-global-access

gcloud compute forwarding-rules create kafka-psc-endpoint-2 \
  --region=us-central1 \
  --network=kafka-vpc \
  --address=kafka-psc-2 \
  --target-service-attachment=projects/PROJECT/regions/us-central1/serviceAttachments/ATTACHMENT_2 \
  --allow-psc-global-access

gcloud compute forwarding-rules create kafka-psc-endpoint-3 \
  --region=us-central1 \
  --network=kafka-vpc \
  --address=kafka-psc-3 \
  --target-service-attachment=projects/PROJECT/regions/us-central1/serviceAttachments/ATTACHMENT_3 \
  --allow-psc-global-access
```

Check the addresses:

```bash
gcloud compute addresses list --filter="name~kafka-psc" --regions=us-central1
gcloud compute forwarding-rules list --filter="name~kafka-psc" --regions=us-central1
```

### Global Access

`--allow-psc-global-access` lets clients in other GCP regions use the endpoint. Enable it when a Cloud Run service in `us-east1` must reach a Confluent cluster in `us-central1`.

Best practice is still to keep the steady-state data path local. Global Access is for failover, centralized tooling, and exceptional cross-region access.

Global Access has caveats:

- The PSC endpoint and the client can be in different regions, but the endpoint is still regional.
- The VPC network and firewall rules must permit the traffic.
- Cross-region traffic has latency and egress cost.
- Test the actual path; do not assume that enabling the flag makes every cross-region topology work.

## Step 4: Configure private DNS

### Use the real Confluent hostnames

The Kafka client must validate the broker TLS certificate. Confluent certificates are issued for Confluent hostnames, not for a custom name such as `kafka.example.com`. If you point a custom DNS name at the PSC IPs, TLS hostname verification fails unless you disable verification or install a custom certificate, both of which are poor choices.

The correct pattern is:

1. Keep `KAFKA_BOOTSTRAP_SERVERS` set to the real Confluent bootstrap hostname.
2. Override DNS inside the VPC so that the bootstrap and broker hostnames resolve to the PSC endpoint IPs.
3. Let TLS validate the real hostname against the Confluent certificate.

### Create a private zone for the regional Confluent domain

Create a private Cloud DNS zone that matches the regional Confluent suffix, for example `us-central1.gcp.confluent.cloud.`:

```bash
gcloud dns managed-zones create confluent-us-central1 \
  --dns-name=us-central1.gcp.confluent.cloud. \
  --description="Confluent PSC override for us-central1" \
  --visibility=private \
  --networks=kafka-vpc
```

Add A records for the bootstrap endpoint and all broker hostnames that Confluent reports. The record names are relative to the zone. For example, if the bootstrap is `pkc-xxxxx.us-central1.gcp.confluent.cloud`, create:

```bash
gcloud dns record-sets create pkc-xxxxx \
  --zone=confluent-us-central1 \
  --type=A \
  --ttl=60 \
  --rrdatas=10.10.0.10,10.10.0.11,10.10.0.12
```

Repeat for each broker hostname. If the zone is dedicated to a single cluster and you understand the blast radius, a wildcard record can cover the bootstrap and broker names:

```bash
gcloud dns record-sets create '*' \
  --zone=confluent-us-central1 \
  --type=A \
  --ttl=60 \
  --rrdatas=10.10.0.10,10.10.0.11,10.10.0.12
```

Prefer explicit records when multiple Confluent clusters share the same regional domain.

### Verify DNS from the VPC

Create a small VM in `kafka-vpc` or run a Cloud Run job with Direct VPC egress and test:

```bash
getent hosts pkc-xxxxx.us-central1.gcp.confluent.cloud
```

The result must be the PSC endpoint IPs, not public Confluent IPs.

## Step 5: Allow egress to the PSC endpoints

Allow Cloud Run egress to the PSC addresses on the Kafka port:

```bash
gcloud compute firewall-rules create allow-kafka-psc \
  --network=kafka-vpc \
  --direction=EGRESS \
  --action=ALLOW \
  --rules=tcp:9092 \
  --destination-ranges=10.10.0.0/26
```

If you use Confluent Schema Registry over a separate PSC endpoint, allow TCP 443 to that endpoint range as well. Do not open ingress to the PSC endpoints from the internet.

## Step 6: Attach Cloud Run to the VPC

Deploy the service or worker pool with Direct VPC egress:

```bash
gcloud run deploy kafka-producer-api \
  --image us-central1-docker.pkg.dev/PROJECT_ID/REPO/kafka-app:latest \
  --region us-central1 \
  --network kafka-vpc \
  --subnet kafka-run-us-central1 \
  --vpc-egress private-ranges-only \
  --allow-unauthenticated \
  --ingress internal-and-cloud-load-balancing \
  --min-instances 2 \
  --startup-probe httpGet.path=/healthz,initialDelaySeconds=10,failureThreshold=30,periodSeconds=5,timeoutSeconds=3 \
  --readiness-probe httpGet.path=/readyz,httpGet.port=8080,successThreshold=2,failureThreshold=3,timeoutSeconds=2,periodSeconds=10
```

For a worker pool:

```bash
gcloud run worker-pools deploy kafka-consumer-worker \
  --image us-central1-docker.pkg.dev/PROJECT_ID/REPO/kafka-app:latest \
  --region us-central1 \
  --network kafka-vpc \
  --subnet kafka-run-us-central1 \
  --vpc-egress private-ranges-only \
  --min-instances 2
```

The producer is not exposed directly. See [Regional failover and Cloudflare edge routing](failover-routing.md) for the regional external ALB, serverless NEG, and Cloudflare Load Balancing layer. The service allows unauthenticated requests only because ingress is restricted to that ALB.

Notes:

- Direct VPC egress can delay the first outbound packet while the network is attached. Use a startup probe for services.
- The Cloud Run egress subnet must be in the same region as the Cloud Run resource.
- Keep Kafka traffic on the local PSC endpoint. PSC Global Access is a fallback while the endpoint region is healthy; it does not make a failed region available.
- `private-ranges-only` routes private destinations to the VPC. If private DNS or cross-region routing does not work, test with `all-traffic` before changing the design.

## Step 7: Verify with the repository tooling

Build the image, deploy a one-off Cloud Run job with the same VPC settings, and run the connectivity check:

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

The job logs the cluster ID and topic metadata. A successful run proves DNS, routing, firewall, TLS, and authentication.

## Multi-region DNS and Global Access

For each region, create a regional private zone and regional records. Keep the regional bootstrap hostnames separate:

| Region | Bootstrap hostname | Private zone |
| --- | --- | --- |
| us-central1 | `pkc-xxxxx.us-central1.gcp.confluent.cloud:9092` | `us-central1.gcp.confluent.cloud.` |
| us-east1 | `pkc-yyyyy.us-east1.gcp.confluent.cloud:9092` | `us-east1.gcp.confluent.cloud.` |

Do not point every region at one cluster unless that is an explicit design choice. The regional hostname should resolve to the regional cluster's PSC endpoints.

The public HTTP producer hostname is separate and is routed by Cloudflare Load Balancing. Do not put Kafka bootstrap traffic behind Cloudflare or the HTTP load balancers. See [Regional failover and Cloudflare edge routing](failover-routing.md).

If you use Shared VPC, attach the Cloud Run projects to the host project's network and create the private zones in the host project, or use Cloud DNS peering. Confirm that the Cloud Run service project can resolve the private zone.

## Troubleshooting

| Symptom | Likely cause | Check |
| --- | --- | --- |
| `ERR__RESOLVE` | DNS does not resolve the Confluent hostname to PSC IPs | `getent hosts` from a VPC VM or Cloud Run job |
| `ERR__TRANSPORT` | Egress route, firewall, or PSC endpoint is wrong | Forwarding rule state, firewall logs, endpoint IPs |
| TLS hostname mismatch | Custom DNS name used instead of the Confluent hostname | Use the real bootstrap hostname |
| Authentication failure | API key/secret or ACL is wrong | Confluent API key, cluster ACLs, Secret Manager version |
| First requests fail after deploy | Direct VPC egress attachment delay | Startup probe with enough failure threshold |
| Cross-region requests fail | Global Access not enabled or firewall blocks the path | `--allow-psc-global-access`, VPC routes, firewall |
| Only some brokers work | Missing DNS records for broker hostnames | Add every broker A record |
| Intermittent timeouts at high load | PSC endpoint throughput or Cloud Run concurrency | Endpoint metrics, instance count, request rate |

## Security notes

- Keep the Confluent bootstrap endpoint private. Do not create a public DNS override.
- Restrict egress with firewall rules to the PSC ranges and required ports.
- Use separate Confluent API keys per workload and environment.
- Store credentials in Secret Manager, not in images or source control.
- Do not disable TLS hostname verification to make a custom DNS name work.
- Consider VPC Service Controls and organization policies if your environment requires them.

## References

- [Confluent Cloud PSC networking](https://docs.confluent.io/cloud/current/networking/private-links/gcp-psc.html)
- [Cloud Run Direct VPC egress](https://cloud.google.com/run/docs/configuring/private-networking)
- [Cloud DNS private zones](https://cloud.google.com/dns/docs/zones)
- [PSC global access](https://cloud.google.com/vpc/docs/about-accessing-vpc-hosted-services-endpoints)
