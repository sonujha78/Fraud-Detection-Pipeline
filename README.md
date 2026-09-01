# Real-Time Fraud Detection Pipeline

A production-style, real-time payment fraud detection system built on Kafka Streams, Cassandra, gRPC microservices, HashiCorp Vault, and Prometheus/Grafana — all running on a self-provisioned Kubernetes cluster.

This README documents the full build process in the order it was actually done, including the problems hit along the way and how each one was diagnosed and fixed. It's written as a build log rather than a polished spec, because the debugging is as much a part of this project as the final architecture.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Tech Stack](#tech-stack)
3. [Build Journey — Step by Step](#build-journey--step-by-step)
4. [Cassandra Data Model & Partition-Key Reasoning](#cassandra-data-model--partition-key-reasoning)
5. [Fraud Detection Rules](#fraud-detection-rules)
6. [Problems Faced & How They Were Resolved](#problems-faced--how-they-were-resolved)
7. [Load Test & Broker-Failure Evidence](#load-test--broker-failure-evidence)
8. [Vault Dynamic Secrets Evidence](#vault-dynamic-secrets-evidence)
9. [Known Limitations](#known-limitations)
10. [How to Run This Yourself](#how-to-run-this-yourself)

---

## Architecture Overview

```
                                   ┌─────────────────┐
                                   │  gRPC Client     │
                                   └────────┬─────────┘
                                            │
                                            ▼
┌──────────────────┐   produces   ┌─────────────────┐
│  Ingest Service   │─────────────▶│ transactions.raw │
│  (Go, gRPC)        │   (keyed by  │  (Kafka, 6 part, │
└──────────────────┘   card_id)    │   RF=3)          │
                                    └────────┬─────────┘
                                             │ consumes
                                             ▼
                              ┌───────────────────────────┐
                              │   Kafka Streams App         │
                              │   (Java)                    │
                              │  - 5-min velocity window     │
                              │  - 1-hr amount-avg window     │
                              │  - impossible-travel check    │
                              │  - risk scoring                │
                              └────────┬──────────┬──────────┘
                                       │          │
                          produces     │          │ writes
                                       ▼          ▼
                     ┌──────────────────┐   ┌──────────────────┐
                     │ transactions.scored│   │  Cassandra        │
                     │ transactions.flagged│  │  (transactions_by_│
                     └──────────────────┘   │   user, flagged_   │
                                             │   transactions)     │
                                             └─────────┬──────────┘
                                                        │ reads
                                                        ▼
                                             ┌──────────────────┐
                                             │  Query Service    │
                                             │  (Go, gRPC)        │
                                             └──────────────────┘

Cross-cutting: HashiCorp Vault issues dynamic, short-TTL Cassandra
credentials to the Streams app and Query Service. Prometheus scrapes
Kafka consumer lag, gRPC latency/error rate; Grafana visualizes it and
alerts on consumer lag.
```

Everything runs inside a single **kind** (Kubernetes-in-Docker) cluster, provisioned with **Terraform**, on a single Ubuntu machine.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Infra-as-code | Terraform (`tehcyx/kind` provider) |
| Container orchestration | Kubernetes (kind, 1 control-plane + 2 workers) |
| Streaming | Apache Kafka 4.0 (KRaft mode, no ZooKeeper), 3 brokers |
| Stream processing | Kafka Streams (Java 21, Gradle) |
| Storage | Apache Cassandra 5.0 (wide-column store) |
| Microservices | Go 1.25, gRPC, Protocol Buffers |
| Secrets management | HashiCorp Vault (Kubernetes auth + dynamic DB credentials) |
| Observability | Prometheus + Grafana + kafka-exporter |
| Package management | Helm (Bitnami/community charts) |

---

## Build Journey — Step by Step

### Phase 1 — Local tooling
Installed Docker, kubectl, kind, Terraform, Vault CLI, Helm, Go 1.25, protoc + Go plugins. Hit an early **IPv6 connectivity problem**: several installers (HashiCorp's apt repo, Go's download server, Docker Hub's CDN) intermittently failed with `No route to host` or DNS resolution errors because the machine preferred IPv6 addresses that weren't actually routable on this network. Fixed by editing `/etc/gai.conf` to prefer IPv4 over IPv6 in glibc's address selection — without fully disabling IPv6, just deprioritizing it. This one fix resolved failures in `wget`, `curl`, `helm pull`, and Docker image pulls for the rest of the project.

### Phase 2 — Kubernetes cluster via Terraform
Wrote a `kind_cluster` Terraform resource (1 control-plane + 2 workers, kindest/node:v1.30.0). `terraform apply` took ~8 minutes on the first run (image pulls), which looked stuck but wasn't — just slow. Verified with `kubectl get nodes` that all 3 nodes came up `Ready`.

### Phase 3 — HashiCorp Vault
Deployed via Helm in standalone (non-HA) mode with file storage and a persistent volume. Ran `vault operator init` (1 key share / 1 threshold, since this is a single-node dev setup) and `vault operator unseal`. Enabled the Kubernetes auth method and both a KV-v2 engine (for Kafka credentials) and the `database` secrets engine (for Cassandra).

### Phase 4 — Kafka (3 brokers, KRaft mode)
Installed via the Bitnami Kafka Helm chart. Hit the **Bitnami registry migration** problem here for the first time (see Problems section) and had to redirect all image references to `bitnamilegacy/*`. Also had a **JMX port conflict** when running `kafka-topics.sh` interactively, because the container's `JMX_PORT` env var collided with the CLI tool trying to start its own JMX agent on the same port — fixed by unsetting `JMX_PORT` before running any Kafka CLI command inside the pod.

Created three topics reflecting the pipeline stages:
- `transactions.raw` — 6 partitions, RF=3 (high write volume)
- `transactions.scored` — 6 partitions, RF=3 (same volume as raw)
- `transactions.flagged` — 3 partitions, RF=3 (much lower volume — only high-risk transactions)

### Phase 5 — Cassandra (3 nodes)
Also via Bitnami's chart (same registry fix required). This phase had by far the most debugging — repeated OOM kills, an authentication deadlock, and a schema mismatch. Full details in the Problems section. Ended up running Cassandra as a **single node** for most of local development (with the 3-node configuration validated separately and documented) purely because of the host machine's limited RAM (14GB shared across Kafka, Cassandra, Vault, Prometheus/Grafana, the JVM stream-processing app, IDE, and browser).

Keyspace: `fraud_detection`, replication_factor adjusted from 3 down to 1 to match the single-node reality during heavy debugging (documented as a known limitation, not a design choice).

### Phase 6 — Kafka Streams fraud-detection engine
This is the intellectual core of the project. Built a Gradle/Java 21 application with:
- A `Transaction` → `ScoredTransaction` pipeline using a custom Jackson-based `Serde`
- Three pieces of state: a 5-minute tumbling-window per-card transaction count, a 1-hour tumbling-window per-user amount sum/average, and a persistent key-value store tracking each card's last-known location (for the impossible-travel check)
- A `FraudRules` class with independently testable rule logic and a weighted risk-scoring function (velocity +40, amount deviation +35, impossible travel +45; flag threshold at 50)
- Output routed to `transactions.scored` (always) and `transactions.flagged` (only if `flagged == true`)

Hit a subtle **partitioning bug** here: the first end-to-end test showed a transaction that *should* have triggered impossible-travel not getting flagged. Root cause — the test producer sent messages without an explicit key, so Kafka round-robin'd them across partitions, and two transactions on the same card ended up processed by different Streams tasks with independent local state stores. Fixed by always keying messages by `card_id` (both in test scripts and, more importantly, inside `IngestService.SubmitTransaction`), so all events for one card are guaranteed to land on the same partition and be processed by the same task in order.

### Phase 7 — Ingest Service (Go, gRPC)
A small, stateless Go service exposing `SubmitTransaction`, validating the payload, and producing to `transactions.raw` keyed by `card_id` using `segmentio/kafka-go`. Chose `kafka-go` over `confluent-kafka-go` specifically to avoid cgo/librdkafka cross-compilation overhead — the resulting Docker image is ~41MB versus ~586MB for the Java Streams app.

### Phase 8 — Cassandra write path (Streams app → Cassandra)
Added a `CassandraWriter` class to the Streams app so every scored transaction is persisted to `transactions_by_user`, and flagged ones additionally go to `flagged_transactions`. This surfaced three separate bugs in sequence (Cassandra authentication, then a UUID-vs-text type mismatch) — see Problems section.

### Phase 9 — Query Service (Go, gRPC)
Implements `GetFlaggedTransactionsToday` and `GetUserFraudScoreHistory`, both reading from Cassandra via `gocql`. Hit the same UUID-vs-text mismatch as the Streams app (fixed the same way — plain strings, no UUID coercion) and a `decimal`-vs-`float64` unmarshalling error (fixed with an explicit `CAST(amount AS DOUBLE)` in the CQL query, since `gocql` doesn't auto-convert Cassandra's `decimal` type to Go's `float64`).

### Phase 10 — Vault dynamic secrets
Configured the `database` secrets engine to talk to Cassandra using Vault's **built-in** `cassandra-database-plugin` (no external plugin download needed — this was a pleasant surprise, since most guides assume you need to fetch and register a plugin binary manually). Created a role (`fraud-detection-role`) with a 1-hour default TTL and CQL statements to create a scoped Cassandra user on each credential request. Verified end-to-end: requested credentials via `vault read database/creds/fraud-detection-role`, got back a freshly generated username/password, and used them to log into Cassandra and query real data — proving the dynamic-credential flow actually works, not just that Vault returns *something*. Also demonstrated lease renewal (`vault lease renew`) extending a credential's TTL without generating new credentials or restarting any service.

### Phase 11 — Observability (Prometheus + Grafana)
Installed both via Helm with deliberately trimmed-down resource requests/limits (this machine was already tight on RAM by this point). Discovered that Kafka's JMX-exporter sidecar (already running as part of the broker pods) and Prometheus's default Kubernetes service-discovery scrape config picked up Kafka JMX metrics *automatically*, with zero extra config. Consumer-group lag specifically wasn't exposed by the JMX exporter, so added a small dedicated `kafka-exporter` (danielqsj/kafka-exporter) deployment, which immediately exposed `kafka_consumergroup_lag` — the single most important metric for this task ("if the stream job falls behind, fraud detection becomes useless"). Instrumented both Go services with `grpc-ecosystem/go-grpc-prometheus` and a `/metrics` endpoint to get request latency/error-rate metrics.

Built a Grafana dashboard with panels for: live gRPC throughput, Kafka consumer-group lag by topic, gRPC p99 latency, gRPC error rate, and Kafka broker/partition health. Created an alert rule (`Kafka Consumer Lag High`) that fires when `sum(kafka_consumergroup_lag{consumergroup="fraud-detection-streams-app"})` exceeds 1000, evaluated every minute, routed to a webhook contact point.

Cassandra JMX metrics were **not** enabled — the exporter sidecar had previously caused repeated OOM kills (see Problems section) and re-enabling it wasn't worth the stability risk on this machine. This is called out explicitly as a limitation below.

### Phase 12 — Load test & broker-failure test
Wrote a small Go load-testing tool (`scripts/load-test/main.go`) using `kafka-go`, run *from inside* the cluster (as a throwaway pod) rather than through `kubectl port-forward`, because Kafka's advertised listeners point to internal cluster DNS names that don't resolve from the host machine over a forwarded port. Ran a scaled-down burst (~200 transactions over ~15s, targeting ~800 tx/min) — scaled down from the spec's 10,000 tx/min because the host machine could not reliably sustain that volume alongside three JVMs (Kafka x3, Cassandra) already running. The test achieved 100% delivery success and confirmed consumer lag returned to 0 immediately after.

For the broker-failure test, captured a "before" snapshot of partition leadership/ISR for `transactions.raw` (all 3 brokers in-sync), sent a transaction, force-killed `kafka-controller-1`, immediately sent another transaction *during* the outage (it succeeded — no downtime), and confirmed the pod self-healed via the StatefulSet within ~42 seconds. Verified both the pre-failure and during-failure transactions were present in Cassandra afterward — zero data loss.

---

## Cassandra Data Model & Partition-Key Reasoning

### `transactions_by_user`
```sql
CREATE TABLE transactions_by_user (
    user_id text,
    transaction_time timestamp,
    transaction_id text,
    card_id text,
    amount decimal,
    merchant text,
    location text,
    currency text,
    PRIMARY KEY (user_id, transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC);
```
**Partition key: `user_id`.** The Streams app's most frequent read pattern is "give me this user's recent transaction history to compute a historical average." Partitioning by `user_id` means that query is always a single-partition read — no cross-node scatter-gather. Clustering by `transaction_time DESC` means the most recent transactions come back first without any extra sorting.

**Hot-partition risk:** if one user (or a compromised/bot account) generated an abnormal number of transactions, their partition would grow disproportionately. In practice this is bounded — no legitimate user generates thousands of transactions per day — so it's an acceptable trade-off for this access pattern. A production system might additionally bucket by `(user_id, month)` to cap partition growth over time.

### `flagged_transactions`
```sql
CREATE TABLE flagged_transactions (
    flag_date date,
    flagged_time timestamp,
    transaction_id text,
    user_id text,
    card_id text,
    amount decimal,
    risk_score double,
    risk_reason text,
    location text,
    PRIMARY KEY (flag_date, flagged_time)
) WITH CLUSTERING ORDER BY (flagged_time DESC);
```
**Partition key: `flag_date`.** The ops dashboard's primary query is "show me today's flagged transactions," so partitioning by date turns that into a single-partition read as well.

**Hot-partition risk — this one is real and worth calling out honestly.** If a single day sees an unusually large fraud event (an attack, a bulk-testing campaign against stolen cards), *all* of that day's flags land in one partition, which could grow into the hundreds-of-MB range that Cassandra operators generally consider "hot." The production fix would be a composite key like `(flag_date, hour)` to spread load across the day. For this project's scale, daily partitioning was kept for query simplicity, with this trade-off documented rather than silently ignored.

---

## Fraud Detection Rules

Implemented in `FraudRules.java`, kept deliberately separate from the Kafka Streams topology so the rules themselves are easy to read and reason about independently of the plumbing:

1. **Velocity check** — more than 5 transactions on the same card within a 5-minute tumbling window.
2. **Amount deviation** — a transaction more than 5x the user's historical average amount (or, for a brand-new user with no history, any single transaction above an absolute ₹50,000 cold-start threshold).
3. **Impossible travel** — two transactions on the same card from different locations less than 30 minutes apart (physically implausible for any real mode of travel).

Each rule contributes a weight toward a 0–100 risk score (velocity +40, amount deviation +35, impossible travel +45), and a transaction is flagged once the cumulative score reaches 50. This means no single weak signal auto-flags a transaction by itself except impossible travel combined with even a mild amount signal — the scoring is intentionally graduated rather than a single binary rule.

---

## Problems Faced & How They Were Resolved

This project involved a lot of hands-on debugging on a resource-constrained local machine (14GB RAM shared across the entire stack). Below is every significant issue encountered, in the order it came up, with the actual root cause and fix — not just "it works now."

### 1. IPv6 connectivity breaking installers
**Symptom:** `wget`, `curl`, `helm pull`, and Docker pulls intermittently failed with `No route to host` or DNS resolution errors, even though the same commands sometimes worked.
**Root cause:** The machine's IPv6 route was not actually functional on this network, but glibc's default address-selection policy still preferred IPv6 addresses when both were returned by DNS.
**Fix:** Edited `/etc/gai.conf` to add `precedence ::ffff:0:0/96 100`, which tells the system to prefer IPv4 over IPv6 without disabling IPv6 entirely. This fixed every subsequent download issue in the project.

### 2. Bitnami container registry migration
**Symptom:** Every Bitnami Helm chart (Kafka, Cassandra) failed with `ErrImagePull` / `ImagePullBackOff` on images like `docker.io/bitnami/kafka:...`.
**Root cause:** Bitnami moved its free-tier container images from `docker.io/bitnami/*` to `docker.io/bitnamilegacy/*` in August 2025 as part of a shift toward a paid "Secure Images" model. The Helm charts' default `values.yaml` still pointed at the old registry.
**Fix:** Overrode `image.registry`/`image.repository` (and the same for `volumePermissions`, `metrics`, and other sidecar images) to `bitnamilegacy/*` for every affected chart. Also had to set `global.security.allowInsecureImages: true` because newer chart versions added an explicit safety check that refuses to deploy "unrecognized" (non-standard) container images without an opt-in flag.

### 3. Kafka JMX port conflict when running CLI tools
**Symptom:** Running `kafka-topics.sh` or `kafka-console-producer.sh` inside a broker pod failed with `Port already in use: 5555`.
**Root cause:** The broker container sets `JMX_PORT` as an environment variable so the main Kafka process can expose JMX metrics. Any CLI tool launched in the same shell inherits that same env var and tries to bind its own JMX agent to the same port.
**Fix:** Always `unset JMX_PORT` (and often `KAFKA_JMX_OPTS`) before running any Kafka CLI command inside the pod.

### 4. Cassandra OOMKilled repeatedly
**Symptom:** Cassandra pods crash-looped with `OOMKilled` even after significantly raising memory limits and reducing JVM heap size.
**Root cause:** Turned out to be the Bitnami **metrics sidecar** (`cassandra-exporter`), not the main Cassandra process — it had its own small, un-tuned memory limit that it kept exceeding, causing the *pod* to restart even though the actual database process was healthy.
**Fix:** Disabled the metrics sidecar (`metrics.enabled: false`) for local development; the main Cassandra container then ran stably. Documented as a known limitation (see below) rather than spending further time force-fitting an exporter into a memory-constrained environment.

### 5. Cassandra authentication deadlock (QUORUM vs single node)
**Symptom:** After scaling Cassandra down to a single node (to save RAM), login with the `cassandra` superuser started failing with `Cannot achieve consistency level QUORUM`.
**Root cause:** The `system_auth` keyspace (where Cassandra stores its own users/roles) was still configured with `replication_factor: 3` from when the cluster had 3 nodes. Authentication reads/writes to `system_auth` require QUORUM consistency by default, and QUORUM of 3 replicas can't be satisfied by 1 live node — a chicken-and-egg problem, since you need to *already be authenticated* to run the `ALTER KEYSPACE` command that would fix it.
**Fix:** Temporarily patched the StatefulSet to mount a custom `cassandra.yaml` (via a ConfigMap and the chart's `existingConfiguration` parameter) with both `authenticator` and `authorizer` set to `AllowAll*` (both had to be changed together — Cassandra refuses to start if only one is disabled, since the authorizer depends on authentication being active). With auth temporarily open, ran `ALTER KEYSPACE system_auth WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, then reverted the ConfigMap back to password authentication. Also had to `nodetool assassinate` two stale dead-node entries left over from the earlier 3-node topology, which were blocking the replication-factor change with a "not all endpoints in normal state" error.

### 6. Schema/type mismatches between application code and actual table schema
**Symptom:** `Codec not found for requested operation: [TEXT <-> java.util.UUID]` from the Streams app, and later `can not marshal gocql.UUID into varchar` / `can not unmarshal decimal into *float64` from the Go Query Service.
**Root cause:** The original table design used UUID columns, but the actual tables that existed by the time the write path was built had `text` columns for IDs (from an earlier schema iteration) and a Cassandra `decimal` type for `amount` that neither `gocql` nor the initial Java code handled correctly by default.
**Fix:** Rewrote `CassandraWriter.java` to bind all IDs as plain strings (matching the `text` columns) instead of coercing them into UUIDs. In the Go Query Service, did the same for IDs, and added explicit `CAST(amount AS DOUBLE)` in the CQL `SELECT` queries so `gocql` receives a type it can unmarshal directly into `float64`, rather than the Cassandra-native `decimal` type.

### 7. Partitioning bug causing missed fraud detection
Covered above in Phase 6 — messages without an explicit Kafka key were round-robined across partitions, breaking the per-card stateful impossible-travel check. Fixed by always keying by `card_id`.

### 8. `kubectl port-forward` breaking Kafka client connections
**Symptom:** A load-testing script connecting to `localhost:9092` via `kubectl port-forward` would connect but then hang indefinitely on every produce call, eventually failing all messages.
**Root cause:** Kafka's client protocol involves a two-step connection: the client first connects to *any* broker, which then tells it the "advertised" address of the broker that actually owns the relevant partition — and that advertised address is an internal cluster DNS name (`kafka-controller-N.kafka-controller-headless...`) that doesn't resolve from outside the cluster, even through a forwarded port.
**Fix:** Ran the load-testing binary as a pod *inside* the cluster (`kubectl cp` the compiled binary into a throwaway pod, then `kubectl exec` it) so all Kafka connections stayed inside the cluster's network and could resolve internal DNS names normally.

### 9. General resource exhaustion on the host machine
Throughout the project, running Kafka (3 brokers) + Cassandra (1–3 nodes) + Vault + Prometheus + Grafana + a Java Streams app + two Go services on a single 14GB-RAM machine (alongside an IDE and browser) repeatedly pushed the system into swap, causing seemingly random command failures (`exit code 137` on simple CLI tools, not just application containers) that had nothing to do with the actual pipeline logic. Diagnosed each time via `free -h` and `kubectl top pods`, and mitigated by trimming resource requests/limits aggressively, temporarily scaling Cassandra down to 1 replica during heavy debugging, and closing unrelated host processes (browser tabs, the Gradle daemon) when RAM got critical.

---

## Load Test & Broker-Failure Evidence

- **Load test:** 200 transactions sent over ~17 seconds (scaled down from the spec's 10,000/min target — see Known Limitations), 100% delivery success, 0 failures. Kafka consumer-group lag for `fraud-detection-streams-app` confirmed at `0` immediately after the burst via a live Prometheus query.
- **Broker failure:** Captured partition leadership/ISR for `transactions.raw` before killing `kafka-controller-1` (all 3 brokers in-sync, RF=3). Force-deleted the pod, sent a transaction *during* the outage (succeeded, proving no write downtime), and confirmed the broker self-healed within ~42 seconds via the StatefulSet. Verified both the pre-failure and mid-failure transactions were present in Cassandra afterward.
- Evidence files: `docs/evidence/broker-failure-before.txt`, `docs/evidence/broker-failure-after.txt`.

---

## Vault Dynamic Secrets Evidence

- Configured Vault's built-in `cassandra-database-plugin` (no external plugin install required) against the live Cassandra cluster.
- Created role `fraud-detection-role` (1-hour default TTL, 24-hour max TTL) with CQL statements that create a scoped, non-superuser Cassandra account per credential request.
- Requested credentials via `vault read database/creds/fraud-detection-role`, received a freshly generated username/password pair, and **used those exact credentials to log into Cassandra and query live data** — confirming the dynamic-credential flow is genuinely functional, not just that Vault returns a response.
- Demonstrated lease renewal: looked up a lease's remaining TTL, ran `vault lease renew`, and confirmed the lease duration reset to a full hour — proving credentials can be extended without generating new ones or restarting any service.
- Configured a Kubernetes auth role and policy (`fraud-detection-policy`, scoped to read-only access on `database/creds/fraud-detection-role`) so services could eventually authenticate via their Kubernetes ServiceAccount identity rather than a static token.
- Evidence file: `docs/evidence/vault-dynamic-credentials.md`.

---

## Known Limitations

Being upfront about the gaps, since a resource-constrained local dev environment forced some pragmatic trade-offs:

- **Cassandra ran as a single node** for most local development and testing (3-node topology was validated once and documented, but reverted due to host RAM constraints). The application code and Vault database-role are cluster-size-agnostic, so this is a deployment-time constraint, not an architectural one.
- **Cassandra JMX metrics are disabled** in the current Prometheus setup — the metrics sidecar caused repeated OOM kills on this machine (see Problems #4). Kafka and gRPC metrics are fully wired up.
- **Load test volume was scaled down** from the spec's 10,000 tx/min to roughly 800 tx/min, proportional to what this single 14GB-RAM machine could sustain alongside the rest of the running stack. The pipeline mechanics being validated (delivery success, consumer lag recovery) don't change with scale — only the absolute throughput number does.
- **`system_auth` and `fraud_detection` keyspaces run with `replication_factor=1`**, matching the single-node Cassandra deployment above. A 3-node production deployment would use RF=3 for both, as originally designed.
- Services currently connect to Cassandra/Kafka using credentials passed via environment variables rather than a fully automated Vault-Agent sidecar injection and renewal loop. The dynamic-credential *mechanism* is proven end-to-end (see Vault section above); wiring it into the running services' actual runtime credential-refresh cycle was scoped out given time constraints.

---

## How to Run This Yourself

Prerequisites: Docker, kubectl, kind, Terraform, Helm, Go 1.25+, Java 21 + Gradle, `protoc` with Go plugins.

```bash
# 1. Provision the Kubernetes cluster
cd terraform/kind-cluster
terraform init && terraform apply -auto-approve
export KUBECONFIG=$(pwd)/fraud-detection-config

# 2. Deploy Vault, Kafka, Cassandra (each via Helm — see k8s/<component>/*-values.yaml)
# See individual chart directories under k8s/ for the exact values files used.

# 3. Build and deploy the Kafka Streams app
cd stream-processor
./gradlew clean build -x test
docker build -t fraud-detection-streams:latest .
kind load docker-image fraud-detection-streams:latest --name fraud-detection
kubectl apply -f ../k8s/stream-processor/deployment.yaml

# 4. Build and deploy the Go microservices
cd services/ingest-service && docker build -t ingest-service:latest . && kind load docker-image ingest-service:latest --name fraud-detection
cd services/query-service && docker build -t query-service:latest . && kind load docker-image query-service:latest --name fraud-detection
kubectl apply -f k8s/ingest-service/deployment.yaml
kubectl apply -f k8s/query-service/deployment.yaml

# 5. Deploy observability
helm install prometheus ./k8s/prometheus/prometheus -n monitoring -f k8s/prometheus/prometheus-values.yaml
helm install grafana ./k8s/grafana/grafana -n monitoring -f k8s/grafana/grafana-values.yaml
kubectl apply -f k8s/kafka-exporter/deployment.yaml
```

See the `docs/evidence/` directory for load-test output, broker-failure test logs, and Vault credential-rotation proof.
