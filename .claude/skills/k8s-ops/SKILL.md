---
name: k8s-ops
description: >
  Manages Kubernetes and CNPG database operations for the wallet-backend deployment on EKS.
  Use when user mentions "deploy", "pods", "kubectl", "k8s", "kubernetes", "CNPG", "cluster status",
  "database logs", "psql", "rollout", "scale", "port-forward", "exec into pod", "restart deployment",
  "check pods", "events", "health check", "backfill job", "hibernate", "backup database", "pgbench",
  or asks about the dev environment infrastructure.
  Do NOT use for local Docker Compose development, Go code changes, or test execution.
metadata:
  author: adityavyas
  version: 1.0.0
---

# Kubernetes & CNPG Operations for wallet-backend

## Critical: Safety Rules

1. **Always verify context before mutations**: Run `kubectl config current-context` and confirm it shows the expected EKS cluster before any write operation.
2. **Always use `-n wallet-eng-dev`** on every kubectl command — never rely on default namespace.
3. **Never delete the CNPG cluster without explicit user confirmation** — this risks data loss on 1.5 TiB storage.
4. **Confirm before any `kubectl delete`** or destructive CNPG operations (`destroy`, `hibernate on`).
5. **Warn before `kubectl cnpg destroy`** — this permanently removes a database instance.

## Resource Inventory

| Resource | Name | Details |
|----------|------|---------|
| Namespace | `wallet-eng-dev` | All resources live here |
| API Deployment | `wallet-backend-api-mainnet` | Runs `serve`, port 8001 |
| Ingest Deployment | `wallet-backend-ingest-mainnet` | Runs `ingest`, Recreate strategy, port 8002 |
| CNPG Cluster | `wallet-backend-mainnet-db` | PG17 + TimescaleDB 2.25.0 |
| ConfigMap | `wallet-backend-env-mainnet` | Environment configuration |
| Service | `wallet-backend-api-mainnet` | ClusterIP, port 80 -> 8001 |
| Ingress | `wallet-backend-mainnet-dev.kube001-dev.services.stellar-ops.com` | External access |
| Image Registry | `docker-registry.services.stellar-ops.com/dev/wallet-backend` | Container images |
| DB Credentials | `wallet-backend-mainnet-db-app-creds` (Secret) | Database connection credentials |

### Manifest File Paths

- **App manifests**: `/Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-mainnet.yaml`
- **CNPG manifest**: `/Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-cnpg-mainnet.yaml`
- **Backfill job**: `/Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-backfill-job.yaml`
- **TSDB monitoring**: `/Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-tsdb-monitoring.yaml`

## Instructions

### Step 1: Identify the Operation Type

Classify the user's request into one of these categories and jump to the relevant section:

- **Status / Health Check** -> Section A or Section C (CNPG)
- **View Logs** -> Section B or Section C (CNPG Logs)
- **Deploy / Update Image** -> Section D
- **Scale** -> Section E
- **Exec / Port-Forward** -> Section F
- **CNPG Database Operations** -> Section C
- **Composite Workflow** -> Section G

### Section A: Check Overall Status

Run these commands to get a complete picture:

```bash
# Verify context first
kubectl config current-context

# All wallet-backend resources
kubectl get all -n wallet-eng-dev -l app=wallet-backend

# Deployment status
kubectl get deployments -n wallet-eng-dev

# Pod status with node info
kubectl get pods -n wallet-eng-dev -o wide

# Recent events (last 30 minutes)
kubectl get events -n wallet-eng-dev --sort-by='.lastTimestamp' --field-selector 'involvedObject.namespace=wallet-eng-dev'

# Resource usage
kubectl top pods -n wallet-eng-dev
```

### Section B: View Pod Logs

Determine whether the user wants API or Ingest logs:

**API logs:**
```bash
# Main container logs (follow)
kubectl logs -n wallet-eng-dev deployment/wallet-backend-api-mainnet -f

# Previous container (if crashed)
kubectl logs -n wallet-eng-dev deployment/wallet-backend-api-mainnet --previous

# Init container logs
kubectl logs -n wallet-eng-dev deployment/wallet-backend-api-mainnet -c <init-container-name>

# Last N lines
kubectl logs -n wallet-eng-dev deployment/wallet-backend-api-mainnet --tail=200
```

**Ingest logs:**
```bash
# Main container logs (follow)
kubectl logs -n wallet-eng-dev deployment/wallet-backend-ingest-mainnet -f

# Previous container (if crashed)
kubectl logs -n wallet-eng-dev deployment/wallet-backend-ingest-mainnet --previous

# Last N lines
kubectl logs -n wallet-eng-dev deployment/wallet-backend-ingest-mainnet --tail=200
```

**Tip:** If multiple pods exist, first list pods then target a specific one:
```bash
kubectl get pods -n wallet-eng-dev -l app=wallet-backend-api-mainnet
kubectl logs -n wallet-eng-dev <specific-pod-name> -f
```

### Section C: CNPG Database Operations

**Prerequisite check** — the CNPG kubectl plugin must be installed:
```bash
kubectl cnpg version
# If not installed: brew install kubectl-cnpg
```

If the plugin is not installed, tell the user to run `brew install kubectl-cnpg` first.

#### C1: Cluster Status & Monitoring

> **Note:** `kubectl cnpg status` is **not usable** in this environment. The EKS node security
> group blocks port 8000 from the API server (the `pods/proxy` subresource routes API Server → Pod
> IP:8000). Other `cnpg` commands (`psql`, `logs`, `restart`, `backup`) still work because they use
> `exec`, which goes through kubelet on port 10250.
>
> **Additionally:** The `cnpg-timescaledb` image does **not** include `curl` or `wget`, so the
> `exec`-based `curl https://localhost:8000/pg/status` approach also fails. Use the `psql`-based
> workaround below instead — it is always available and provides the critical health signals.

```bash
# Step 1: Cluster-level info from CRD (always works, no port 8000 needed)
kubectl get cluster wallet-backend-mainnet-db -n wallet-eng-dev -o json | \
  jq '{phase: .status.phase, instances: .status.instances, readyInstances: .status.readyInstances, currentPrimary: .status.currentPrimary, targetPrimary: .status.targetPrimary, timelineID: .status.timelineID, image: .status.image, instanceNames: .status.instanceNames, conditions: [.status.conditions[] | {type: .type, status: .status, message: .message}]}'

# Step 2: Live instance health via psql (curl/wget not available in cnpg-timescaledb image)
PODS=$(kubectl get pods -n wallet-eng-dev -l cnpg.io/cluster=wallet-backend-mainnet-db -o jsonpath='{.items[*].metadata.name}')

for POD in $PODS; do
  echo "=== $POD ==="
  kubectl exec -n wallet-eng-dev $POD -- \
    psql -U postgres -c "SELECT pg_current_wal_lsn() AS current_lsn, pg_is_in_recovery() AS is_replica, version();"
  # If this is a replica, also show replication lag:
  kubectl exec -n wallet-eng-dev $POD -- \
    psql -U postgres -c "SELECT now() - pg_last_xact_replay_timestamp() AS replication_lag WHERE pg_is_in_recovery();" 2>/dev/null || true
done
```

#### C2: Database Logs

```bash
# Follow all DB pod logs
kubectl cnpg logs cluster wallet-backend-mainnet-db -n wallet-eng-dev -f

# Pretty-print with readable formatting
kubectl cnpg logs cluster wallet-backend-mainnet-db -n wallet-eng-dev -f | kubectl cnpg logs pretty

# Filter by log level (error, warning, info)
kubectl cnpg logs cluster wallet-backend-mainnet-db -n wallet-eng-dev -f | kubectl cnpg logs pretty --log-level error
```

#### C3: Direct Database Connection (psql)

```bash
kubectl cnpg psql wallet-backend-mainnet-db -n wallet-eng-dev
```

This opens an interactive psql session connected to the primary instance. Use for ad-hoc queries, schema inspection, or debugging.

#### C4: Instance Management

```bash
# Rolling restart of all DB instances (safe, one at a time)
kubectl cnpg restart wallet-backend-mainnet-db -n wallet-eng-dev

# Apply config changes without restart (e.g., after editing postgresql.conf params)
kubectl cnpg reload wallet-backend-mainnet-db -n wallet-eng-dev

# Destroy a specific instance (DANGEROUS — confirm with user first!)
# This permanently removes the instance; CNPG will recreate it from backup
kubectl cnpg destroy wallet-backend-mainnet-db <instance-number> -n wallet-eng-dev
```

#### C5: Backup

```bash
# Trigger an on-demand backup
kubectl cnpg backup wallet-backend-mainnet-db -n wallet-eng-dev
```

#### C6: Diagnostics

```bash
# Generate a diagnostic bundle (includes logs, configs, events)
kubectl cnpg report cluster wallet-backend-mainnet-db -n wallet-eng-dev --logs -f report.zip
```

#### C7: Hibernation (Cost Saving)

```bash
# Suspend the cluster (all instances stopped, PVCs retained)
# CONFIRM with user — this takes the database offline!
kubectl cnpg hibernate on wallet-backend-mainnet-db -n wallet-eng-dev

# Resume the cluster
kubectl cnpg hibernate off wallet-backend-mainnet-db -n wallet-eng-dev
```

#### C8: Benchmarking

```bash
# Quick pgbench run (30 seconds, 1 client)
kubectl cnpg pgbench wallet-backend-mainnet-db -n wallet-eng-dev -- --time 30 --client 1 --jobs 1
```

### Section D: Deploy a New Image Tag

This is a multi-step workflow. **Always confirm the image tag with the user before applying.**

1. **Verify context:**
   ```bash
   kubectl config current-context
   ```

2. **Read the current manifest** to understand the current image tag:
   Read the file at `/Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-mainnet.yaml`

3. **Update the image tag** in the manifest file. The image format is:
   ```
   docker-registry.services.stellar-ops.com/dev/wallet-backend:<tag>
   ```
   Edit both the API and Ingest deployment image fields to the new tag.

4. **Apply the updated manifest:**
   ```bash
   kubectl apply -f /Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-mainnet.yaml
   ```

5. **Monitor the rollout:**
   ```bash
   kubectl rollout status deployment/wallet-backend-api-mainnet -n wallet-eng-dev --timeout=120s
   kubectl rollout status deployment/wallet-backend-ingest-mainnet -n wallet-eng-dev --timeout=120s
   ```

6. **Verify pods are running:**
   ```bash
   kubectl get pods -n wallet-eng-dev -o wide
   ```

7. **Check logs for startup errors:**
   ```bash
   kubectl logs -n wallet-eng-dev deployment/wallet-backend-api-mainnet --tail=50
   kubectl logs -n wallet-eng-dev deployment/wallet-backend-ingest-mainnet --tail=50
   ```

### Section E: Scale Deployments

```bash
# Scale API deployment
kubectl scale deployment/wallet-backend-api-mainnet -n wallet-eng-dev --replicas=<N>

# Scale Ingest deployment (note: Recreate strategy — brief downtime during scale)
kubectl scale deployment/wallet-backend-ingest-mainnet -n wallet-eng-dev --replicas=<N>

# Verify
kubectl get pods -n wallet-eng-dev -o wide
```

### Section F: Exec & Port-Forward

**Exec into a running pod:**
```bash
# List pods first
kubectl get pods -n wallet-eng-dev

# Exec into API pod
kubectl exec -it -n wallet-eng-dev deployment/wallet-backend-api-mainnet -- /bin/sh

# Exec into Ingest pod
kubectl exec -it -n wallet-eng-dev deployment/wallet-backend-ingest-mainnet -- /bin/sh
```

**Port-forward to services:**
```bash
# Forward API service to localhost:8001
kubectl port-forward -n wallet-eng-dev svc/wallet-backend-api-mainnet 8001:80

# Forward directly to a DB pod (port 5432)
kubectl port-forward -n wallet-eng-dev pod/<db-pod-name> 5432:5432
```

### Section G: Composite Workflows

#### G1: Full Health Check

Run all of these in sequence:
1. `kubectl get all -n wallet-eng-dev`
2. CNPG status via CRD + psql (Section C1 — `kubectl cnpg status` is blocked; `curl`/`wget` not in the image):
   ```bash
   kubectl get cluster wallet-backend-mainnet-db -n wallet-eng-dev -o json | \
     jq '{phase: .status.phase, instances: .status.instances, readyInstances: .status.readyInstances, currentPrimary: .status.currentPrimary}'
   PODS=$(kubectl get pods -n wallet-eng-dev -l cnpg.io/cluster=wallet-backend-mainnet-db -o jsonpath='{.items[*].metadata.name}')
   for POD in $PODS; do
     echo "=== $POD ==="
     kubectl exec -n wallet-eng-dev $POD -- \
       psql -U postgres -c "SELECT pg_current_wal_lsn() AS current_lsn, pg_is_in_recovery() AS is_replica, version();"
   done
   ```
3. `kubectl get events -n wallet-eng-dev --sort-by='.lastTimestamp' --field-selector 'involvedObject.namespace=wallet-eng-dev'`
4. `kubectl top pods -n wallet-eng-dev`

Report a summary of: pod states, CNPG replication health, any warning/error events, and resource usage.

#### G2: Reset DB and Restart Deployments

**This is destructive — require explicit user confirmation at each step.**

1. Confirm with user that they want to delete and recreate the DB cluster.
2. Delete the CNPG cluster:
   ```bash
   kubectl delete -f /Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-cnpg-mainnet.yaml
   ```
3. Wait for pods to terminate:
   ```bash
   kubectl get pods -n wallet-eng-dev -w
   ```
4. Re-apply the CNPG manifest:
   ```bash
   kubectl apply -f /Users/adityavyas/Desktop/Work/kube/kube001-dev-eks/namespaces/wallet-eng-dev/freighter/wallet-backend-cnpg-mainnet.yaml
   ```
5. Wait for the DB cluster to become ready:
   ```bash
   kubectl cnpg status wallet-backend-mainnet-db -n wallet-eng-dev
   ```
6. Restart the application deployments:
   ```bash
   kubectl rollout restart deployment/wallet-backend-api-mainnet -n wallet-eng-dev
   kubectl rollout restart deployment/wallet-backend-ingest-mainnet -n wallet-eng-dev
   ```
7. Monitor rollout and check logs.

#### G3: Deploy New Version (End-to-End)

1. Get the image tag from the user.
2. Follow Section D (Deploy a New Image Tag) steps 1-7.
3. After successful rollout, run a health check (Section G1).

## Examples

### Example 1: Quick Status Check
User says: "What's the status of my deployment?"
Actions:
1. Run `kubectl config current-context` to verify cluster
2. Run `kubectl get pods -n wallet-eng-dev -o wide`
3. Run `kubectl get events -n wallet-eng-dev --sort-by='.lastTimestamp'` (last few)
4. Summarize pod health, restart counts, and any recent events

### Example 2: Check CNPG Health
User says: "Is the database healthy?"
Actions:
1. Fetch cluster-level state from the CRD:
   `kubectl get cluster wallet-backend-mainnet-db -n wallet-eng-dev -o json | jq '{phase: .status.phase, instances: .status.instances, readyInstances: .status.readyInstances, currentPrimary: .status.currentPrimary}'`
2. Query each pod via psql (curl/wget not available in the cnpg-timescaledb image):
   ```bash
   PODS=$(kubectl get pods -n wallet-eng-dev -l cnpg.io/cluster=wallet-backend-mainnet-db -o jsonpath='{.items[*].metadata.name}')
   for POD in $PODS; do
     echo "=== $POD ==="
     kubectl exec -n wallet-eng-dev $POD -- \
       psql -U postgres -c "SELECT pg_current_wal_lsn() AS current_lsn, pg_is_in_recovery() AS is_replica, version();"
   done
   ```
3. Report cluster state, WAL LSN, recovery state (primary vs replica), and pod readiness
4. If issues found, check DB logs: `kubectl cnpg logs cluster wallet-backend-mainnet-db -n wallet-eng-dev | kubectl cnpg logs pretty --log-level error`

### Example 3: View DB Error Logs
User says: "Show me database error logs"
Actions:
1. Check CNPG plugin is available
2. Run `kubectl cnpg logs cluster wallet-backend-mainnet-db -n wallet-eng-dev | kubectl cnpg logs pretty --log-level error`
3. Summarize errors found

### Example 4: Connect to Database
User says: "I need to run a query on the database"
Actions:
1. Run `kubectl cnpg psql wallet-backend-mainnet-db -n wallet-eng-dev`
2. This opens an interactive psql session

### Example 5: Deploy New Image
User says: "Deploy image tag abc123"
Actions:
1. Verify kubectl context
2. Read current manifest
3. Update image tag to `docker-registry.services.stellar-ops.com/dev/wallet-backend:abc123`
4. Confirm with user before applying
5. Apply and monitor rollout
6. Check pod logs for errors

## Troubleshooting

### CNPG plugin not found
Cause: `kubectl-cnpg` is not installed.
Solution: Run `brew install kubectl-cnpg` and retry.

### Pods stuck in CrashLoopBackOff
Cause: Application failing to start (bad config, DB not ready, etc.)
Solution:
1. Check logs: `kubectl logs -n wallet-eng-dev <pod-name> --previous`
2. Check events: `kubectl describe pod -n wallet-eng-dev <pod-name>`
3. Check ConfigMap: `kubectl get configmap wallet-backend-env-mainnet -n wallet-eng-dev -o yaml`

### CNPG cluster not ready
Cause: Database instances haven't finished starting or recovering.
Solution:
1. Check CRD state (always works): `kubectl get cluster wallet-backend-mainnet-db -n wallet-eng-dev -o json | jq '{phase: .status.phase, readyInstances: .status.readyInstances, conditions: [.status.conditions[] | {type, status, message}]}'`
2. Check psql reachability: `kubectl exec -n wallet-eng-dev <pod-name> -- psql -U postgres -c "SELECT pg_current_wal_lsn(), pg_is_in_recovery();"`
3. Check DB logs: `kubectl cnpg logs cluster wallet-backend-mainnet-db -n wallet-eng-dev | kubectl cnpg logs pretty --log-level error`
4. Check events: `kubectl get events -n wallet-eng-dev --field-selector 'involvedObject.name=wallet-backend-mainnet-db'`

### Wrong kubectl context
Cause: kubectl is pointed at the wrong cluster.
Solution: Switch context with `kubectl config use-context <correct-context>`. The expected context should reference `kube001-dev-eks`.

### `kubectl cnpg status` hangs with no output
Cause: EKS node security group blocks port 8000 from the API server. The `cnpg status`
plugin uses the `pods/proxy` subresource (API Server -> Pod IP:8000), which requires
port 8000 to be open in the node SG. Other cnpg commands (`psql`, `logs`, `restart`)
work because they use `exec` (which goes through kubelet on port 10250).
Solution: Use the exec-based workaround in Section C1 above.
Permanent fix: Add a node SG rule allowing the cluster SG to reach port 8000 on nodes
(requires Terraform change to the EKS module).

## Performance Notes

- Always verify the kubectl context before running any command
- For destructive operations, take your time and confirm each step
- Quality and safety are more important than speed
- Do not skip confirmation steps for delete/destroy/hibernate operations
