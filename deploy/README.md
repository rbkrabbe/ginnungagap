# Ginnungagap on Kind

Local 3-node Ginnungagap cluster with an OpenTelemetry Collector scraping
Prometheus metrics from every pod, a minimal Prometheus for PromQL, and
Grafana with a provisioned dashboard.

## Prerequisites

- Docker
- `kind` (tested with v0.23+)
- `kubectl`
- `grpcurl` (only needed for the verification commands below)

## One-shot bring-up

```
make up
```

That runs:

1. `make kind-up` — create a 1 control-plane + 3 worker Kind cluster named `ggap`
2. `make build` — build the `ggap-node` image and `kind load` it
3. `make deploy` — `kubectl apply -k deploy/k8s`
4. `make wait` — wait for ggap / OTel / Prometheus / Grafana rollouts
5. `make bootstrap` — run a Job that forms the 3-voter Raft cluster on shard 0

After it completes:

- Grafana: <http://localhost:3000> (admin / admin). Folder **Ginnungagap**
  holds the provisioned **Ginnungagap — Overview** dashboard.
- ggap client gRPC on `localhost:17000` (NodePort 30700 on the control-plane
  node, mapped to host port 17000 via Kind `extraPortMappings`).

## Pipeline

```
ggap-{0,1,2}           :9090/   (Prometheus text, served at path "/")
      |
      v
OTel Collector         (prometheus receiver + kubernetes_sd pod discovery)
      |
      |  prometheus exporter on :8889
      v
Prometheus             (emptyDir, 1h retention)
      |
      v
Grafana                (provisioned Prometheus datasource + dashboard)
```

The metrics endpoint on `ggap-node` is `/`, not `/metrics` —
`deploy/k8s/otel/configmap.yaml` sets `metrics_path: /` accordingly.

## Useful commands

```
make status         # pods
make logs           # follow ggap-0
make reset          # delete StatefulSet + PVCs and redeploy
make down           # delete the Kind cluster
```

Try a Put/Get through the NodePort:

```
grpcurl -plaintext localhost:17000 list
grpcurl -plaintext -d '{"key":"aGVsbG8=","value":"d29ybGQ="}' \
  localhost:17000 ginnungagap.v1.KvService/Put
grpcurl -plaintext -d '{"key":"aGVsbG8="}' \
  localhost:17000 ginnungagap.v1.KvService/Get
```

The Grafana dashboard should show `ggap_kv_requests_total` climbing within
~30 seconds (OTel scrape + Prometheus scrape intervals stacked).

## How cluster formation works

`ggap-node` normally self-bootstraps a fresh data dir as a single-voter
cluster. With three pods starting simultaneously that would produce three
separate clusters, so a `--seed` flag gates that behavior: only `ggap-0`
(ordinal 0) runs with `--seed`. `ggap-1` and `ggap-2` start uninitialized —
openraft instances that will accept `AppendEntries` but do nothing on their
own.

The `ggap-bootstrap` Job (run by `make bootstrap`) then calls AdminService
on `ggap-0.ggap-headless.ginnungagap.svc:17001`:

1. `AddLearner` for node_id 2 (ggap-1) and node_id 3 (ggap-2)
2. `ChangeMembership([1, 2, 3])` to promote the learners to voters

The Job is idempotent: if `ClusterStatus` already reports 3 voters it exits 0.
It is kept out of the root `kubectl apply -k` target because Job pod templates
are immutable, which would break re-apply.

To inspect the Raft state from inside the cluster:

```
kubectl -n ginnungagap run --rm -it grpcurl \
  --image=fullstorydev/grpcurl:v1.9.1-alpine --restart=Never -- \
  -plaintext ggap-0.ggap-headless.ginnungagap.svc:17001 \
  ginnungagap.v1.AdminService/ClusterStatus
```

## Caveats

- **No metric history after `make down`** — Prometheus uses an `emptyDir` with
  1h retention. Data is lost when the pod is deleted.
- **Dev credentials** — Grafana ships with `admin/admin`. Do not deploy this
  manifest set outside a local machine.
- **PVC retention** is `Delete` on StatefulSet deletion for convenience; `make
  reset` and `make down` both drop ggap data.
