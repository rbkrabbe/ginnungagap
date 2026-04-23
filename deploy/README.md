# Ginnungagap on Kind

Local observability harness: three Ginnungagap pods, an OpenTelemetry
Collector scraping their Prometheus metrics, a minimal Prometheus for PromQL,
and Grafana with a provisioned dashboard.

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

## Known limitation: cluster formation

`ggap-node` today single-node-bootstraps a Raft group on any fresh data dir
(`crates/ggap-node/src/main.rs:238-274`). Because all three pods start from
empty PVCs and there is no CLI surface to skip that bootstrap, **this deploy
runs three independent single-node Raft clusters, not one 3-voter quorum**.

The observability pipeline still works as intended — OTel discovers and scrapes
all three pods, Grafana fans them in side-by-side — and each pod's
`KvService` is usable on its own.

To exercise true Raft quorum on this harness, scale down to a real single
node:

```
kubectl -n ginnungagap scale sts/ggap --replicas=1
```

Forming an actual 3-voter quorum needs a future `--initial-members` or
`--seed` flag on `ggap-node` plus either a bootstrap Job or a pre-written
`bootstrap_members` meta key. That work is tracked for a later pass.

## Caveats

- **No metric history after `make down`** — Prometheus uses an `emptyDir` with
  1h retention. Data is lost when the pod is deleted.
- **Dev credentials** — Grafana ships with `admin/admin`. Do not deploy this
  manifest set outside a local machine.
- **PVC retention** is `Delete` on StatefulSet deletion for convenience; `make
  reset` and `make down` both drop ggap data.
