CLUSTER ?= ggap
IMAGE   ?= ginnungagap:dev
NS      ?= ginnungagap

.PHONY: help kind-up build deploy wait bootstrap grafana status logs down up reset

help:
	@echo "Ginnungagap local Kind cluster targets:"
	@echo "  make up          - full end-to-end: kind, build, deploy, wait, bootstrap"
	@echo "  make kind-up     - create the kind cluster only"
	@echo "  make build       - docker build + kind load docker-image"
	@echo "  make deploy      - kubectl apply -k deploy/k8s"
	@echo "  make wait        - wait for ggap / otel / prometheus / grafana rollouts"
	@echo "  make bootstrap   - run the Job that forms the 3-voter Raft cluster"
	@echo "  make grafana     - print Grafana URL (NodePort mapped to localhost:3000)"
	@echo "  make status      - show pods"
	@echo "  make logs        - follow ggap-0 logs"
	@echo "  make reset       - delete and recreate the StatefulSet's PVCs and pods"
	@echo "  make down        - delete the kind cluster"

kind-up:
	kind create cluster --config deploy/kind/kind-config.yaml --name $(CLUSTER)

build:
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)

deploy:
	kubectl apply -k deploy/k8s/

wait:
	kubectl -n $(NS) rollout status statefulset/ggap --timeout=300s
	kubectl -n $(NS) rollout status deploy/otel-collector --timeout=180s
	kubectl -n $(NS) rollout status deploy/prometheus --timeout=180s
	kubectl -n $(NS) rollout status deploy/grafana --timeout=180s

# Applied separately from the root kustomization: Job pod templates are
# immutable, so keeping it out of `kubectl apply -k` lets re-apply stay
# idempotent. The Job itself is idempotent (exits 0 if 3 voters already).
bootstrap:
	kubectl -n $(NS) delete job ggap-bootstrap --ignore-not-found
	kubectl apply -f deploy/k8s/bootstrap/job.yaml
	kubectl -n $(NS) wait --for=condition=complete --timeout=120s job/ggap-bootstrap
	@echo "--- bootstrap Job logs ---"
	@kubectl -n $(NS) logs job/ggap-bootstrap

grafana:
	@echo "Grafana:    http://localhost:3000  (admin / admin)"
	@echo "ggap gRPC:  localhost:17000        (client NodePort)"

status:
	@kubectl -n $(NS) get pods -o wide

logs:
	kubectl -n $(NS) logs -f ggap-0

reset:
	kubectl -n $(NS) delete job ggap-bootstrap --ignore-not-found
	kubectl -n $(NS) delete statefulset ggap --ignore-not-found
	kubectl -n $(NS) delete pvc -l app.kubernetes.io/name=ggap-node --ignore-not-found
	kubectl apply -k deploy/k8s/

up: kind-up build deploy wait bootstrap grafana

down:
	kind delete cluster --name $(CLUSTER)
