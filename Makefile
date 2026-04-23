CLUSTER ?= ggap
IMAGE   ?= ginnungagap:dev
NS      ?= ginnungagap

.PHONY: help kind-up build deploy wait grafana status logs down up reset

help:
	@echo "Ginnungagap local Kind cluster targets:"
	@echo "  make up          - full end-to-end: create kind, build image, deploy, wait"
	@echo "  make kind-up     - create the kind cluster only"
	@echo "  make build       - docker build + kind load docker-image"
	@echo "  make deploy      - kubectl apply -k deploy/k8s"
	@echo "  make wait        - wait for ggap / otel / grafana rollouts"
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

grafana:
	@echo "Grafana:    http://localhost:3000  (admin / admin)"
	@echo "ggap gRPC:  localhost:17000        (client NodePort)"

status:
	@kubectl -n $(NS) get pods -o wide

logs:
	kubectl -n $(NS) logs -f ggap-0

reset:
	kubectl -n $(NS) delete statefulset ggap --ignore-not-found
	kubectl -n $(NS) delete pvc -l app.kubernetes.io/name=ggap-node --ignore-not-found
	kubectl apply -k deploy/k8s/

up: kind-up build deploy wait grafana

down:
	kind delete cluster --name $(CLUSTER)
