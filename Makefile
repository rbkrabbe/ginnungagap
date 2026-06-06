CLUSTER       ?= ggap
IMAGE         ?= ginnungagap:dev
CONSOLE_IMAGE ?= ginnungagap-console:dev
NS            ?= ginnungagap

.PHONY: help kind-up build build-node build-console load-console deploy wait bootstrap urls status logs down up reset

help:
	@echo "Ginnungagap local Kind cluster targets:"
	@echo "  make up             - full end-to-end: kind, build, deploy, wait, bootstrap"
	@echo "  make kind-up        - create the kind cluster only"
	@echo "  make build          - build node + console images and kind load both"
	@echo "  make build-node     - docker build ggap-node image + kind load"
	@echo "  make build-console  - docker build console image + kind load"
	@echo "  make deploy         - kubectl apply -k deploy/k8s"
	@echo "  make wait           - wait for ggap / otel / prometheus / grafana / console rollouts"
	@echo "  make bootstrap      - run the Job that forms the 3-voter Raft cluster"
	@echo "  make urls           - print service URLs"
	@echo "  make status         - show pods"
	@echo "  make logs           - follow ggap-0 logs"
	@echo "  make reset          - delete and recreate the StatefulSet's PVCs and pods"
	@echo "  make down           - delete the kind cluster"

kind-up:
	kind create cluster --config deploy/kind/kind-config.yaml --name $(CLUSTER)

build-node:
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)

build-console:
	docker build -t $(CONSOLE_IMAGE) console/
	kind load docker-image $(CONSOLE_IMAGE) --name $(CLUSTER)

build: build-node build-console

deploy:
	kubectl apply -k deploy/k8s/

wait:
	kubectl -n $(NS) rollout status statefulset/ggap --timeout=300s
	kubectl -n $(NS) rollout status deploy/otel-collector --timeout=180s
	kubectl -n $(NS) rollout status deploy/prometheus --timeout=180s
	kubectl -n $(NS) rollout status deploy/grafana --timeout=180s
	kubectl -n $(NS) rollout status deploy/console --timeout=120s

# Applied separately from the root kustomization: Job pod templates are
# immutable, so keeping it out of `kubectl apply -k` lets re-apply stay
# idempotent. The Job itself is idempotent (exits 0 if 3 voters already).
bootstrap:
	kubectl -n $(NS) delete job ggap-bootstrap --ignore-not-found
	kubectl apply -f deploy/k8s/bootstrap/job.yaml
	kubectl -n $(NS) wait --for=condition=complete --timeout=120s job/ggap-bootstrap
	@echo "--- bootstrap Job logs ---"
	@kubectl -n $(NS) logs job/ggap-bootstrap

urls:
	@echo "Console:     http://localhost:8080"
	@echo "Grafana:     http://localhost:3000  (admin / admin)"
	@echo "ggap gRPC:   localhost:17000        (KvService NodePort)"
	@echo "ggap admin:  localhost:17001        (AdminService NodePort)"

status:
	@kubectl -n $(NS) get pods -o wide

logs:
	kubectl -n $(NS) logs -f ggap-0

reset:
	kubectl -n $(NS) delete job ggap-bootstrap --ignore-not-found
	kubectl -n $(NS) delete statefulset ggap --ignore-not-found
	kubectl -n $(NS) delete pvc -l app.kubernetes.io/name=ggap-node --ignore-not-found
	kubectl apply -k deploy/k8s/

up: kind-up build deploy wait bootstrap urls

down:
	kind delete cluster --name $(CLUSTER)
