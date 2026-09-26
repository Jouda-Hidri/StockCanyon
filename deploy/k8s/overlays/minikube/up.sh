#!/usr/bin/env bash
# Deploys everything to a dedicated minikube profile. No registry: both images are built inside
# minikube's Docker. Re-runnable.
#
#   deploy/k8s/overlays/minikube/up.sh
#   kubectl -n marketdata port-forward svc/grafana 3000:3000                  # dashboards
#   kubectl -n marketdata port-forward svc/marketdata-distribution 8099:80   # the API
set -euo pipefail

PROFILE=${PROFILE:-stockcanyon}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../../.." && pwd)

# A profile of its own, so no other cluster is touched.
if ! minikube status -p "$PROFILE" >/dev/null 2>&1; then
  minikube start -p "$PROFILE" --memory="${MEMORY:-8g}" --cpus="${CPUS:-6}" --disk-size=30g
fi
kubectl config use-context "$PROFILE"
kubectl create namespace marketdata --dry-run=client -o yaml | kubectl apply -f -

echo "--- operators"
kubectl apply --server-side -f https://github.com/cloudnative-pg/cloudnative-pg/releases/download/v1.30.1/cnpg-1.30.1.yaml
curl -fsSL https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-cluster-operator-1.2.0.yaml \
  | sed 's/namespace: .*/namespace: marketdata/' | kubectl apply --server-side -n marketdata -f -
kubectl apply --server-side -k "$HERE/redis-operator"
kubectl apply --server-side -f https://github.com/prometheus-operator/prometheus-operator/releases/download/v0.94.1/bundle.yaml
kubectl -n cnpg-system wait --for=condition=Available deployment --all --timeout=300s
kubectl -n marketdata wait --for=condition=Available deployment/strimzi-cluster-operator --timeout=300s
kubectl -n redis-operator-system wait --for=condition=Available deployment --all --timeout=300s
kubectl -n default wait --for=condition=Available deployment/prometheus-operator --timeout=300s

echo "--- images (inside minikube)"
minikube -p "$PROFILE" image build -t stockcanyon:local "$ROOT"
minikube -p "$PROFILE" image build -t marketdata-connect:local -f connect.Dockerfile "$HERE"

echo "--- secrets"
if ! kubectl -n marketdata get secret marketdata-db-debezium >/dev/null 2>&1; then
  kubectl -n marketdata create secret generic marketdata-db-debezium --type=kubernetes.io/basic-auth \
    --from-literal=username=debezium --from-literal=password="$(openssl rand -hex 16)"
fi

echo "--- the stack"
kubectl apply --server-side -k "$HERE"
kubectl -n marketdata wait --for=condition=Ready cluster/marketdata-db --timeout=600s
kubectl -n marketdata wait --for=condition=Ready kafka/marketdata-kafka --timeout=600s
kubectl -n marketdata wait --for=condition=Ready kafkaconnector/marketdata-outbox --timeout=600s
kubectl -n marketdata rollout status deployment/marketdata-ingest --timeout=600s
kubectl -n marketdata rollout status deployment/marketdata-distribution --timeout=600s
kubectl -n marketdata get pods
