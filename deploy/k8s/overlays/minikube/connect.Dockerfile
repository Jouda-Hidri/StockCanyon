# Kafka Connect with the Debezium PostgreSQL plugin, built inside minikube's Docker so nothing is
# pushed to a registry. Production uses Strimzi's build (deploy/k8s/base/kafka.yaml), which needs one.
FROM quay.io/strimzi/kafka:1.2.0-0-kafka-4.3.1
USER root
RUN mkdir -p /opt/kafka/plugins/debezium-postgres \
 && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.6.3.Final/debezium-connector-postgres-3.6.3.Final-plugin.tar.gz \
    | tar -xz -C /opt/kafka/plugins/debezium-postgres
USER 1001
