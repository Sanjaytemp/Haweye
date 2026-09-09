# =============================================================================
# Spark image with everything this project needs baked in:
#   * Apache Iceberg            (open table format / lakehouse)
#   * hadoop-aws + AWS SDK      (s3a:// -> MinIO)
#   * postgresql JDBC driver    (Iceberg catalog metadata lives in Postgres)
#   * the small python deps the executors need
#
# Why not plain `bitnami/spark` + `--packages`?  Because `--packages` resolves
# jars from Maven Central on *every* job submission (slow, flaky, offline-hostile).
# Building once pins the versions and makes restarts instant — that is what a
# production image looks like.
#
# Build context = repo root (see docker-compose.yml `build.context: .`).
# =============================================================================
ARG SPARK_VERSION=3.5.1

# ---------- stage 1: fetch the jars (uses python only, no curl/wget needed) ----------
FROM docker.io/library/eclipse-temurin:17-jre-jammy AS jars
ARG SPARK_VERSION=3.5.1
ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=2.25.6
ARG POSTGRES_JDBC_VERSION=42.7.3

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY infra/docker/spark/download_jars.py /tmp/download_jars.py
RUN python3 /tmp/download_jars.py /opt/jars \
      "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION}" \
      "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}" \
      "software.amazon.awssdk:bundle:${AWS_SDK_VERSION}" \
      "org.postgresql:postgresql:${POSTGRES_JDBC_VERSION}" \
 && ls -l /opt/jars

# ---------- stage 2: the actual runtime image ----------
FROM docker.io/bitnami/spark:${SPARK_VERSION}

ARG SPARK_VERSION=3.5.1
COPY --from=jars /opt/jars/*.jar /opt/spark/jars/
COPY infra/docker/spark/requirements.txt /tmp/requirements.txt
COPY jobs /opt/haweye/jobs

USER root
RUN /opt/bitnami/python/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm -f /tmp/requirements.txt \
 && chmod -R a+rX /opt/spark/jars \
 && echo "haweye spark=${SPARK_VERSION} iceberg runtime + s3a + jdbc" > /opt/spark/IMAGE_INFO

USER 1001
ENV PYTHONPATH=/opt/haweye/jobs \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
