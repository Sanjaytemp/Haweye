# =============================================================================
# Airflow image: providers + the DAG code copied in (so the image is
# reproducible) while ./airflow/dags is *also* bind-mounted for a fast
# edit-restart loop during development.
# =============================================================================
ARG AIRFLOW_VERSION=2.8.0
ARG PYTHON_VERSION=3.11

FROM docker.io/apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
ENV AIRFLOW_PIP_INDEX_URL="" \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY infra/docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

# DAGs live in the image *and* are mounted at runtime; mounts win during dev.
COPY airflow/dags /opt/airflow/dags
COPY airflow/tests /opt/airflow/tests
COPY infra/scripts /opt/airflow/scripts
COPY sql /opt/airflow/sql
COPY jobs /opt/airflow/jobs

RUN chmod +x /opt/airflow/scripts/*.sh 2>/dev/null || true \
 && mkdir -p /opt/airflow/logs /opt/airflow/dags \
 && chown -R airflow:0 /opt/airflow \
 && chmod -R g+rwX /opt/airflow

USER airflow
ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/jobs:/opt/airflow/scripts
