FROM apache/airflow:2.9.3

ARG AIRFLOW_VERSION=2.9.3
ARG PY_MINOR=3.12
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_MINOR}.txt
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"

COPY requirements.txt /tmp/requirements.txt

RUN set -eux; \
    python --version; \
    pip --version; \
    echo "Using constraints: ${CONSTRAINT_URL}"; \
    pip install --no-cache-dir -r /tmp/requirements.txt -c "${CONSTRAINT_URL}"
