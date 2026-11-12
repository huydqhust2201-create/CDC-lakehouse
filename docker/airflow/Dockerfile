FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
 && mkdir -p /opt/lakehouse && chown -R airflow:root /opt/lakehouse

ENV JAVA_HOME=/opt/java \
    PYTHONPATH=/opt/lakehouse

USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
COPY common/download_jars.sh /tmp/download_jars.sh

# Keep the Airflow version pinned while adding Spark / Delta / GE.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
 && sed 's/\r$//' /tmp/download_jars.sh > /tmp/download_jars_lf.sh \
 && sh /tmp/download_jars_lf.sh
