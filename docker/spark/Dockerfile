FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/lakehouse

COPY spark/requirements.txt /tmp/requirements.txt
COPY common/download_jars.sh /tmp/download_jars.sh

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && sed -i 's/\r$//' /tmp/download_jars.sh \
 && sh /tmp/download_jars.sh

WORKDIR /opt/lakehouse
