#!/bin/sh
# Download the JVM dependencies needed by PySpark (Delta Lake, Kafka source, S3A)
# straight into the pyspark/jars folder so no Ivy resolution happens at runtime.
set -eu

MAVEN="https://repo1.maven.org/maven2"
JARS_DIR="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"

SPARK_VERSION="3.5.3"
DELTA_VERSION="3.2.1"
HADOOP_VERSION="3.3.4"

for path in \
  "io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
  "io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
  "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
  "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
  "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
  "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
  "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
do
  file="$(basename "$path")"
  echo "Downloading ${file}"
  curl -fsSL --retry 3 -o "${JARS_DIR}/${file}" "${MAVEN}/${path}"
done

echo "Jars installed into ${JARS_DIR}"
