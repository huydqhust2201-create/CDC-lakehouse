"""SparkSession factory with Delta Lake + MinIO (S3A) configured."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakehouse import config


def get_spark(app_name: str, master: str | None = None, s3: bool | None = None) -> SparkSession:
    """Build (or reuse) a SparkSession.

    Required jars (delta, kafka, hadoop-aws) are baked into the Docker images'
    pyspark/jars folder, so no ``spark.jars.packages`` resolution is needed.
    """
    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    if s3 is None:
        s3 = config.LAKEHOUSE_ROOT.startswith("s3a://")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Schema evolution for MERGE INTO (new source columns are added to the target).
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if s3:
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", config.S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", config.S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.S3_ENDPOINT.startswith("https")).lower())
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
