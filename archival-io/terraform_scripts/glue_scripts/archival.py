import sys
import json
import re
import uuid
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, current_date, add_months,
    md5, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# ------------------ Logging ------------------ #
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------ Parse Arguments ------------------ #
raw_args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'glue_connection',
    'source_database',
    'source_schema',
    'source_table',
    'target_database',
    'target_table',
    'target_s3_path',
    'retention_policy',
    'legal_hold',
])

def read_optional_param(param: str) -> Optional[str]:
    try:
        idx = sys.argv.index(f"--{param}")
        return sys.argv[idx + 1]
    except Exception:
        return None

override_sql = read_optional_param("query")
row_filter = read_optional_param("condition")

source_db = raw_args['source_database']
schema = raw_args['source_schema']
table = raw_args['source_table']
target_db = raw_args['target_database']
target_tbl = raw_args['target_table']
conn_name = raw_args['glue_connection']
warehouse_path = raw_args['target_s3_path']
retain_months = int(raw_args['retention_policy'])
legal_hold_flag = raw_args['legal_hold'].lower() == "true"

# ------------------ Glue & Spark Setup ------------------ #
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(raw_args['JOB_NAME'], raw_args)

def configure_spark_catalog(warehouse: str):
    confs = {
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": warehouse,
        "fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
        "fs.s3a.path.style.access": "false",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    }
    for key, value in confs.items():
        spark.conf.set(key, value)

configure_spark_catalog(warehouse_path)

# ------------------ Glue DB Safety Check ------------------ #
def validate_or_create_glue_db(db: str, region: str = "eu-west-1") -> None:
    client = boto3.client("glue", region_name=region)
    try:
        client.get_database(Name=db)
        logging.info(f"Glue DB exists: {db}")
    except ClientError as err:
        code = err.response.get("Error", {}).get("Code")
        if code == "EntityNotFoundException":
            client.create_database(DatabaseInput={"Name": db})
            logging.info(f"Glue DB created: {db}")
        elif code == "AlreadyExistsException":
            logging.info(f"Race condition hit; DB already created: {db}")
        else:
            raise

# ------------------ Helpers ------------------ #
def build_checksum_column(df: DataFrame) -> DataFrame:
    """
    Generates a checksum by hashing all fields into one column.
    """
    transformed = [coalesce(col(c).cast("string"), lit("NULL")) for c in df.columns]
    return df.withColumn("checksum", md5(concat_ws("||", *transformed)))

def get_base_table_name(sql: str) -> str:
    """Extracts table name from SQL string."""
    compact = " ".join(sql.strip().split())
    match = re.search(r'\bfrom\s+([a-zA-Z0-9_\.]+)', compact, re.IGNORECASE)
    if match:
        return match.group(1)
    raise ValueError("Cannot parse table from SQL query.")

# ------------------ Data Ingestion ------------------ #
def load_data(conn: str, db: str, source: str) -> DataFrame:
    dyn_df = glue_ctx.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": source,
            "database": db,
            "connectionName": conn,
        }
    )
    df = dyn_df.toDF()

    if row_filter:
        df = df.filter(row_filter)

    if override_sql:
        temp_view = "source_temp_view"
        df.createOrReplaceTempView(temp_view)
        patched_sql = re.sub(r'from\s+' + re.escape(source), f'from {temp_view}', override_sql, flags=re.IGNORECASE)
        df = spark.sql(patched_sql)

    df = df \
        .withColumn("archived_date", current_date()) \
        .withColumn("retention_expiry_date", add_months(current_date(), retain_months)) \
        .withColumn("legal_hold", lit(legal_hold_flag))

    df = build_checksum_column(df)
    df = df.select([col(c).alias(c.strip().lower()) for c in df.columns])

    logging.info(f"Dataframe schema:")
    df.printSchema()
    return df

# ------------------ Iceberg Write ------------------ #
def write_to_iceberg_table(df: DataFrame, db: str, tbl: str):
    full_path = f"glue_catalog.{db.lower()}.{tbl.lower()}"
    df.write.format("iceberg").mode("overwrite").saveAsTable(full_path)
    logging.info(f"Written to Iceberg table: {full_path}")

# ------------------ Main Execution ------------------ #
if __name__ == "__main__":
    target_ref = f"{schema}.{table}"
    if override_sql:
        target_ref = get_base_table_name(override_sql)
        logging.info(f"Resolved source from query: {target_ref}")

    final_df = load_data(conn=conn_name, db=source_db, source=target_ref)

    validate_or_create_glue_db(target_db)
    write_to_iceberg_table(final_df, db=target_db, tbl=target_tbl)

    logging.info("Archival job completed.")
    job.commit()
