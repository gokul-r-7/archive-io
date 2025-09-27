#!/usr/bin/env python3
"""
validation_io.py (refactored version)

- Preserves all behaviors from the original script:
  * Read source (Oracle) via Glue connection
  * Read target Iceberg table via Spark SQL
  * Optional inline SQL query and server-side condition pushdown
  * Normalization for datatypes (date/timestamp, decimal/float/double, ints, boolean, string)
  * Column-level and row-level checksum validations
  * Row-count and schema validations
  * Writes a validation summary to S3 (CSV + JSON)
- Reorganized into classes and helper modules with different names and naming conventions
"""

import sys
import re
import json
import logging
from typing import List, Dict, Any, Optional

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------
# Logging configuration
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validation_io_refactored")

# ---------------------------
# CLI / ARG Parsing
# ---------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "glue_connection",
        "source_schema",
        "source_table",
        "target_database",
        "target_table",
    ],
)

GLUE_CONN = args["glue_connection"]
SRC_SCHEMA = args["source_schema"]
SRC_TABLE = args["source_table"]
TGT_DB = args["target_database"]
TGT_TABLE = args["target_table"]

# Optional CLI flags parsed manually (same approach as original)
def _cli_opt(flag_name: str) -> Optional[str]:
    flag = f"--{flag_name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return None

INLINE_QUERY = _cli_opt("query")        # optional inline SQL
PUSHDOWN_CONDITION = _cli_opt("condition")  # optional condition to filter both sides

if INLINE_QUERY:
    # try to extract the referenced table name from inline query (used when temp view substitution needed)
    def _table_from_sql(q: str) -> str:
        cleaned = " ".join(q.strip().split())
        m = re.search(r"\bfrom\s+([a-zA-Z0-9_\.]+)", cleaned, flags=re.IGNORECASE)
        if not m:
            raise ValueError("Unable to locate table name in query")
        return m.group(1)
    try:
        QUERY_TABLE_NAME = _table_from_sql(INLINE_QUERY)
    except Exception:
        QUERY_TABLE_NAME = None
else:
    QUERY_TABLE_NAME = None

logger.info(f"Job starting: {args['JOB_NAME']}")
logger.info(f"Configured source: {SRC_SCHEMA}.{SRC_TABLE} -> target: {TGT_DB}.{TGT_TABLE}")
if PUSHDOWN_CONDITION:
    logger.info(f"Pushdown condition provided: {PUSHDOWN_CONDITION}")
if INLINE_QUERY:
    logger.info("Inline query provided; will substitute temp view if needed.")

# ---------------------------
# Spark & Glue init (do not re-create SparkSession outside Glue)
# ---------------------------
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = (
    SparkSession.builder.appName("validation_io_refactored")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)
logger.info("Spark & Glue contexts initialized")

# ---------------------------
# Loader classes
# ---------------------------

class SourceLoader:
    """Loads the source from Oracle via Glue dynamic frame and returns a Spark DataFrame."""
    def __init__(self, connection_name: str, schema_name: str, table_name: str, condition: Optional[str], query: Optional[str]):
        self.conn = connection_name
        self.schema = schema_name
        self.table = table_name
        self.condition = condition
        self.query = query

    def read(self) -> DataFrame:
        dbtable = f"{self.schema}.{self.table}"
        logger.info(f"Loading source table '{dbtable}' via Glue connection '{self.conn}'")
        dyf = glue_ctx.create_dynamic_frame.from_options(
            connection_type="oracle",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": dbtable,
                "connectionName": self.conn,
            },
        )
        df = dyf.toDF()
        if self.condition:
            logger.info("Applying source-side condition filter")
            df = df.filter(self.condition)
        if self.query:
            # run query against a temp view created from the loaded table data
            temp = "src_temp_view"
            df.createOrReplaceTempView(temp)
            logger.info(f"Created temp view '{temp}' for inline query execution")
            # replace the original table reference in query with temp view
            if QUERY_TABLE_NAME:
                modified = re.sub(r"\bfrom\s+" + re.escape(QUERY_TABLE_NAME), f"from {temp}", self.query, flags=re.IGNORECASE)
            else:
                modified = self.query
            logger.info(f"Executing modified query on temp view")
            df = spark.sql(modified)
        # unify column names (lower-case, trimmed)
        df = df.select([F.col(c).alias(c.strip().lower()) for c in df.columns])
        logger.info(f"Source loaded: rows={df.count()}, cols={df.columns}")
        df.printSchema()
        return df

class TargetLoader:
    """Loads the Iceberg target table into a DataFrame using Spark SQL."""
    def __init__(self, glue_db: str, table_name: str, condition: Optional[str], query: Optional[str]):
        self.glue_db = glue_db
        self.table = table_name
        self.condition = condition
        self.query = query

    def read(self) -> DataFrame:
        full = f"glue_catalog.{self.glue_db}.{self.table}".lower()
        logger.info(f"Reading target table '{full}' via spark.sql")
        df = spark.sql(f"SELECT * FROM {full}")
        if self.condition:
            logger.info("Applying target-side condition filter")
            df = df.filter(self.condition)
        if self.query:
            # modify query to reference the full target table
            modified = re.sub(r"\bfrom\s+" + re.escape(QUERY_TABLE_NAME or ""), f"from {full}", self.query, flags=re.IGNORECASE)
            logger.info("Executing modified inline query against target table")
            df = spark.sql(modified)
        df = df.select([F.col(c).alias(c.strip().lower()) for c in df.columns])
        logger.info(f"Target loaded: rows={df.count()}, cols={df.columns}")
        df.printSchema()
        return df

# ---------------------------
# Normalizer
# ---------------------------

class DataFrameNormalizer:
    """
    Applies deterministic normalization rules column-by-column so checksums remain stable:
     - date/timestamp -> yyyy-MM-dd HH:mm:ss
     - decimal/double/float -> formatted to 5 decimal places
     - ints -> cast to string
     - boolean -> 'true'/'false' or 'null'
     - strings -> trimmed, lower-cased
    """
    def __init__(self, ignore_columns: List[str]):
        self.ignore_columns = ignore_columns or []

    def normalize(self, df: DataFrame) -> DataFrame:
        logger.info("Starting normalization process")
        for colname in df.columns:
            if colname in self.ignore_columns:
                continue
            dtype = str(df.schema[colname].dataType).lower()
            try:
                if "date" in dtype or "timestamp" in dtype:
                    df = df.withColumn(
                        colname,
                        F.when(F.col(colname).isNull(), F.lit("null"))
                        .otherwise(F.date_format(F.col(colname), "yyyy-MM-dd HH:mm:ss")),
                    )
                elif any(x in dtype for x in ("decimal", "double", "float")):
                    df = df.withColumn(
                        colname,
                        F.when(F.col(colname).isNull(), F.lit("null"))
                        .otherwise(F.format_number(F.col(colname).cast("double"), 5)),
                    )
                elif any(x in dtype for x in ("int", "bigint", "smallint")):
                    df = df.withColumn(
                        colname,
                        F.when(F.col(colname).isNull(), F.lit("null"))
                        .otherwise(F.col(colname).cast("string")),
                    )
                elif "boolean" in dtype:
                    df = df.withColumn(
                        colname,
                        F.when(F.col(colname).isNull(), F.lit("null"))
                        .otherwise(F.when(F.col(colname) == True, F.lit("true")).otherwise(F.lit("false"))),
                    )
                else:
                    df = df.withColumn(
                        colname,
                        F.when(F.col(colname).isNull(), F.lit("null"))
                        .otherwise(F.trim(F.lower(F.col(colname).cast("string")))),
                    )
            except Exception as exc:
                logger.warning(f"Skipped normalization for {colname} due to: {exc}")
        logger.info("Normalization finished")
        return df

# ---------------------------
# Validation utilities
# ---------------------------

class Validator:
    """Performs schema, row-count, column checksum, and row checksum validations."""
    def __init__(self, source_df: DataFrame, target_df: DataFrame, key_columns: List[str]):
        self.src = source_df
        self.tgt = target_df
        self.keys = key_columns

    def schema_check(self, ignore: List[str]) -> Dict[str, Any]:
        logger.info("Performing schema validation")
        s_cols = sorted([c for c in self.src.columns if c not in ignore])
        t_cols = sorted([c for c in self.tgt.columns if c not in ignore])
        diff = list(set(s_cols).symmetric_difference(set(t_cols)))
        result = {
            "result": "passed" if not diff else "failed",
            "source_columns": s_cols,
            "target_columns": t_cols,
            "difference": diff,
        }
        logger.info(f"Schema check: {result['result']} (diff count={len(diff)})")
        return result

    def rowcount_check(self) -> Dict[str, Any]:
        logger.info("Performing row count validation")
        s_count = self.src.count()
        t_count = self.tgt.count()
        diff = abs(s_count - t_count)
        res = {
            "result": "passed" if s_count == t_count else "failed",
            "source_row_count": s_count,
            "target_row_count": t_count,
            "difference": diff,
        }
        logger.info(f"Row count: source={s_count}, target={t_count}, result={res['result']}")
        return res

    def column_checksums(self, columns: List[str]) -> Dict[str, Any]:
        logger.info("Computing column-level checksums")
        def checksum_for_column(df: DataFrame, column: str) -> Optional[str]:
            # Fill nulls, collect ordered list, md5 of concatenated values
            agg = (
                df.select(column)
                .na.fill("null")
                .orderBy(column)
                .agg(F.md5(F.concat_ws("", F.collect_list(F.col(column)))).alias("chk"))
            )
            chk_val = agg.collect()[0]["chk"]
            return chk_val

        src_map: Dict[str, Optional[str]] = {}
        tgt_map: Dict[str, Optional[str]] = {}
        for c in columns:
            src_map[c] = checksum_for_column(self.src, c)
            tgt_map[c] = checksum_for_column(self.tgt, c)

        mismatches = [c for c in columns if src_map.get(c) != tgt_map.get(c)]
        logger.info(f"Column checksum mismatches: {len(mismatches)}")
        return {
            "result": "passed" if not mismatches else "failed",
            "source": src_map,
            "target": tgt_map,
            "mismatched_columns": mismatches,
        }

    def row_checksums(self, columns: List[str], check_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        logger.info("Computing row-level checksums and diffs")
        cols_to_use = check_columns if check_columns else columns

        src_rows = self.src.select(*columns).withColumn("row_checksum", F.md5(F.concat_ws("|", *[F.col(c) for c in cols_to_use])))
        tgt_rows = self.tgt.select(*columns).withColumn("row_checksum", F.md5(F.concat_ws("|", *[F.col(c) for c in cols_to_use])))

        # log sample checksums (up to 100)
        sample_src = src_rows.select("row_checksum").limit(100).collect()
        sample_tgt = tgt_rows.select("row_checksum").limit(100).collect()
        logger.info(f"Sample source checksums: {len(sample_src)}; Sample target checksums: {len(sample_tgt)}")

        # rows present only in source
        src_only = src_rows.join(tgt_rows.select(*columns, "row_checksum"), on=columns, how="left_anti")
        # rows present only in target
        tgt_only = tgt_rows.join(src_rows.select(*columns, "row_checksum"), on=columns, how="left_anti")

        def _to_key_tuple(r):
            return tuple(r[c] for c in columns)

        src_dict = {
            _to_key_tuple(r): {"record": {c: r[c] for c in cols_to_use}, "checksum": r["row_checksum"]}
            for r in src_only.collect()
        }
        tgt_dict = {
            _to_key_tuple(r): {"record": {c: r[c] for c in cols_to_use}, "checksum": r["row_checksum"]}
            for r in tgt_only.collect()
        }

        all_keys = sorted(set(src_dict.keys()).union(set(tgt_dict.keys())))
        diffs = []
        for k in all_keys:
            pair: Dict[str, Any] = {}
            if k in src_dict:
                pair["source_row"] = src_dict[k]
            if k in tgt_dict:
                pair["target_row"] = tgt_dict[k]
            diffs.append(pair)

        mismatch_count = (src_only.count() + tgt_only.count()) // 2
        total = self.src.count()
        mismatch_percent = round((mismatch_count / total) * 100, 2) if total else 0.0

        logger.info(f"Row-level mismatches: {mismatch_count} of {total} rows ({mismatch_percent}%)")
        # logging full diffs may be verbose; still include for parity with original behavior
        logger.info(f"Full not-matching records (count={len(diffs)}):")
        for rec in diffs:
            logger.info(rec)

        return {
            "result": "passed" if mismatch_count == 0 else "failed",
            "row_count": total,
            "mismatch_count": mismatch_count,
            "mismatch_percent": mismatch_percent,
            "not_matching_records": diffs,
        }

# ---------------------------
# Reporter: summary writer
# ---------------------------

class SummaryReporter:
    """Compose a tidy summary DataFrame and write CSV & JSON to S3."""
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def emit(self, outcome: Dict[str, Any], src_table_ref: str, tgt_table_ref: str, s3_prefix: str) -> None:
        rows = []

        # schema
        s = outcome["schema_validation"]
        schema_val_str = f"source_columns: {s['source_columns']}, target_columns: {s['target_columns']}"
        schema_remarks = "Mismatched columns: " + ", ".join(s["difference"]) if s["result"] == "failed" else ""
        rows.append(Row(validation_report="schema_validation", source_table=src_table_ref, target_table=tgt_table_ref, result=s["result"], value=schema_val_str, remarks=schema_remarks))

        # row_count
        r = outcome["row_count"]
        row_val = f"source_table_count: {r['source_row_count']}, target_table_count: {r['target_row_count']}"
        row_remarks = f"Difference: {r['difference']}" if r["result"] == "failed" else ""
        rows.append(Row(validation_report="row_count_validation", source_table=src_table_ref, target_table=tgt_table_ref, result=r["result"], value=row_val, remarks=row_remarks))

        # row checksum
        rc = outcome["row_checksum"]
        rc_val = f"row checksums validated: {rc['row_count']}"
        rc_remarks = f"Mismatched checksums: {rc['mismatch_count']} rows ({rc['mismatch_percent']}%). Records: {rc['not_matching_records']}" if rc["result"] == "failed" else ""
        rows.append(Row(validation_report="row_checksum_validation", source_table=src_table_ref, target_table=tgt_table_ref, result=rc["result"], value=rc_val, remarks=rc_remarks))

        # column checksum
        cc = outcome["column_checksum"]
        src_ck_list = [f"{k}: {v}" for k, v in cc["source"].items()]
        tgt_ck_list = [f"{k}: {v}" for k, v in cc["target"].items()]
        cc_val = f"source_column_checksum: [{', '.join(src_ck_list)}], target_column_checksum: [{', '.join(tgt_ck_list)}]"
        cc_remarks = "Mismatched columns: " + ", ".join(cc["mismatched_columns"]) if cc["result"] == "failed" else "Passed"
        rows.append(Row(validation_report="column_checksum_validation", source_table=src_table_ref, target_table=tgt_table_ref, result=cc["result"], value=cc_val, remarks=cc_remarks))

        summary_df = self.spark.createDataFrame(rows)
        # write to s3
        csv_path = s3_prefix.rstrip("/") + "/csv/"
        json_path = s3_prefix.rstrip("/") + "/json/"

        summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)
        summary_df.coalesce(1).write.mode("overwrite").json(json_path)

        logger.info(f"Summary CSV written to {csv_path}")
        logger.info(f"Summary JSON written to {json_path}")

# ---------------------------
# Main orchestration
# ---------------------------

def main():
    # Build source & target
    src_loader = SourceLoader(GLUE_CONN, SRC_SCHEMA, SRC_TABLE, PUSHDOWN_CONDITION, INLINE_QUERY)
    tgt_loader = TargetLoader(TGT_DB, TGT_TABLE, PUSHDOWN_CONDITION, INLINE_QUERY)

    src_df = src_loader.read()
    tgt_df = tgt_loader.read()

    # Determine columns to ignore (present in target but not in source)
    ignore_cols = list(set(tgt_df.columns) - set(src_df.columns))
    logger.info(f"Ignored columns (target-only): {ignore_cols}")

    # Normalize both DataFrames
    normalizer = DataFrameNormalizer(ignore_cols)
    src_norm = normalizer.normalize(src_df)
    tgt_norm = normalizer.normalize(tgt_df)

    # Common columns to evaluate
    common_cols = [c for c in src_norm.columns if c not in ignore_cols]
    logger.info(f"Common columns used for checks: {common_cols}")

    # Run validations
    validator = Validator(src_norm, tgt_norm, common_cols)
    schema_result = validator.schema_check(ignore_cols)
    rowcount_result = validator.rowcount_check()
    colchk_result = validator.column_checksums(common_cols)
    # For row-level comparison, pass the column list to check. If column_checksums found mismatched columns,
    # we pass mismatched columns to row-level's check_columns argument (to replicate original behavior).
    mismatched_cols = colchk_result.get("mismatched_columns", [])
    rowchk_result = validator.row_checksums(common_cols, mismatched_cols if mismatched_cols else None)

    full_outcome = {
        "schema_validation": schema_result,
        "row_count": rowcount_result,
        "column_checksum": colchk_result,
        "row_checksum": rowchk_result,
    }

    logger.info("Validation finished — summary:")
    logger.info(json.dumps(full_outcome, indent=2, default=str))

    # Write report to S3
    s3_out = f"s3://archival-io-227/validation_report/{TGT_DB}/{TGT_TABLE}/validation_summary/"
    reporter = SummaryReporter(spark)
    reporter.emit(full_outcome, f"{SRC_SCHEMA}.{SRC_TABLE}", f"{TGT_DB}.{TGT_TABLE}", s3_out)

    # commit Glue job
    job.commit()
    logger.info("Job committed and completed")

if __name__ == "__main__":
    main()
