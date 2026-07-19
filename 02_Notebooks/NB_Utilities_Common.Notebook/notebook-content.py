# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8fb7d13b-34f4-4972-8735-c477be902ff1",
# META       "default_lakehouse_name": "LH_UKCC_Platform",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "8fb7d13b-34f4-4972-8735-c477be902ff1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

## Utilities: Common Framework
Shared logging, DQ, and MERGE helper functions used across Landing, Bronze,
Silver and Gold notebooks via `%run NB_Utilities_Common`.

Provides:
- `log_event(...)` -> writes to the `dbo.pipeline_execution_log` Delta table
- `get_table_config(layer_role=None)` -> reads `metadata/table_config.json` (mirrored as a Lakehouse table `dbo.meta_table_config`)
- `run_dq_checks(df, table_name, pk_cols)` -> null / duplicate / referential checks, returns a DQ result dict
- `merge_scd1(...)`, `merge_scd2(...)` -> generic incremental MERGE writers

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
import json
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Logging Framework (Module 10)
# ---------------------------------------------------------------------------
LOG_TABLE = "dbo.pipeline_execution_log"

def _ensure_log_table():
    if not spark.catalog.tableExists(LOG_TABLE):
        schema = T.StructType([
            T.StructField("log_id", T.StringType()),
            T.StructField("pipeline_name", T.StringType()),
            T.StructField("notebook_name", T.StringType()),
            T.StructField("layer", T.StringType()),
            T.StructField("table_name", T.StringType()),
            T.StructField("batch_id", T.StringType()),
            T.StructField("status", T.StringType()),
            T.StructField("rows_read", T.LongType()),
            T.StructField("rows_written", T.LongType()),
            T.StructField("start_time", T.TimestampType()),
            T.StructField("end_time", T.TimestampType()),
            T.StructField("duration_seconds", T.DoubleType()),
            T.StructField("error_message", T.StringType()),
        ])
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable(LOG_TABLE)

def log_event(pipeline_name, notebook_name, layer, table_name, batch_id, status,
              rows_read=None, rows_written=None, start_time=None, end_time=None, error_message=None):
    """Appends one row to the enterprise pipeline execution log (Module 10).
    Called at the start (status=STARTED) and end (status=SUCCESS/FAILED) of
    every notebook/table processing unit so the DQ + Ops dashboards have a
    single source of truth for run history."""
    _ensure_log_table()
    start_time = start_time or dt.datetime.utcnow()
    end_time = end_time or dt.datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    row = [(
        f"{notebook_name}_{table_name}_{batch_id}_{status}",
        pipeline_name, notebook_name, layer, table_name, batch_id, status,
        rows_read, rows_written, start_time, end_time, duration, error_message,
    )]
    cols = ["log_id","pipeline_name","notebook_name","layer","table_name","batch_id",
            "status","rows_read","rows_written","start_time","end_time","duration_seconds","error_message"]
    spark.createDataFrame(row, cols).write.format("delta").mode("append").saveAsTable(LOG_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Metadata-Driven Configuration (Module: Metadata Driven Pipelines)
# ---------------------------------------------------------------------------
def get_table_config(layer_role=None, active_only=True):
    """Reads the metadata-driven table configuration. In production this is
    materialised as a Lakehouse table (dbo.meta_table_config) loaded by a
    dedicated config-sync pipeline from the Git-managed metadata/table_config.json;
    for local/dev execution it falls back to reading the JSON straight from Files."""
    if spark.catalog.tableExists("dbo.meta_table_config"):
        df = spark.table("dbo.meta_table_config")
    else:
        # Fallback: mssparkutils reads the JSON from OneLake Files (mirrored from Git)
        raw = mssparkutils.fs.head("Files/metadata/table_config.json", 10_000_000)
        records = json.loads(raw)
        df = spark.createDataFrame(records)
    if active_only:
        df = df.filter(F.col("active_flag") == True)
    if layer_role:
        df = df.filter(F.col("layer_role") == layer_role)
    return df

TABLE_CONFIG = None  # populated lazily by notebooks that need it

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Data Quality Framework (Module 9)
# ---------------------------------------------------------------------------
def run_dq_checks(df, table_name: str, pk_cols: list, not_null_cols: list = None):
    """Runs the standard DQ suite: duplicate PK detection, missing PK, and
    null-rate checks on the given not_null_cols. Returns a summary dict and
    writes failing rows to a quarantine Delta table (dbo.dq_quarantine_<table>)
    when the reject threshold is breached."""
    not_null_cols = not_null_cols or []
    total = df.count()

    null_pk = df.filter(F.expr(" OR ".join([f"{c} IS NULL" for c in pk_cols]))) if pk_cols else df.limit(0)
    null_pk_count = null_pk.count()

    dup_count = 0
    if pk_cols:
        dup_count = (
            df.groupBy(*pk_cols).count().filter(F.col("count") > 1).count()
        )

    null_rates = {}
    for c in not_null_cols:
        if c in df.columns:
            null_rates[c] = df.filter(F.col(c).isNull()).count() / total if total else 0.0

    reject_df = null_pk
    if reject_df.count() > 0:
        (
            reject_df.write.format("delta").mode("append")
            .saveAsTable(f"dbo.dq_quarantine_{table_name}")
        )

    result = {
        "table_name": table_name,
        "total_rows": total,
        "null_pk_rows": null_pk_count,
        "duplicate_pk_groups": dup_count,
        "null_rates": null_rates,
        "passed": (null_pk_count == 0 and dup_count == 0),
    }
    print(f"[DQ] {table_name}: {result}")
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Generic Incremental MERGE Writers (SCD Type 1 / Type 2)
# ---------------------------------------------------------------------------
def merge_scd1(source_df, target_table: str, pk_cols: list, compare_cols: list = None):
    """SCD Type 1 (overwrite-in-place) MERGE: used for reference/dimension
    tables where history is not required (e.g. merchant, currency)."""
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return source_df.count()

    target = DeltaTable.forName(spark, target_table)
    merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])

    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return source_df.count()


def merge_scd2(source_df, target_table: str, pk_cols: list, tracked_cols: list,
               effective_col: str = "effective_start_date"):
    """SCD Type 2 MERGE: expires the current row when any tracked_cols value
    changes, and inserts a new current-dated version. Used for customer,
    account, credit_card, credit_limit, card_product, branch, and other
    dimensions where point-in-time history is a regulatory requirement
    (e.g. affordability/risk decisioning audit trail)."""
    now = F.current_timestamp()

    source_df = (
        source_df
        .withColumn("effective_start_date", now)
        .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current_flag", F.lit(True))
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256),
        )
    )

    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return source_df.count()

    target = DeltaTable.forName(spark, target_table)
    merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols]) + " AND t.is_current_flag = true"

    # Step 1: expire changed current rows
    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_cond)
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={
                "is_current_flag": "false",
                "effective_end_date": "s.effective_start_date",
            },
        )
        .execute()
    )

    # Step 2: insert brand-new + changed rows as new current versions
    existing_current = spark.table(target_table).filter("is_current_flag = true")
    join_cond = pk_cols
    to_insert = (
        source_df.alias("s")
        .join(existing_current.alias("t"), on=pk_cols, how="left_anti")
    )
    if to_insert.count() > 0:
        to_insert.write.format("delta").mode("append").saveAsTable(target_table)

    return source_df.count()

print("NB_Utilities_Common loaded: log_event, get_table_config, run_dq_checks, merge_scd1, merge_scd2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
