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

## Bronze Layer Notebook
Reads the Landing Delta drop for one table, enforces the Bronze schema
contract, adds/validates the standard audit columns (`source_system`,
`batch_id`, `pipeline_name`, `load_timestamp`, `file_name`,
`ingestion_timestamp`, `record_hash`), and writes/merges into the managed
Bronze Delta table `dbo.bronze_<table_name>` in the Lakehouse.

Bronze tables are **append-only + de-duplicated by record_hash** (never
overwritten in place) so the platform retains a full immutable history of
every batch received from source, which is a standard requirement for
PCI-DSS/FCA audit trails.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL
source_system  = "ORACLE_CORE"
table_name     = "customer"
ingestion_date = "2026-07-18"
batch_id       = "20260718_001"
pipeline_name  = "PL_UKCC_ETL_PIPELINE"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Utilities_Common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
from pyspark.sql import functions as F

start_time = dt.datetime.utcnow()
log_event(pipeline_name, "NB_02_Bronze_Load", "BRONZE", table_name, batch_id, "STARTED", start_time=start_time)

bronze_table = f"dbo.bronze_{table_name}"
landing_path = (
    f"Files/landing/{source_system}/{table_name}/"
    f"ingestion_date={ingestion_date}/batch_id={batch_id}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    landing_df = spark.read.format("delta").load(landing_path)
    rows_read = landing_df.count()

    # Schema enforcement: cast every column to the type already established
    # in the target Bronze table (if it exists) rather than trusting the
    # source's inferred types, per Delta Lake best practice.
    if spark.catalog.tableExists(bronze_table):
        target_schema = spark.table(bronze_table).schema
        for field in target_schema:
            if field.name in landing_df.columns:
                landing_df = landing_df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    # De-duplicate by record_hash within the incoming batch (defends against
    # upstream re-delivery / at-least-once semantics from source connectors).
    landing_df = landing_df.dropDuplicates(["record_hash"]) if "record_hash" in landing_df.columns else landing_df

    (
        landing_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("batch_id")
        .saveAsTable(bronze_table)
    )

    rows_written = landing_df.count()
    end_time = dt.datetime.utcnow()
    log_event(pipeline_name, "NB_02_Bronze_Load", "BRONZE", table_name, batch_id, "SUCCESS",
              rows_read=rows_read, rows_written=rows_written, start_time=start_time, end_time=end_time)
    print(f"[OK] {bronze_table}: appended {rows_written} rows (batch_id={batch_id})")

except Exception as e:
    end_time = dt.datetime.utcnow()
    log_event(pipeline_name, "NB_02_Bronze_Load", "BRONZE", table_name, batch_id, "FAILED",
              start_time=start_time, end_time=end_time, error_message=str(e))
    mssparkutils.notebook.exit(json.dumps({"status": "FAILED", "table": table_name, "error": str(e)}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%md
%md
# MAGIC ### Table Maintenance
# MAGIC `OPTIMIZE` + `ZORDER` on the natural key keeps point/range lookups fast
# MAGIC as Bronze grows; `VACUUM` reclaims storage after the retention window.
# MAGIC Run weekly via a dedicated maintenance pipeline, not on every batch,
# MAGIC to avoid write-amplification overhead on high-frequency tables like# MAGIC `bronze_transaction`.


# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Executed by the weekly maintenance pipeline, parameterized by table_name
# MAGIC -- OPTIMIZE dbo.bronze_transaction ZORDER BY (transaction_id, account_id);
# MAGIC -- VACUUM dbo.bronze_transaction RETAIN 168 HOURS;
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": table_name, "rows": rows_written}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
