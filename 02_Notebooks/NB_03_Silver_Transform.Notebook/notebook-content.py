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

## Silver Layer Notebook
Metadata-driven cleansing, standardisation and incremental load of one
Bronze table into its Silver counterpart. Behaviour branches on
`scd_type` from `dbo.meta_table_config`:

- **scd_type = 1** (fact/transactional tables, e.g. `transaction`, `payment`,
  `fraud_alert`): cleansed, deduplicated, incrementally **appended** by
  `batch_id` — facts are immutable once posted.
- **scd_type = 2** (slowly-changing dimensions, e.g. `customer`, `account`,
  `credit_card`, `credit_limit`, `card_product`, `branch`): history-tracked
  via `merge_scd2` so point-in-time joins (e.g. "what was the customer's
  risk grade on the date of this transaction") are always answerable —
  a regulatory requirement for FCA/PRA affordability and fraud
  investigations.

Business rules applied uniformly: null handling, string standardisation
(trim/upper where relevant), duplicate removal by primary key, and
referential validation against parent dimensions already loaded to Silver.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL
table_name    = "customer"
batch_id      = "20260718_001"
pipeline_name = "PL_UKCC_ETL_PIPELINE"

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
log_event(pipeline_name, "NB_03_Silver_Transform", "SILVER", table_name, batch_id, "STARTED", start_time=start_time)

cfg_row = get_table_config().filter(F.col("table_name") == table_name).collect()
if not cfg_row:
    raise ValueError(f"No metadata config found for table '{table_name}' in dbo.meta_table_config")
cfg = cfg_row[0].asDict()

bronze_table = cfg["bronze_table"]
silver_table = cfg["silver_table"]
pk_col       = cfg["primary_key"]
scd_type     = cfg["scd_type"]
print(f"Processing {bronze_table} -> {silver_table} | pk={pk_col} | scd_type={scd_type}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cleanse(df):
    """Business-rule cleansing applied to every table (Module 5):
    - Trim/standardise string columns
    - Drop exact-duplicate business rows
    - Drop rows missing the primary key (routed to DQ quarantine separately)
    """
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))

    business_cols = [c for c in df.columns if c not in
                     ("source_system", "batch_id", "pipeline_name", "load_timestamp",
                      "file_name", "ingestion_timestamp", "record_hash")]
    df = df.dropDuplicates(business_cols)
    df = df.filter(F.col(pk_col).isNotNull())
    return df


# Read only the current batch from Bronze (incremental, not full reload)
bronze_df = spark.table(bronze_table).filter(F.col("batch_id") == batch_id)
rows_read = bronze_df.count()

clean_df = cleanse(bronze_df)

dq_result = run_dq_checks(clean_df, table_name, pk_cols=[pk_col])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    business_cols = [c for c in clean_df.columns if c not in
                     ("source_system", "batch_id", "pipeline_name", "load_timestamp",
                      "file_name", "ingestion_timestamp", "record_hash")]
    silver_df = clean_df.select(*business_cols).withColumn("silver_load_timestamp", F.current_timestamp())

    if scd_type == 2:
        tracked_cols = [c for c in business_cols if c != pk_col]
        rows_written = merge_scd2(silver_df, silver_table, pk_cols=[pk_col], tracked_cols=tracked_cols)
    else:
        rows_written = merge_scd1(silver_df, silver_table, pk_cols=[pk_col])

    end_time = dt.datetime.utcnow()
    log_event(pipeline_name, "NB_03_Silver_Transform", "SILVER", table_name, batch_id, "SUCCESS",
              rows_read=rows_read, rows_written=rows_written, start_time=start_time, end_time=end_time)
    print(f"[OK] {silver_table}: {rows_written} rows processed (scd_type={scd_type})")

except Exception as e:
    end_time = dt.datetime.utcnow()
    log_event(pipeline_name, "NB_03_Silver_Transform", "SILVER", table_name, batch_id, "FAILED",
              start_time=start_time, end_time=end_time, error_message=str(e))
    mssparkutils.notebook.exit(json.dumps({"status": "FAILED", "table": table_name, "error": str(e)}))

mssparkutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": table_name, "rows": rows_written, "dq": dq_result["passed"]}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
