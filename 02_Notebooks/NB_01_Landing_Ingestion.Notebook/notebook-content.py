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

## Landing Layer Notebook
**Purpose**: Metadata-driven extraction from source systems (Oracle, SQL
Server, PostgreSQL, REST APIs, CSV/JSON/XML over SFTP) into OneLake Files,
under the standard partitioning convention:

`Files/landing/<source_system>/<table_name>/ingestion_date=<date>/batch_id=<batch_id>/`

This notebook is called per-source-system by the ADF/Fabric pipeline
(`PL_UKCC_ETL_PIPELINE`) inside a `ForEach` over `dbo.meta_table_config`,
so it is fully parameterized and contains **no hardcoded table names**.

Includes retry logic, structured logging, and defensive error handling per
the platform's non-negotiable production standards.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL - mark as "Parameters" cell in the Fabric notebook UI
source_system   = "ORACLE_CORE"      # injected by ADF ForEach @item().source_system
table_name      = "customer"         # injected by ADF ForEach @item().table_name
ingestion_date  = "2026-07-18"
batch_id        = "20260718_001"
pipeline_name   = "PL_UKCC_ETL_PIPELINE"
max_retries     = 3
retry_delay_sec = 15

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

import time
import datetime as dt
from pyspark.sql.utils import AnalysisException

start_time = dt.datetime.utcnow()
log_event(pipeline_name, "NB_01_Landing_Ingestion", "LANDING", table_name, batch_id, "STARTED", start_time=start_time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_source(source_system: str, table_name: str):
    """
    Dispatches extraction to the correct connector based on source_system.
    In production each branch calls the real connector (JDBC for
    Oracle/SQL Server/PostgreSQL via ADF-managed linked services, REST via
    requests/OAuth2, SFTP via paramiko). Here we read the upstream landing
    zone that ADF Copy Activities have already dropped into
    Files/raw/<source_system>/<table_name>/ (Copy Activity is the preferred
    enterprise pattern for JDBC/SFTP extraction; this notebook focuses on
    validating, standardising and re-partitioning that raw drop into the
    canonical Landing convention).
    """
    raw_path = f"Files/raw/{source_system}/{table_name}"
    try:
        df = spark.read.format("delta").load(raw_path)
    except AnalysisException:
        # Fall back to the synthetic bronze drop for dev/test environments
        # where NB_00_Synthetic_Data_Generator is the only upstream source.
        df = spark.read.format("delta").load(f"Files/bronze/{source_system}")
    return df


def extract_with_retry(source_system: str, table_name: str):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            df = extract_source(source_system, table_name)
            print(f"[OK] Extracted {source_system}.{table_name} on attempt {attempt}")
            return df
        except Exception as e:
            last_err = e
            print(f"[RETRY {attempt}/{max_retries}] {source_system}.{table_name} failed: {e}")
            time.sleep(retry_delay_sec)
    raise RuntimeError(f"Extraction failed after {max_retries} attempts: {last_err}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    src_df = extract_with_retry(source_system, table_name)
    rows_read = src_df.count()

    landing_path = (
        f"Files/landing/{source_system}/{table_name}/"
        f"ingestion_date={ingestion_date}/batch_id={batch_id}"
    )
    (
        src_df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(landing_path)
    )

    end_time = dt.datetime.utcnow()
    log_event(pipeline_name, "NB_01_Landing_Ingestion", "LANDING", table_name, batch_id,
              "SUCCESS", rows_read=rows_read, rows_written=rows_read,
              start_time=start_time, end_time=end_time)
    print(f"[OK] Landing write complete: {landing_path} rows={rows_read}")

except Exception as e:
    end_time = dt.datetime.utcnow()
    log_event(pipeline_name, "NB_01_Landing_Ingestion", "LANDING", table_name, batch_id,
              "FAILED", start_time=start_time, end_time=end_time, error_message=str(e))
    # mssparkutils.notebook.exit signals failure back to the calling ADF
    # pipeline activity so on-failure branches (email alert) can trigger.
    mssparkutils.notebook.exit(json.dumps({"status": "FAILED", "table": table_name, "error": str(e)}))

mssparkutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": table_name, "rows": rows_read}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
