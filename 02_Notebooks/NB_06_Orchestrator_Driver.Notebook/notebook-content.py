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

## Orchestrator Driver (Dev/Test Convenience Runner)
Chains Landing -> Bronze -> Silver -> Gold -> Validation for every active
table in `dbo.meta_table_config`, using `mssparkutils.notebook.run` with
per-table parameters. **This notebook is for local/dev/test iteration
only** — production scheduling, retries, and failure alerting are owned by
the Fabric Data Pipeline `PL_UKCC_ETL_PIPELINE` (see `03_Pipelines/`),
which calls the same notebooks with the same parameter contract.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL
batch_id       = "20260718_001"
ingestion_date = "2026-07-18"
pipeline_name  = "PL_UKCC_ETL_PIPELINE_DEV_RUNNER"

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

tables = get_table_config().collect()
print(f"Processing {len(tables)} active tables from meta_table_config")

results = []
for row in tables:
    t = row.asDict()
    table_name, source_system = t["table_name"], t["source_system"]
    print(f"\n{'='*70}\n{table_name} ({source_system})\n{'='*70}")
    try:
        mssparkutils.notebook.run("NB_01_Landing_Ingestion", 600,
            {"source_system": source_system, "table_name": table_name,
             "ingestion_date": ingestion_date, "batch_id": batch_id})
        mssparkutils.notebook.run("NB_02_Bronze_Load", 600,
            {"source_system": source_system, "table_name": table_name,
             "ingestion_date": ingestion_date, "batch_id": batch_id})
        mssparkutils.notebook.run("NB_03_Silver_Transform", 600,
            {"table_name": table_name, "batch_id": batch_id})
        results.append({"table_name": table_name, "status": "SUCCESS"})
    except Exception as e:
        print(f"[FAILED] {table_name}: {e}")
        results.append({"table_name": table_name, "status": "FAILED", "error": str(e)})
        continue

print("\n=== LANDING/BRONZE/SILVER SUMMARY ===")
for r in results:
    print(r)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold + Validation run once, after ALL Silver tables have loaded
# (Gold joins across many Silver tables, so it cannot run per-table).
mssparkutils.notebook.run("NB_04_Gold_Load", 900, {"batch_id": batch_id})
mssparkutils.notebook.run("NB_05_Data_Quality_Validation", 300, {"batch_id": batch_id})
print("Gold load + DQ validation complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
