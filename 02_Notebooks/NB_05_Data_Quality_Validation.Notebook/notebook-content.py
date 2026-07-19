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

## Data Quality Validation Notebook
Runs cross-table referential integrity checks and threshold-based gating
across the full Gold layer after `NB_04_Gold_Load` completes, and writes a
single consolidated result to `dbo.dq_validation_summary` which feeds the
DQ Dashboard (Module 9/11). If any check breaches its configured threshold,
the notebook raises so the ADF pipeline's failure branch (email alert to
Data Engineering On-Call) triggers before Power BI refresh runs on bad data.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL
batch_id            = "20260718_001"
pipeline_name       = "PL_UKCC_ETL_PIPELINE"
null_rate_threshold = 0.02     # fail if >2% nulls in a required column
orphan_fk_threshold = 0.001    # fail if >0.1% of fact rows have an unresolved FK

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
log_event(pipeline_name, "NB_05_Data_Quality_Validation", "VALIDATION", "GOLD_LAYER", batch_id, "STARTED", start_time=start_time)

validation_results = []

def check_referential_integrity(fact_table, fk_col, dim_table, sk_col):
    fact_df = spark.table(fact_table)
    total = fact_df.count()
    orphans = fact_df.filter(F.col(fk_col).isNull()).count()
    rate = orphans / total if total else 0.0
    passed = rate <= orphan_fk_threshold
    validation_results.append({
        "check_type": "REFERENTIAL_INTEGRITY", "fact_table": fact_table, "fk_column": fk_col,
        "dim_table": dim_table, "total_rows": total, "orphan_rows": orphans,
        "orphan_rate": rate, "threshold": orphan_fk_threshold, "passed": passed,
    })
    return passed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

checks = [
    ("dbo.FactTransaction", "account_sk", "dbo.DimAccount", "account_sk"),
    ("dbo.FactTransaction", "card_sk",    "dbo.DimCard",    "card_sk"),
    ("dbo.FactPayment",     "account_sk", "dbo.DimAccount", "account_sk"),
    ("dbo.FactStatement",   "account_sk", "dbo.DimAccount", "account_sk"),
    ("dbo.FactFraud",       "customer_sk","dbo.DimCustomer","customer_sk"),
    ("dbo.FactCollections", "account_sk", "dbo.DimAccount", "account_sk"),
]

all_passed = True
for fact_table, fk_col, dim_table, sk_col in checks:
    ok = check_referential_integrity(fact_table, fk_col, dim_table, sk_col)
    all_passed = all_passed and ok

for r in validation_results:
    print(f"[{'PASS' if r['passed'] else 'FAIL'}] {r['fact_table']}.{r['fk_column']} "
          f"orphan_rate={r['orphan_rate']:.4%} (threshold={r['threshold']:.4%})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_df = spark.createDataFrame(validation_results)
(
    summary_df
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("validation_timestamp", F.current_timestamp())
    .write.format("delta").mode("append")
    .saveAsTable("dbo.dq_validation_summary")
)

end_time = dt.datetime.utcnow()
if all_passed:
    log_event(pipeline_name, "NB_05_Data_Quality_Validation", "VALIDATION", "GOLD_LAYER", batch_id, "SUCCESS",
              start_time=start_time, end_time=end_time)
    mssparkutils.notebook.exit(json.dumps({"status": "SUCCESS", "all_checks_passed": True}))
else:
    log_event(pipeline_name, "NB_05_Data_Quality_Validation", "VALIDATION", "GOLD_LAYER", batch_id, "FAILED",
              start_time=start_time, end_time=end_time, error_message="One or more DQ thresholds breached")
    # Non-zero-signalling exit: ADF's TridentNotebook activity treats this
    # as a failure and routes to the on-failure Office 365 email alert.
    raise Exception("Data Quality validation FAILED - see dbo.dq_validation_summary for details. Power BI refresh blocked.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
