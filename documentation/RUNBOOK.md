# Operations Runbook

## Daily Pipeline Run
`PL_UKCC_ETL_PIPELINE` runs automatically at 02:00 GMT (see `.schedules`).

**Happy path**: Lookup active tables -> ForEach (Landing -> Bronze ->
Silver, 8 tables in parallel) -> Gold -> DQ Validation -> Power BI refresh.

## If the pipeline fails

1. Check `dbo.pipeline_execution_log` filtered to today's `batch_id`,
   `status = 'FAILED'` to find which notebook/table failed.
2. Check `dbo.dq_quarantine_<table>` if the failure was in Silver — this
   holds the rejected rows and usually points straight at a source data
   issue (missing PK, upstream schema drift).
3. Check `dbo.dq_validation_summary` if the failure was in DQ Validation —
   shows which fact -> dimension referential check breached its threshold.
4. Fix the root cause (source system, config, or code), then **re-run the
   pipeline with the same `batch_id`** — every step is idempotent so this
   is always safe.
5. If Gold/Power BI refresh needs to be re-triggered manually without
   re-running the whole pipeline, run `NB_04_Gold_Load` and
   `NB_05_Data_Quality_Validation` directly with the failed `batch_id`.

## Adding a new source table

1. Add a row to `metadata/table_config.json` (table_name, source_system,
   layer_role, primary_key, scd_type, natural_keys) and commit.
2. Confirm the ADF Copy Activity (or connector) lands the raw extract at
   `Files/raw/<source_system>/<table_name>/`.
3. No notebook code changes needed — `NB_01`/`NB_02`/`NB_03` are fully
   metadata-driven and will pick up the new table on the next pipeline run
   once `dbo.meta_table_config` is refreshed from the JSON.
4. If the table is a new Gold dimension/fact, extend `NB_04_Gold_Load` with
   the new `build_scd1_dimension`/`build_scd2_dimension` call or fact join,
   and add the table + relationships to the semantic model.

## Data Quality Quarantine Review (weekly)

Business/Data Governance reviews `dbo.dq_quarantine_*` tables weekly;
persistent quarantine volume from one source system is escalated to that
system's owning team as a source-data-quality defect, not silently
patched downstream.

## Delta Maintenance (weekly, separate maintenance pipeline)

`OPTIMIZE ... ZORDER BY (<natural_key>)` + `VACUUM ... RETAIN 168 HOURS` on
Bronze and Silver tables, prioritising the highest-volume tables first
(`bronze_transaction`, `silver_transaction`, `bronze_digital_banking_activity`).
