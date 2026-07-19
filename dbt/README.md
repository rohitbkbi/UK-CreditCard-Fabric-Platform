# dbt (optional / not used for the primary pipeline)

The platform's Silver and Gold transforms are implemented natively in
PySpark notebooks (see `02_Notebooks/NB_03_Silver_Transform` and
`NB_04_Gold_Load`) because Microsoft Fabric's Lakehouse + Spark engine is
the primary compute layer and the metadata-driven SCD1/SCD2 MERGE pattern
used here needs the Delta Lake MERGE API directly.

This folder is kept as a placeholder for teams that prefer to express the
Silver -> Gold business logic as dbt models on top of the **Fabric
Warehouse SQL endpoint** (dbt-fabric adapter) instead of / in addition to
the notebooks — e.g. for analysts who are more comfortable in SQL than
PySpark. If adopted:

- `models/staging/` -> thin SELECT * views over `bronze_*` tables (1:1 with source)
- `models/marts/` -> the star schema (`dim_*`, `fact_*`) as dbt models, replacing
  or running alongside `NB_04_Gold_Load`
- Use `dbt-fabric` (https://github.com/Microsoft/dbt-fabric) as the adapter
- Tests: `not_null`, `unique` on every surrogate key; `relationships` tests
  for every fact -> dim FK (mirrors `NB_05_Data_Quality_Validation`)

Not wired into `PL_UKCC_ETL_PIPELINE` by default — the notebooks are the
system of record for this platform.
