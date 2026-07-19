# UK Credit Card Data Platform — Microsoft Fabric

Production-grade, metadata-driven Medallion (Bronze/Silver/Gold) data
platform for a UK credit card banking business, built natively on
Microsoft Fabric (Lakehouse + Notebooks + Data Pipeline + Power BI Direct
Lake), with a full synthetic data generator covering 40 referentially
consistent tables across Customer, Accounts & Cards, Merchant,
Transactions & Payments, Fraud & Risk, Collections, Service/Digital
Activity, and Regulatory Reporting domains.

This repository is a **Fabric Git-integration export** — the exact folder
layout Fabric uses when you connect a workspace to Git (Azure DevOps /
GitHub). Importing it into a new empty Fabric workspace via
**Workspace Settings → Git Integration → Connect** and syncing will
recreate every item (Lakehouse, Notebooks, Pipeline, Semantic Model).

## Folder Structure

```
UK-CreditCard-Fabric-Platform/
├── 01_Lakehouse/
│   └── LH_UKCC_Platform.Lakehouse/     # the single Lakehouse: Bronze/Silver/Gold all live here as managed Delta tables, plus Files/ for Landing
├── 02_Notebooks/
│   ├── NB_00_Synthetic_Data_Generator.Notebook/   # dev/test data seeding (40 tables)
│   ├── NB_Utilities_Common.Notebook/              # shared logging / DQ / MERGE helpers, loaded via %run
│   ├── NB_01_Landing_Ingestion.Notebook/          # source extraction -> OneLake Files (Landing)
│   ├── NB_02_Bronze_Load.Notebook/                # Landing -> managed Bronze Delta tables
│   ├── NB_03_Silver_Transform.Notebook/           # cleansing, SCD1/SCD2 MERGE -> Silver
│   ├── NB_04_Gold_Load.Notebook/                  # star schema build -> Gold
│   ├── NB_05_Data_Quality_Validation.Notebook/    # cross-table referential integrity gate
│   └── NB_06_Orchestrator_Driver.Notebook/         # dev/test convenience runner (chains everything)
├── 03_Pipelines/
│   └── PL_UKCC_ETL_PIPELINE.DataPipeline/         # production orchestration: Lookup -> ForEach(Landing/Bronze/Silver) -> Gold -> DQ -> Power BI refresh, daily schedule + failure alerting
├── 04_Reports/
│   └── UKCC_Analytics.SemanticModel/              # Direct Lake semantic model over Gold: dims, facts, relationships, DAX measures
├── metadata/
│   └── table_config.json                          # the metadata-driven config: every table's source_system, primary_key, scd_type, layer_role — this is what NB_01/02/03 read to avoid hardcoding
├── sql/
│   └── gold_ddl_reference.sql                      # documented DDL for the Gold star schema (Gold tables are actually created by NB_04 in PySpark; this is for architects/BI tooling reference)
├── dbt/
│   └── README.md                                   # placeholder/optional path if a team prefers SQL-based transforms over the notebooks
└── documentation/
    ├── ARCHITECTURE.md                              # end-to-end flow, medallion rationale, security, DR/HA, CI/CD
    ├── DATA_DICTIONARY.md                            # all 40 tables + Gold star schema + glossary
    └── RUNBOOK.md                                    # on-call operational procedures
```

## How the pieces fit together

1. **`NB_00_Synthetic_Data_Generator`** is your dev/test data source — run
   it once to populate `Files/bronze/<source_system>/...` with 40
   referentially-consistent tables (2,000 customers, ~25k transactions,
   etc. — tune the volume parameters at the top of the notebook).
2. **`metadata/table_config.json`** must be loaded into a Lakehouse table
   `dbo.meta_table_config` (a one-time/CI step) — this is what makes
   `NB_01`/`NB_02`/`NB_03` and the pipeline's `ForEach` fully
   metadata-driven instead of having 40 hardcoded notebook copies.
3. **`PL_UKCC_ETL_PIPELINE`** is what actually runs in production: it
   looks up the active table list, fans out Landing→Bronze→Silver per
   table (8-way parallel), then runs Gold once (it joins across many
   Silver tables) and Data Quality Validation, then triggers the Power BI
   semantic model refresh — only if DQ passed.
4. **`UKCC_Analytics.SemanticModel`** uses **Direct Lake** mode, so Power
   BI queries the Gold Delta tables directly with no import/refresh lag
   beyond the explicit refresh trigger at the end of the pipeline.

## What you still need to do to go live

- Point `NB_01_Landing_Ingestion`'s `extract_source()` at your real
  Oracle/SQL Server/PostgreSQL/REST/SFTP connectors (currently reads from
  the synthetic Bronze drop as a dev fallback) — production pattern is
  ADF Copy Activities landing to `Files/raw/<source_system>/<table>/`
  ahead of this notebook, as already assumed in the code.
- Load `metadata/table_config.json` into `dbo.meta_table_config` (a small
  one-cell notebook or a Fabric Dataflow Gen2 — not included, since the
  exact mechanism depends on your CI/CD tooling).
- Replace the placeholder `WorkspaceId` / `SemanticModelId` / webhook URL
  Global Parameters referenced in `PL_UKCC_ETL_PIPELINE`'s Power BI
  refresh and failure-alert activities with your real workspace/model IDs.
- Build out the remaining 14 Power BI report pages (Executive, Customer,
  Fraud, Risk, Collections, Merchant, Operations, Finance, Campaign,
  Rewards, Customer Service, Digital Banking, Delinquency, Portfolio) on
  top of the semantic model — the model already exposes all the
  dims/facts/measures needed; only the visual layer is left as a design
  exercise per your bank's BI standards.
- Configure Row-Level Security roles on the semantic model per
  `documentation/ARCHITECTURE.md` §3.

## Design principles followed throughout

- **Metadata-driven, not hardcoded** — one Landing/Bronze/Silver notebook
  handles all 40 tables via `dbo.meta_table_config`.
- **Idempotent by `batch_id`** — every layer can be safely re-run for the
  same batch without creating duplicates or requiring manual cleanup.
- **SCD2 where regulators need history, SCD1 where they don't** — see
  `ARCHITECTURE.md` for the full rationale per table.
- **Fail closed, not open** — the Data Quality Validation gate blocks the
  Power BI refresh (and alerts on-call) rather than letting a bad batch
  reach business dashboards.
