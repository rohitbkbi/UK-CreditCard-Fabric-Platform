-- ============================================================================
-- Gold Layer DDL Reference (Fabric Warehouse / Lakehouse SQL Endpoint)
-- These statements document the target Gold schema. In the running platform
-- the tables are CREATED by NB_04_Gold_Load.Notebook (Delta/PySpark), which
-- is the single source of truth; this file exists for architects/reviewers
-- and for anyone wiring an external BI tool via the SQL Analytics Endpoint.
-- ============================================================================

CREATE TABLE dbo.DimDate (
    date_sk               INT         NOT NULL,
    calendar_date         DATE        NOT NULL,
    year                  INT, quarter INT, month INT,
    month_name            VARCHAR(20),
    day_of_month          INT, day_of_week INT,
    day_name              VARCHAR(20),
    week_of_year          INT,
    is_weekend            BIT,
    fca_reporting_quarter VARCHAR(10),
    CONSTRAINT PK_DimDate PRIMARY KEY NONCLUSTERED (date_sk) NOT ENFORCED
);

CREATE TABLE dbo.DimCustomer (
    customer_sk           BIGINT NOT NULL,
    customer_id           VARCHAR(20) NOT NULL,
    first_name VARCHAR(100), last_name VARCHAR(100),
    customer_segment      VARCHAR(20),
    kyc_status            VARCHAR(20),
    customer_status       VARCHAR(20),
    customer_since_date   DATE,
    effective_start_date  DATETIME2,
    effective_end_date    DATETIME2,
    is_current_flag       BIT,
    CONSTRAINT PK_DimCustomer PRIMARY KEY NONCLUSTERED (customer_sk) NOT ENFORCED
);

CREATE TABLE dbo.DimAccount (
    account_sk            BIGINT NOT NULL,
    account_id             VARCHAR(20) NOT NULL,
    customer_id             VARCHAR(20),
    product_id              VARCHAR(20),
    account_status VARCHAR(20), account_type VARCHAR(20), currency_code VARCHAR(3),
    account_open_date DATE,
    effective_start_date DATETIME2, effective_end_date DATETIME2, is_current_flag BIT,
    CONSTRAINT PK_DimAccount PRIMARY KEY NONCLUSTERED (account_sk) NOT ENFORCED
);

CREATE TABLE dbo.FactTransaction (
    transaction_id        VARCHAR(20) NOT NULL,
    account_sk BIGINT, card_sk BIGINT, merchant_sk BIGINT, date_sk INT,
    transaction_amount     DECIMAL(18,2),
    transaction_currency   VARCHAR(3),
    transaction_type       VARCHAR(20),
    transaction_status     VARCHAR(20),
    channel                VARCHAR(20),
    is_international_flag  BIT,
    CONSTRAINT PK_FactTransaction PRIMARY KEY NONCLUSTERED (transaction_id) NOT ENFORCED,
    CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (account_sk) REFERENCES dbo.DimAccount(account_sk) NOT ENFORCED,
    CONSTRAINT FK_FactTransaction_DimDate FOREIGN KEY (date_sk) REFERENCES dbo.DimDate(date_sk) NOT ENFORCED
);

-- FactPayment, FactStatement, FactFraud, FactCollections, FactRevenue,
-- DimCard, DimProduct, DimMerchant, DimBranch, DimRisk, DimCurrency,
-- DimCampaign, DimGeography follow the identical pattern: surrogate PK,
-- FK columns to their parent dims, NOT ENFORCED constraints (Fabric
-- Warehouse does not enforce FK/PK physically -- they exist for the
-- optimizer and for BI-tool relationship inference only).
