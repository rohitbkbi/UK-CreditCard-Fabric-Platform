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

# Fabric Notebook: 00_synthetic_data_generator
# ============================================================================
# LAYER      : Bronze (synthetic source simulation)
# PURPOSE    : Generate referentially-consistent synthetic data for the full
#              UK Credit Card Data Platform data model (40 tables) and land
#              it into OneLake Files as Delta, using the path convention:
#
#              Files/bronze/<source_system>/ingestion_date=<date>/
#                    batch_id=<batch_id>/delta/<table_name>/
#
# FORMAT     : Delta only (CSV/Parquet deliberately excluded — see chat).
# RUN ORDER  : Cells must run top-to-bottom; later domains reference ID pools
#              built by earlier domains (referential integrity by design).
# ============================================================================

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 1. Parameters
Exposed as notebook parameters so this can be called from an ADF/Fabric
pipeline with different batch/date values per run (Module 7/8 pattern).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL - mark this cell as "Parameters" cell in Fabric notebook UI
INGESTION_DATE = "2026-07-18"          # str, format YYYY-MM-DD
BATCH_ID       = "20260718_001"        # str
BASE_PATH      = "Files/bronze"        # OneLake Files root for this run
PIPELINE_NAME  = "pl_synthetic_data_generation"
RANDOM_SEED    = 42

# Volume controls - tune per environment (Dev should be small, Perf/Test larger)
NUM_BRANCHES        = 25
NUM_CUSTOMERS        = 2000
NUM_MERCHANTS        = 300
CARDS_PER_ACCOUNT_MAX = 2      # 1 or 2 cards per account
TXNS_PER_CARD_AVG    = 25
FRAUD_ALERT_RATE     = 0.02    # % of transactions that spawn an alert
COMPLAINT_RATE       = 0.05    # % of customers who log a complaint
SERVICE_CALL_RATE    = 0.30    # % of customers who make a service call
DIGITAL_ACTIVITY_PER_CUSTOMER = 8

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 2. Imports & Setup

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
import hashlib
import datetime as dt
from faker import Faker

from pyspark.sql import functions as F
from pyspark.sql import types as T

random.seed(RANDOM_SEED)
fake = Faker("en_GB")
Faker.seed(RANDOM_SEED)

# `spark` is the ambient SparkSession provided by the Fabric notebook runtime.
# No need to construct one manually.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 3. Utility Functions
Shared helpers: ID generation, weighted picks, bronze metadata stamping,
and the Delta writer that implements the required folder convention.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gen_ids(prefix: str, n: int, width: int = 7):
    """Deterministic zero-padded natural keys, e.g. CUST0000001."""
    return [f"{prefix}{str(i).zfill(width)}" for i in range(1, n + 1)]


def pick(pool):
    return random.choice(pool)


def weighted(pool_weights: dict):
    """pool_weights: {value: weight}. Returns one weighted-random value."""
    return random.choices(list(pool_weights.keys()), weights=list(pool_weights.values()), k=1)[0]


def random_date(start: dt.date, end: dt.date):
    """Defensive wrapper: if start > end (can happen with randomized bounds
    such as dob + N years), collapse the range to `end` rather than raising."""
    if start > end:
        start = end
    return fake.date_between(start_date=start, end_date=end)


def random_ts(start: dt.date, end: dt.date):
    d = random_date(start, end)
    return dt.datetime.combine(d, dt.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)))


def add_bronze_metadata(df, table_name: str, source_system: str):
    """
    Stamps the standard Bronze audit columns defined in the platform
    architecture (Module 1 / Module 4):
    source_system, batch_id, pipeline_name, load_timestamp, file_name,
    ingestion_timestamp, record_hash.
    """
    business_cols = df.columns
    df = (
        df
        .withColumn("source_system", F.lit(source_system))
        .withColumn("batch_id", F.lit(BATCH_ID))
        .withColumn("pipeline_name", F.lit(PIPELINE_NAME))
        .withColumn("load_timestamp", F.current_timestamp())
        .withColumn("file_name", F.lit(f"{table_name}_synthetic.delta"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn(
            "record_hash",
            F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_cols]), 256),
        )
    )
    return df


def write_bronze(rows: list, table_name: str, source_system: str, schema: T.StructType = None):
    """
    Converts a list-of-dicts to a Spark DataFrame, stamps Bronze metadata,
    and writes Delta to:
    {BASE_PATH}/{source_system}/ingestion_date={INGESTION_DATE}/batch_id={BATCH_ID}/delta/{table_name}
    """
    if not rows:
        print(f"[SKIP] {table_name}: no rows generated")
        return

    df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)
    df = add_bronze_metadata(df, table_name, source_system)

    target_path = (
        f"{BASE_PATH}/{source_system}/"
        f"ingestion_date={INGESTION_DATE}/batch_id={BATCH_ID}/delta/{table_name}"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )
    print(f"[OK]   {table_name:<28} rows={df.count():<7} source={source_system:<18} -> {target_path}")


# Source-system mapping mirrors the realistic source landscape from the
# Architecture doc (Module 1) so downstream Landing/Bronze notebooks can be
# tested against the same partitioning scheme as real ingestion.
SRC = {
    "core": "ORACLE_CORE",
    "cards": "CARD_MGMT_SQL",
    "network": "CARD_NETWORK_API",
    "marketing": "MARKETING_PG",
    "fraud": "FRAUD_ENGINE_API",
    "collections": "COLLECTIONS_SQL",
    "crm": "CRM_SQL",
    "digital": "DIGITAL_PG",
    "reference": "REFERENCE_SFTP",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 4. Reference & Dimension-Support Tables
`currency`, `geography`, `branch`, `merchant_category`, `card_product`,
`reward_program`, `campaign`, `offer`, `exchange_rate`.
These have no FK dependencies and are generated first.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CURRENCIES = [
    {"currency_code": "GBP", "currency_name": "Pound Sterling", "currency_symbol": "£", "decimal_places": 2, "is_active_flag": True},
    {"currency_code": "EUR", "currency_name": "Euro", "currency_symbol": "€", "decimal_places": 2, "is_active_flag": True},
    {"currency_code": "USD", "currency_name": "US Dollar", "currency_symbol": "$", "decimal_places": 2, "is_active_flag": True},
]
write_bronze(CURRENCIES, "currency", SRC["reference"])
CURRENCY_CODES = [c["currency_code"] for c in CURRENCIES]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

UK_REGIONS = ["London", "South East", "South West", "West Midlands", "East Midlands",
              "North West", "North East", "Yorkshire and the Humber", "East of England",
              "Scotland", "Wales", "Northern Ireland"]

geo_rows = []
for i, region in enumerate(UK_REGIONS, start=1):
    geo_rows.append({
        "geography_id": f"GEO{str(i).zfill(7)}",
        "country_code": "GBR",
        "country_name": "United Kingdom",
        "region": region,
        "county": fake.county(),
        "postcode_prefix": fake.postcode()[:2],
        "timezone": "Europe/London",
    })
write_bronze(geo_rows, "geography", SRC["reference"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRANCH_IDS = gen_ids("BR", NUM_BRANCHES, width=5)
branch_rows = []
for bid in BRANCH_IDS:
    open_date = random_date(dt.date(1985, 1, 1), dt.date(2020, 1, 1))
    status = weighted({"ACTIVE": 90, "CLOSED": 10})
    branch_rows.append({
        "branch_id": bid,
        "branch_name": f"{fake.city()} Branch",
        "branch_type": weighted({"HIGH_STREET": 60, "DIGITAL_HUB": 15, "CORPORATE": 25}),
        "address_line1": fake.street_address(),
        "city": fake.city(),
        "postcode": fake.postcode(),
        "region": pick(UK_REGIONS),
        "opening_date": open_date,
        "closing_date": random_date(open_date, dt.date(2026, 7, 18)) if status == "CLOSED" else None,
        "branch_status": status,
    })
write_bronze(branch_rows, "branch", SRC["reference"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MCC_CATALOG = [
    ("5411", "Grocery Stores/Supermarkets", "RETAIL", False, True),
    ("5812", "Eating Places/Restaurants", "DINING", False, True),
    ("5541", "Service Stations", "FUEL", False, True),
    ("4111", "Local/Suburban Commuter Transport", "TRANSPORT", False, True),
    ("5732", "Electronics Stores", "RETAIL", False, True),
    ("7995", "Betting/Casino Gambling", "GAMBLING", True, False),
    ("6011", "ATM Cash Disbursement", "CASH", False, False),
    ("5967", "Direct Marketing - Inbound Telemarketing", "OTHER", True, False),
    ("5999", "Miscellaneous Retail", "RETAIL", False, True),
    ("4511", "Airlines", "TRAVEL", False, True),
    ("7011", "Hotels/Motels", "TRAVEL", False, True),
    ("5942", "Book Stores", "RETAIL", False, True),
    ("8011", "Doctors/Physicians", "HEALTHCARE", False, False),
    ("5411", "Supermarkets", "RETAIL", False, True),
    ("6051", "Non-FI, Money Orders/Crypto", "CASH", True, False),
]
mcc_rows = [
    {
        "mcc_code": code,
        "mcc_description": desc,
        "mcc_group": grp,
        "is_high_risk_flag": high_risk,
        "reward_eligible_flag": reward_ok,
    }
    for code, desc, grp, high_risk, reward_ok in {(c, d, g, h, r) for c, d, g, h, r in MCC_CATALOG}
]
write_bronze(mcc_rows, "merchant_category", SRC["network"])
MCC_CODES = [r["mcc_code"] for r in mcc_rows]
MCC_REWARD_ELIGIBLE = [r["mcc_code"] for r in mcc_rows if r["reward_eligible_flag"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

REWARD_PROGRAM_IDS = gen_ids("RWD", 4, width=5)
reward_program_rows = [
    {
        "reward_program_id": rid,
        "program_name": name,
        "earn_rate": rate,
        "points_expiry_months": 36,
        "program_status": "ACTIVE",
    }
    for rid, (name, rate) in zip(
        REWARD_PROGRAM_IDS,
        [("Classic Points", 1.0), ("Travel Rewards", 1.5), ("Cashback Plus", 1.0), ("Premium Miles", 2.0)],
    )
]
write_bronze(reward_program_rows, "reward_program", SRC["marketing"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PRODUCT_IDS = gen_ids("PRD", 8, width=5)
product_names = [
    "Classic Credit Card", "Platinum Rewards Card", "Student Card", "Cashback Card",
    "Travel Elite Card", "Balance Transfer Card", "Business Card", "Secured Credit Card",
]
card_product_rows = []
for pid, name, rwd in zip(PRODUCT_IDS, product_names, random.choices(REWARD_PROGRAM_IDS, k=len(PRODUCT_IDS))):
    card_product_rows.append({
        "product_id": pid,
        "product_name": name,
        "product_category": weighted({"STANDARD": 40, "PREMIUM": 25, "STUDENT": 15, "BUSINESS": 10, "SECURED": 10}),
        "card_network": weighted({"VISA": 55, "MASTERCARD": 40, "AMEX": 5}),
        "annual_fee": round(random.choice([0, 0, 0, 24.99, 95.00, 195.00]), 2),
        "purchase_apr": round(random.uniform(18.9, 34.9), 3),
        "cash_advance_apr": round(random.uniform(24.9, 39.9), 3),
        "balance_transfer_apr": round(random.uniform(0.0, 24.9), 3),
        "default_credit_limit": round(random.choice([500, 1000, 1500, 3000, 5000]), 2),
        "reward_program_id": rwd,
        "product_launch_date": random_date(dt.date(2010, 1, 1), dt.date(2024, 1, 1)),
        "product_status": "ACTIVE",
        "eligibility_min_income": round(random.choice([0, 12000, 18000, 25000, 40000]), 2),
        "eligibility_min_credit_score": random.choice([500, 560, 620, 680, 720]),
    })
write_bronze(card_product_rows, "card_product", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CAMPAIGN_IDS = gen_ids("CMP", 20, width=6)
campaign_rows = []
for cid in CAMPAIGN_IDS:
    start = random_date(dt.date(2024, 1, 1), dt.date(2026, 6, 1))
    campaign_rows.append({
        "campaign_id": cid,
        "campaign_name": f"{fake.bs().title()} Campaign",
        "campaign_type": weighted({"ACQUISITION": 40, "RETENTION": 30, "WINBACK": 15, "CROSS_SELL": 15}),
        "channel": weighted({"EMAIL": 40, "SMS": 20, "APP_PUSH": 25, "DIRECT_MAIL": 15}),
        "start_date": start,
        "end_date": start + dt.timedelta(days=random.randint(14, 90)),
        "target_segment": weighted({"MASS": 40, "AFFLUENT": 25, "YOUNG_PROFESSIONAL": 20, "STUDENT": 15}),
        "budget_amount": round(random.uniform(5000, 250000), 2),
        "campaign_status": weighted({"COMPLETED": 60, "ACTIVE": 25, "PLANNED": 15}),
    })
write_bronze(campaign_rows, "campaign", SRC["marketing"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

OFFER_IDS = gen_ids("OFR", 30, width=6)
offer_rows = []
for oid in OFFER_IDS:
    start = random_date(dt.date(2024, 1, 1), dt.date(2026, 6, 1))
    offer_type = weighted({"CASHBACK": 40, "DISCOUNT": 35, "POINTS_MULTIPLIER": 25})
    offer_rows.append({
        "offer_id": oid,
        "offer_name": f"{random.randint(5,20)}% back at {fake.company()}",
        "offer_type": offer_type,
        "discount_percent": round(random.uniform(5, 25), 2) if offer_type == "DISCOUNT" else None,
        "cashback_amount": round(random.uniform(2, 50), 2) if offer_type == "CASHBACK" else None,
        "offer_start_date": start,
        "offer_end_date": start + dt.timedelta(days=random.randint(7, 60)),
        "mcc_code": pick(MCC_CODES),
        "offer_status": weighted({"EXPIRED": 55, "ACTIVE": 30, "SCHEDULED": 15}),
    })
write_bronze(offer_rows, "offer", SRC["marketing"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exch_rows = []
rate_id = 1
for d_offset in range(0, 30):
    rate_date = dt.date(2026, 7, 18) - dt.timedelta(days=d_offset)
    for to_ccy, base in [("EUR", 1.17), ("USD", 1.27)]:
        exch_rows.append({
            "exchange_rate_id": f"FX{str(rate_id).zfill(7)}",
            "from_currency": "GBP",
            "to_currency": to_ccy,
            "rate_date": rate_date,
            "exchange_rate": round(base + random.uniform(-0.01, 0.01), 6),
            "rate_source": "ECB_REFERENCE",
        })
        rate_id += 1
write_bronze(exch_rows, "exchange_rate", SRC["reference"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 5. Customer Domain
`customer`, `customer_address`, `customer_employment`, `customer_income`,
`customer_risk_profile`. Establishes the `CUSTOMER_IDS` pool used by
every downstream domain.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CUSTOMER_IDS = gen_ids("CUST", NUM_CUSTOMERS)

customer_rows = []
for cid in CUSTOMER_IDS:
    dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
    earliest_since = max(dt.date(2005, 1, 1), dob + dt.timedelta(days=18 * 365))
    latest_since = dt.date(2026, 6, 1)
    since = random_date(earliest_since, latest_since) if earliest_since < latest_since else latest_since
    status = weighted({"ACTIVE": 92, "DORMANT": 6, "CLOSED": 2})
    deceased = random.random() < 0.003
    customer_rows.append({
        "customer_id": cid,
        "title": pick(["Mr", "Mrs", "Miss", "Ms", "Dr"]),
        "first_name": fake.first_name(),
        "middle_name": fake.first_name() if random.random() < 0.2 else None,
        "last_name": fake.last_name(),
        "date_of_birth": dob,
        "gender": pick(["MALE", "FEMALE", "UNDISCLOSED"]),
        "nationality": weighted({"GBR": 85, "IRL": 5, "POL": 3, "IND": 3, "OTHER": 4}),
        "national_insurance_no": fake.bothify("??######?").upper(),
        "email": fake.email(),
        "mobile_phone": fake.phone_number(),
        "home_phone": fake.phone_number() if random.random() < 0.4 else None,
        "marital_status": weighted({"SINGLE": 40, "MARRIED": 40, "DIVORCED": 12, "WIDOWED": 8}),
        "customer_segment": weighted({"MASS": 55, "AFFLUENT": 20, "PREMIUM": 15, "STUDENT": 10}),
        "kyc_status": weighted({"VERIFIED": 92, "PENDING": 5, "REJECTED": 3}),
        "kyc_completed_date": random_date(since, dt.date(2026, 7, 18)),
        "customer_since_date": since,
        "preferred_language": "EN",
        "preferred_contact_channel": weighted({"EMAIL": 45, "SMS": 25, "APP_PUSH": 25, "POST": 5}),
        "marketing_consent_flag": random.random() < 0.65,
        "customer_status": "DECEASED" if deceased else status,
        "deceased_flag": deceased,
        "deceased_date": random_date(since, dt.date(2026, 7, 18)) if deceased else None,
        "pep_flag": random.random() < 0.002,
        "sanctions_screen_status": weighted({"CLEAR": 98, "REVIEW": 2}),
    })
write_bronze(customer_rows, "customer", SRC["core"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

address_rows = []
addr_seq = 1
for cid in CUSTOMER_IDS:
    n_addr = weighted({1: 80, 2: 18, 3: 2})
    for k in range(n_addr):
        move_in = random_date(dt.date(2005, 1, 1), dt.date(2026, 5, 1))
        address_rows.append({
            "address_id": f"ADDR{str(addr_seq).zfill(8)}",
            "customer_id": cid,
            "address_type": "CURRENT" if k == 0 else "PREVIOUS",
            "address_line1": fake.street_address(),
            "address_line2": fake.secondary_address() if random.random() < 0.2 else None,
            "city": fake.city(),
            "county": fake.county(),
            "postcode": fake.postcode(),
            "country_code": "GBR",
            "residency_status": weighted({"OWNER": 35, "TENANT": 45, "LIVING_WITH_FAMILY": 20}),
            "move_in_date": move_in,
            "move_out_date": None if k == 0 else random_date(move_in, dt.date(2026, 5, 1)),
            "is_primary": k == 0,
            "geocode_lat": round(random.uniform(50.0, 58.5), 6),
            "geocode_long": round(random.uniform(-6.0, 1.7), 6),
        })
        addr_seq += 1
write_bronze(address_rows, "customer_address", SRC["core"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employment_rows = []
for i, cid in enumerate(CUSTOMER_IDS, start=1):
    start = random_date(dt.date(2000, 1, 1), dt.date(2025, 1, 1))
    emp_status = weighted({"EMPLOYED": 65, "SELF_EMPLOYED": 12, "RETIRED": 12, "UNEMPLOYED": 6, "STUDENT": 5})
    employment_rows.append({
        "employment_id": f"EMP{str(i).zfill(8)}",
        "customer_id": cid,
        "employer_name": fake.company() if emp_status in ("EMPLOYED", "SELF_EMPLOYED") else None,
        "employment_type": weighted({"FULL_TIME": 70, "PART_TIME": 20, "CONTRACT": 10}),
        "occupation": fake.job(),
        "industry_sector": weighted({
            "FINANCIAL_SERVICES": 15, "RETAIL": 15, "HEALTHCARE": 15, "TECHNOLOGY": 15,
            "EDUCATION": 10, "MANUFACTURING": 10, "PUBLIC_SECTOR": 10, "OTHER": 10,
        }),
        "employment_start_date": start,
        "employment_end_date": None,
        "years_in_employment": round((dt.date(2026, 7, 18) - start).days / 365.25, 1),
        "employment_status": emp_status,
    })
write_bronze(employment_rows, "customer_employment", SRC["core"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

income_rows = []
for i, cid in enumerate(CUSTOMER_IDS, start=1):
    gross = round(random.uniform(12000, 120000), 2)
    income_rows.append({
        "income_id": f"INC{str(i).zfill(8)}",
        "customer_id": cid,
        "income_type": weighted({"SALARY": 70, "SELF_EMPLOYED": 12, "PENSION": 12, "BENEFITS": 6}),
        "gross_annual_income": gross,
        "net_monthly_income": round(gross * 0.72 / 12, 2),
        "income_currency": "GBP",
        "income_verified_flag": random.random() < 0.85,
        "income_verification_method": weighted({"PAYSLIP": 40, "OPEN_BANKING": 35, "BUREAU_ESTIMATE": 15, "SELF_DECLARED": 10}),
        "income_effective_date": random_date(dt.date(2024, 1, 1), dt.date(2026, 6, 1)),
    })
write_bronze(income_rows, "customer_income", SRC["core"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

risk_rows = []
for i, cid in enumerate(CUSTOMER_IDS, start=1):
    score = random.randint(300, 999)
    grade = "A" if score > 800 else "B" if score > 650 else "C" if score > 500 else "D"
    vulnerable = random.random() < 0.06
    risk_rows.append({
        "risk_profile_id": f"RISK{str(i).zfill(8)}",
        "customer_id": cid,
        "internal_credit_score": score,
        "risk_grade": grade,
        "probability_of_default": round(max(0.0001, (999 - score) / 999 * random.uniform(0.05, 0.25)), 6),
        "affordability_score": random.randint(1, 100),
        "behavioural_score": random.randint(1, 999),
        "risk_assessment_date": random_date(dt.date(2025, 1, 1), dt.date(2026, 7, 1)),
        "risk_model_version": weighted({"v3.2": 60, "v3.1": 30, "v2.9": 10}),
        "watchlist_flag": random.random() < 0.01,
        "vulnerable_customer_flag": vulnerable,
        "vulnerability_reason": pick(["FINANCIAL_DIFFICULTY", "HEALTH", "BEREAVEMENT", "LOW_CAPABILITY"]) if vulnerable else None,
    })
write_bronze(risk_rows, "customer_risk_profile", SRC["core"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 6. Accounts & Cards Domain
`account`, `credit_card`, `credit_limit`. Builds `ACCOUNT_IDS` and
`CARD_IDS` pools plus an `account_to_card` map used downstream.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

account_rows = []
ACCOUNT_IDS = []
account_customer_map = {}
acc_seq = 1
for cid in CUSTOMER_IDS:
    n_acc = weighted({1: 80, 2: 17, 3: 3})
    for _ in range(n_acc):
        aid = f"ACC{str(acc_seq).zfill(8)}"
        ACCOUNT_IDS.append(aid)
        account_customer_map[aid] = cid
        open_date = random_date(dt.date(2010, 1, 1), dt.date(2026, 5, 1))
        status = weighted({"ACTIVE": 85, "DORMANT": 8, "CLOSED": 7})
        account_rows.append({
            "account_id": aid,
            "customer_id": cid,
            "product_id": pick(PRODUCT_IDS),
            "account_number_masked": f"****{fake.numerify('####')}",
            "account_open_date": open_date,
            "account_close_date": random_date(open_date, dt.date(2026, 7, 18)) if status == "CLOSED" else None,
            "account_status": status,
            "account_type": weighted({"INDIVIDUAL": 88, "JOINT": 12}),
            "currency_code": "GBP",
            "billing_cycle_day": random.randint(1, 28),
            "joint_account_flag": random.random() < 0.12,
            "branch_id": pick(BRANCH_IDS),
            "relationship_manager_id": f"RM{random.randint(1,50):04d}",
        })
        acc_seq += 1
write_bronze(account_rows, "account", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

card_rows = []
CARD_IDS = []
card_account_map = {}
card_seq = 1
for aid in ACCOUNT_IDS:
    n_cards = random.randint(1, CARDS_PER_ACCOUNT_MAX)
    for k in range(n_cards):
        cardid = f"CARD{str(card_seq).zfill(8)}"
        CARD_IDS.append(cardid)
        card_account_map[cardid] = aid
        issue_date = random_date(dt.date(2015, 1, 1), dt.date(2026, 6, 1))
        expiry = dt.date(issue_date.year + 4, issue_date.month, min(issue_date.day, 28))
        card_rows.append({
            "card_id": cardid,
            "account_id": aid,
            "card_number_masked": f"{random.choice(['4','5'])}{fake.numerify('###')}********{fake.numerify('####')}",
            "card_token": hashlib.sha256(cardid.encode()).hexdigest()[:32],
            "card_type": weighted({"PHYSICAL": 80, "VIRTUAL": 20}),
            "card_network": weighted({"VISA": 55, "MASTERCARD": 40, "AMEX": 5}),
            "card_holder_name": fake.name().upper(),
            "card_issue_date": issue_date,
            "card_expiry_date": expiry,
            "card_status": weighted({"ACTIVE": 82, "BLOCKED": 6, "EXPIRED": 5, "LOST": 4, "STOLEN": 3}),
            "card_form_factor": weighted({"PLASTIC": 70, "DIGITAL_WALLET": 20, "METAL": 10}),
            "is_primary_card": k == 0,
            "activation_date": issue_date + dt.timedelta(days=random.randint(1, 14)),
            "activation_status": "ACTIVATED",
            "contactless_enabled_flag": random.random() < 0.95,
            "digital_wallet_provisioned_flag": random.random() < 0.55,
        })
        card_seq += 1
write_bronze(card_rows, "credit_card", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

limit_rows = []
for i, aid in enumerate(ACCOUNT_IDS, start=1):
    limit_amt = round(random.choice([500, 1000, 1500, 2000, 3000, 5000, 8000, 12000]), 2)
    used = round(limit_amt * random.uniform(0, 0.95), 2)
    limit_rows.append({
        "credit_limit_id": f"CL{str(i).zfill(8)}",
        "account_id": aid,
        "credit_limit_amount": limit_amt,
        "available_credit_amount": round(limit_amt - used, 2),
        "cash_advance_limit": round(limit_amt * 0.3, 2),
        "limit_effective_date": random_date(dt.date(2020, 1, 1), dt.date(2026, 6, 1)),
        "limit_change_reason": weighted({"INITIAL": 40, "CUSTOMER_REQUEST": 25, "RISK_REVIEW": 20, "PROMOTIONAL": 15}),
        "limit_change_type": weighted({"INCREASE": 55, "INITIAL": 40, "DECREASE": 5}),
        "requested_by": weighted({"CUSTOMER": 45, "SYSTEM": 40, "AGENT": 15}),
        "approved_by": weighted({"AUTO_DECISION_ENGINE": 70, "UNDERWRITER": 30}),
    })
write_bronze(limit_rows, "credit_limit", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 7. Merchant Domain

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MERCHANT_IDS = gen_ids("MERCH", NUM_MERCHANTS, width=6)
merchant_rows = []
for mid in MERCHANT_IDS:
    name = fake.company()
    merchant_rows.append({
        "merchant_id": mid,
        "merchant_name": name,
        "merchant_dba_name": name,
        "mcc_code": pick(MCC_CODES),
        "merchant_country": weighted({"GBR": 75, "USA": 8, "IRL": 5, "FRA": 4, "DEU": 4, "OTHER": 4}),
        "merchant_city": fake.city(),
        "acquirer_id": f"ACQ{random.randint(1, 20):04d}",
        "merchant_status": weighted({"ACTIVE": 92, "SUSPENDED": 5, "TERMINATED": 3}),
        "onboarding_date": random_date(dt.date(2010, 1, 1), dt.date(2026, 5, 1)),
        "risk_rating": weighted({"LOW": 70, "MEDIUM": 22, "HIGH": 8}),
    })
write_bronze(merchant_rows, "merchant", SRC["network"])
merchant_mcc_map = {r["merchant_id"]: r["mcc_code"] for r in merchant_rows}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 8. Transactions & Payments Domain
`transaction`, `payment`, `statement`, `interest_charge`, `fee`,
`balance_transfer`, `cash_withdrawal`, `emi_plan`, `loyalty_point`.
This is the highest-volume domain; transactions drive most others.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DECLINE_REASONS = ["INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD", "LIMIT_EXCEEDED", "CARD_EXPIRED", "INVALID_CVV"]

txn_rows = []
TRANSACTION_IDS = []
txn_seq = 1
for cardid in CARD_IDS:
    aid = card_account_map[cardid]
    n_txn = max(1, int(random.gauss(TXNS_PER_CARD_AVG, 8)))
    for _ in range(n_txn):
        tid = f"TXN{str(txn_seq).zfill(9)}"
        TRANSACTION_IDS.append(tid)
        txn_dt = random_ts(dt.date(2025, 7, 1), dt.date(2026, 7, 18))
        amount = round(random.lognormvariate(3.2, 1.1), 2)
        status = weighted({"APPROVED": 92, "DECLINED": 6, "REVERSED": 2})
        intl = random.random() < 0.08
        txn_rows.append({
            "transaction_id": tid,
            "card_id": cardid,
            "account_id": aid,
            "merchant_id": pick(MERCHANT_IDS),
            "transaction_date": txn_dt.date(),
            "transaction_timestamp": txn_dt,
            "posting_date": (txn_dt + dt.timedelta(days=random.randint(0, 2))).date(),
            "transaction_type": weighted({"PURCHASE": 88, "REFUND": 6, "CASH_ADVANCE": 4, "FEE": 2}),
            "transaction_amount": amount,
            "transaction_currency": "GBP" if not intl else pick(["EUR", "USD"]),
            "billing_amount": amount,
            "billing_currency": "GBP",
            "exchange_rate": round(random.uniform(0.85, 1.3), 6) if intl else 1.0,
            "mcc_code": pick(MCC_CODES),
            "authorization_code": fake.bothify("??######").upper(),
            "transaction_status": status,
            "pos_entry_mode": weighted({"CHIP": 45, "CONTACTLESS": 40, "ECOM": 10, "MANUAL": 5}),
            "channel": weighted({"IN_STORE": 60, "ONLINE": 35, "ATM": 5}),
            "is_international_flag": intl,
            "is_contactless_flag": random.random() < 0.4,
            "is_recurring_flag": random.random() < 0.1,
            "decline_reason_code": pick(DECLINE_REASONS) if status == "DECLINED" else None,
        })
        txn_seq += 1
write_bronze(txn_rows, "transaction", SRC["network"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payment_rows = []
for i, aid in enumerate(ACCOUNT_IDS, start=1):
    for _ in range(random.randint(3, 12)):
        payment_rows.append({
            "payment_id": f"PAY{str(i).zfill(8)}{random.randint(0,9)}",
            "account_id": aid,
            "payment_date": random_date(dt.date(2025, 7, 1), dt.date(2026, 7, 18)),
            "payment_amount": round(random.uniform(25, 3000), 2),
            "payment_currency": "GBP",
            "payment_method": weighted({"DIRECT_DEBIT": 50, "BANK_TRANSFER": 25, "DEBIT_CARD": 20, "CHEQUE": 5}),
            "payment_channel": weighted({"AUTOPAY": 45, "APP": 30, "WEB": 15, "BRANCH": 10}),
            "payment_status": weighted({"CLEARED": 90, "PENDING": 6, "RETURNED": 4}),
            "is_minimum_payment_flag": random.random() < 0.3,
            "is_autopay_flag": random.random() < 0.45,
            "returned_payment_flag": random.random() < 0.03,
            "return_reason_code": pick(["INSUFFICIENT_FUNDS", "ACCOUNT_CLOSED", "MANDATE_CANCELLED"]) if random.random() < 0.03 else None,
        })
write_bronze(payment_rows, "payment", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

statement_rows = []
STATEMENT_IDS = []
stmt_seq = 1
for aid in ACCOUNT_IDS:
    for month_offset in range(12):
        period_end = dt.date(2026, 7, 18) - dt.timedelta(days=30 * month_offset)
        period_start = period_end - dt.timedelta(days=30)
        sid = f"STMT{str(stmt_seq).zfill(8)}"
        STATEMENT_IDS.append(sid)
        opening = round(random.uniform(0, 5000), 2)
        purchases = round(random.uniform(0, 2000), 2)
        payments_made = round(random.uniform(0, 2000), 2)
        fees = round(random.choice([0, 0, 0, 12, 25]), 2)
        interest = round(opening * random.uniform(0, 0.025), 2)
        closing = round(opening + purchases - payments_made + fees + interest, 2)
        statement_rows.append({
            "statement_id": sid,
            "account_id": aid,
            "statement_date": period_end,
            "statement_period_start": period_start,
            "statement_period_end": period_end,
            "opening_balance": opening,
            "closing_balance": closing,
            "minimum_payment_due": round(max(25, closing * 0.03), 2),
            "payment_due_date": period_end + dt.timedelta(days=21),
            "total_purchases": purchases,
            "total_payments": payments_made,
            "total_fees": fees,
            "total_interest": interest,
            "statement_status": "GENERATED",
        })
        stmt_seq += 1
write_bronze(statement_rows, "statement", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

interest_rows = []
for i, sid in enumerate([s for s in statement_rows if s["total_interest"] > 0], start=1):
    interest_rows.append({
        "interest_charge_id": f"INT{str(i).zfill(8)}",
        "account_id": sid["account_id"],
        "statement_id": sid["statement_id"],
        "charge_date": sid["statement_date"],
        "interest_type": weighted({"PURCHASE": 70, "CASH_ADVANCE": 20, "BALANCE_TRANSFER": 10}),
        "average_daily_balance": sid["opening_balance"],
        "apr_applied": round(random.uniform(18.9, 34.9), 3),
        "interest_amount": sid["total_interest"],
        "days_in_period": 30,
    })
write_bronze(interest_rows, "interest_charge", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fee_rows = []
for i, sid in enumerate([s for s in statement_rows if s["total_fees"] > 0], start=1):
    waived = random.random() < 0.15
    fee_rows.append({
        "fee_id": f"FEE{str(i).zfill(8)}",
        "account_id": sid["account_id"],
        "fee_type": weighted({"LATE_PAYMENT": 45, "OVER_LIMIT": 25, "ANNUAL": 20, "FOREIGN_TXN": 10}),
        "fee_amount": sid["total_fees"],
        "fee_date": sid["statement_date"],
        "fee_reason": "Late payment - minimum due not met by due date",
        "waived_flag": waived,
        "waived_reason": "GOODWILL_GESTURE" if waived else None,
        "waived_by": "CONTACT_CENTRE_AGENT" if waived else None,
    })
write_bronze(fee_rows, "fee", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bt_rows = []
for i, aid in enumerate(random.sample(ACCOUNT_IDS, k=max(1, len(ACCOUNT_IDS) // 10)), start=1):
    transfer_date = random_date(dt.date(2025, 1, 1), dt.date(2026, 6, 1))
    bt_rows.append({
        "balance_transfer_id": f"BT{str(i).zfill(7)}",
        "account_id": aid,
        "source_institution": fake.company() + " Bank",
        "transfer_amount": round(random.uniform(500, 8000), 2),
        "transfer_fee": round(random.uniform(0, 200), 2),
        "promo_apr": round(random.uniform(0.0, 4.9), 3),
        "promo_end_date": transfer_date + dt.timedelta(days=random.choice([180, 270, 365])),
        "transfer_date": transfer_date,
        "transfer_status": weighted({"COMPLETED": 90, "PENDING": 5, "REJECTED": 5}),
    })
write_bronze(bt_rows, "balance_transfer", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cash_rows = []
for i, cardid in enumerate(random.sample(CARD_IDS, k=max(1, len(CARD_IDS) // 8)), start=1):
    intl = random.random() < 0.15
    cash_rows.append({
        "cash_withdrawal_id": f"ATM{str(i).zfill(8)}",
        "card_id": cardid,
        "account_id": card_account_map[cardid],
        "atm_id": f"ATM{random.randint(1,9999):05d}",
        "withdrawal_date": random_date(dt.date(2025, 7, 1), dt.date(2026, 7, 18)),
        "withdrawal_amount": round(random.uniform(20, 500), 2),
        "withdrawal_currency": "GBP" if not intl else pick(["EUR", "USD"]),
        "cash_advance_fee": round(random.uniform(2, 15), 2),
        "country_code": "GBR" if not intl else pick(["FRA", "ESP", "USA", "IRL"]),
        "is_international_flag": intl,
    })
write_bronze(cash_rows, "cash_withdrawal", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emi_rows = []
emi_txns = random.sample(txn_rows, k=max(1, len(txn_rows) // 25))
for i, t in enumerate(emi_txns, start=1):
    tenure = pick([3, 6, 12, 18, 24])
    principal = t["transaction_amount"]
    rate = round(random.uniform(0.0, 14.9), 3)
    start = t["transaction_date"]
    emi_rows.append({
        "emi_plan_id": f"EMI{str(i).zfill(8)}",
        "account_id": t["account_id"],
        "transaction_id": t["transaction_id"],
        "principal_amount": principal,
        "tenure_months": tenure,
        "interest_rate": rate,
        "monthly_installment": round((principal * (1 + rate / 100)) / tenure, 2),
        "emi_start_date": start,
        "emi_end_date": start + dt.timedelta(days=30 * tenure),
        "installments_paid": random.randint(0, tenure),
        "emi_status": weighted({"ACTIVE": 60, "COMPLETED": 30, "DEFAULTED": 10}),
        "processing_fee": round(principal * 0.02, 2),
    })
write_bronze(emi_rows, "emi_plan", SRC["cards"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

loyalty_rows = []
reward_txns = [t for t in txn_rows if t["mcc_code"] in MCC_REWARD_ELIGIBLE][: max(1, len(txn_rows) // 3)]
running_balance = {}
for i, t in enumerate(reward_txns, start=1):
    earned = int(t["transaction_amount"])
    bal = running_balance.get(t["account_id"], 0) + earned
    redeemed = earned if random.random() < 0.1 else 0
    bal -= redeemed
    running_balance[t["account_id"]] = bal
    loyalty_rows.append({
        "loyalty_point_id": f"LP{str(i).zfill(9)}",
        "account_id": t["account_id"],
        "transaction_id": t["transaction_id"],
        "points_earned": earned,
        "points_redeemed": redeemed,
        "points_balance": bal,
        "transaction_date": t["transaction_date"],
        "expiry_date": t["transaction_date"] + dt.timedelta(days=365 * 3),
        "redemption_type": pick(["STATEMENT_CREDIT", "TRAVEL", "MERCHANDISE", "GIFT_CARD"]) if redeemed else None,
    })
write_bronze(loyalty_rows, "loyalty_point", SRC["marketing"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 9. Fraud & Risk Domain
`fraud_alert`, `fraud_case`, `chargeback`, `credit_bureau_data`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

alert_rows = []
FRAUD_ALERT_IDS = []
alert_seq = 1
fraud_candidate_txns = random.sample(txn_rows, k=max(1, int(len(txn_rows) * FRAUD_ALERT_RATE)))
for t in fraud_candidate_txns:
    aid = f"FA{str(alert_seq).zfill(7)}"
    FRAUD_ALERT_IDS.append(aid)
    alert_rows.append({
        "fraud_alert_id": aid,
        "transaction_id": t["transaction_id"],
        "card_id": t["card_id"],
        "alert_date": t["transaction_timestamp"],
        "alert_type": weighted({"VELOCITY": 30, "GEO_MISMATCH": 25, "UNUSUAL_MERCHANT": 25, "AMOUNT_ANOMALY": 20}),
        "fraud_score": random.randint(50, 100),
        "rule_triggered": pick(["RULE_VEL_001", "RULE_GEO_014", "RULE_MCC_009", "RULE_AMT_022"]),
        "alert_status": weighted({"CLOSED_FALSE_POSITIVE": 55, "CLOSED_CONFIRMED": 25, "OPEN": 20}),
        "resolution": pick(["CUSTOMER_CONFIRMED", "CARD_BLOCKED", "NO_ACTION", "CASE_OPENED"]),
    })
    alert_seq += 1
write_bronze(alert_rows, "fraud_alert", SRC["fraud"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

case_rows = []
confirmed_alerts = [a for a in alert_rows if a["alert_status"] == "CLOSED_CONFIRMED"]
for i, a in enumerate(confirmed_alerts, start=1):
    txn = next(t for t in fraud_candidate_txns if t["transaction_id"] == a["transaction_id"])
    open_date = a["alert_date"].date()
    case_rows.append({
        "fraud_case_id": f"FC{str(i).zfill(7)}",
        "fraud_alert_id": a["fraud_alert_id"],
        "customer_id": account_customer_map[txn["account_id"]],
        "case_open_date": open_date,
        "case_close_date": open_date + dt.timedelta(days=random.randint(3, 30)),
        "fraud_type": weighted({"CARD_NOT_PRESENT": 45, "LOST_STOLEN": 25, "ACCOUNT_TAKEOVER": 20, "COUNTERFEIT": 10}),
        "disputed_amount": txn["transaction_amount"],
        "confirmed_fraud_flag": True,
        "investigator_id": f"INV{random.randint(1,30):04d}",
        "case_status": weighted({"CLOSED": 85, "IN_PROGRESS": 15}),
        "recovery_amount": round(txn["transaction_amount"] * random.uniform(0, 1), 2),
    })
write_bronze(case_rows, "fraud_case", SRC["fraud"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

chargeback_rows = []
for i, c in enumerate(case_rows, start=1):
    chargeback_rows.append({
        "chargeback_id": f"CB{str(i).zfill(7)}",
        "transaction_id": next(a["transaction_id"] for a in alert_rows if a["fraud_alert_id"] == c["fraud_alert_id"]),
        "fraud_case_id": c["fraud_case_id"],
        "chargeback_date": c["case_open_date"] + dt.timedelta(days=2),
        "chargeback_reason_code": pick(["10.4", "4837", "4863", "13.1"]),
        "chargeback_amount": c["disputed_amount"],
        "chargeback_stage": weighted({"REPRESENTMENT": 30, "PRE_ARBITRATION": 10, "FIRST_CHARGEBACK": 60}),
        "chargeback_status": weighted({"WON": 45, "LOST": 25, "PENDING": 30}),
        "representment_date": c["case_open_date"] + dt.timedelta(days=random.randint(5, 20)),
        "final_outcome": weighted({"MERCHANT_LIABLE": 45, "ISSUER_LIABLE": 30, "SPLIT": 25}),
    })
write_bronze(chargeback_rows, "chargeback", SRC["fraud"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bureau_rows = []
for i, cid in enumerate(CUSTOMER_IDS, start=1):
    bureau_rows.append({
        "bureau_record_id": f"BUR{str(i).zfill(8)}",
        "customer_id": cid,
        "bureau_name": weighted({"EXPERIAN": 40, "EQUIFAX": 35, "TRANSUNION": 25}),
        "bureau_score": random.randint(0, 999),
        "report_date": random_date(dt.date(2025, 6, 1), dt.date(2026, 7, 1)),
        "total_external_debt": round(random.uniform(0, 40000), 2),
        "number_of_ccj": weighted({0: 90, 1: 7, 2: 2, 3: 1}),
        "number_of_defaults": weighted({0: 85, 1: 10, 2: 4, 3: 1}),
        "electoral_roll_match_flag": random.random() < 0.9,
        "bankruptcy_flag": random.random() < 0.01,
    })
write_bronze(bureau_rows, "credit_bureau_data", SRC["reference"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 10. Collections & Delinquency Domain
`collections`, `delinquency`, `write_off`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delinquent_accounts = random.sample(ACCOUNT_IDS, k=max(1, len(ACCOUNT_IDS) // 15))

delinquency_rows = []
for i, aid in enumerate(delinquent_accounts, start=1):
    dpd = weighted({15: 40, 30: 25, 60: 15, 90: 10, 120: 10})
    bucket = "1-29" if dpd < 30 else "30-59" if dpd < 60 else "60-89" if dpd < 90 else "90-119" if dpd < 120 else "120+"
    delinquency_rows.append({
        "delinquency_id": f"DEL{str(i).zfill(7)}",
        "account_id": aid,
        "delinquency_date": random_date(dt.date(2025, 9, 1), dt.date(2026, 7, 10)),
        "days_past_due": dpd,
        "dpd_bucket": bucket,
        "overdue_amount": round(random.uniform(25, 3000), 2),
        "cure_date": random_date(dt.date(2026, 6, 1), dt.date(2026, 7, 18)) if random.random() < 0.4 else None,
        "delinquency_status": weighted({"OPEN": 55, "CURED": 35, "ESCALATED": 10}),
    })
write_bronze(delinquency_rows, "delinquency", SRC["collections"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

collections_rows = []
for i, d in enumerate(delinquency_rows, start=1):
    collections_rows.append({
        "collections_id": f"COL{str(i).zfill(7)}",
        "account_id": d["account_id"],
        "collections_stage": weighted({"EARLY_ARREARS": 40, "LATE_ARREARS": 30, "PRE_LEGAL": 15, "LEGAL": 10, "DEBT_SALE": 5}),
        "assigned_agent_id": f"COLAGT{random.randint(1,40):04d}",
        "outstanding_balance": d["overdue_amount"],
        "promise_to_pay_date": d["delinquency_date"] + dt.timedelta(days=random.randint(3, 21)),
        "promise_to_pay_amount": round(d["overdue_amount"] * random.uniform(0.2, 1.0), 2),
        "last_contact_date": d["delinquency_date"] + dt.timedelta(days=random.randint(1, 10)),
        "contact_outcome": weighted({"PROMISE_TO_PAY": 35, "NO_ANSWER": 30, "DISPUTE": 15, "REFUSED_TO_PAY": 10, "PAID_IN_FULL": 10}),
        "collections_status": weighted({"OPEN": 60, "RESOLVED": 30, "WRITTEN_OFF": 10}),
    })
write_bronze(collections_rows, "collections", SRC["collections"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeoff_rows = []
writeoff_candidates = [c for c in collections_rows if c["collections_status"] == "WRITTEN_OFF"]
for i, c in enumerate(writeoff_candidates, start=1):
    sold = random.random() < 0.5
    writeoff_rows.append({
        "write_off_id": f"WO{str(i).zfill(7)}",
        "account_id": c["account_id"],
        "write_off_date": c["last_contact_date"] + dt.timedelta(days=random.randint(30, 90)),
        "write_off_amount": c["outstanding_balance"],
        "write_off_reason": weighted({"UNRECOVERABLE_DEBT": 60, "CUSTOMER_BANKRUPTCY": 25, "DECEASED": 15}),
        "recovery_amount": round(c["outstanding_balance"] * random.uniform(0, 0.3), 2),
        "sold_to_third_party_flag": sold,
        "debt_sale_date": c["last_contact_date"] + dt.timedelta(days=random.randint(91, 150)) if sold else None,
        "debt_buyer_name": fake.company() + " Debt Purchasing Ltd" if sold else None,
    })
write_bronze(writeoff_rows, "write_off", SRC["collections"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 11. Service, Complaints & Digital Activity Domain
`customer_complaint`, `customer_service_call`, `digital_banking_activity`,
`mobile_app_activity`, `web_banking_activity`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

complaint_rows = []
complaint_customers = random.sample(CUSTOMER_IDS, k=max(1, int(NUM_CUSTOMERS * COMPLAINT_RATE)))
for i, cid in enumerate(complaint_customers, start=1):
    cust_accounts = [aid for aid, c in account_customer_map.items() if c == cid]
    complaint_date = random_date(dt.date(2025, 7, 1), dt.date(2026, 7, 18))
    escalated = random.random() < 0.08
    complaint_rows.append({
        "complaint_id": f"CMPL{str(i).zfill(7)}",
        "customer_id": cid,
        "account_id": pick(cust_accounts) if cust_accounts else None,
        "complaint_date": complaint_date,
        "complaint_category": weighted({"FEES_CHARGES": 25, "FRAUD_DISPUTE": 20, "SERVICE_QUALITY": 20, "CARD_DELIVERY": 15, "CREDIT_LIMIT": 10, "OTHER": 10}),
        "complaint_channel": weighted({"PHONE": 40, "APP": 25, "EMAIL": 20, "BRANCH": 15}),
        "complaint_description": fake.sentence(nb_words=12),
        "resolution_date": complaint_date + dt.timedelta(days=random.randint(1, 21)),
        "resolution_outcome": weighted({"UPHELD": 40, "PARTIALLY_UPHELD": 20, "REJECTED": 30, "GOODWILL_PAYMENT": 10}),
        "escalated_to_fos_flag": escalated,
        "complaint_status": weighted({"CLOSED": 85, "OPEN": 15}),
    })
write_bronze(complaint_rows, "customer_complaint", SRC["crm"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

call_rows = []
call_customers = random.sample(CUSTOMER_IDS, k=max(1, int(NUM_CUSTOMERS * SERVICE_CALL_RATE)))
for i, cid in enumerate(call_customers, start=1):
    call_rows.append({
        "call_id": f"CALL{str(i).zfill(8)}",
        "customer_id": cid,
        "call_date": random_ts(dt.date(2025, 7, 1), dt.date(2026, 7, 18)),
        "call_reason": weighted({"BALANCE_ENQUIRY": 25, "CARD_ISSUE": 20, "FRAUD_REPORT": 15, "PAYMENT_ARRANGEMENT": 15, "GENERAL": 25}),
        "call_duration_seconds": random.randint(60, 1200),
        "agent_id": f"AGT{random.randint(1,100):04d}",
        "channel": weighted({"PHONE": 80, "WEBCHAT": 20}),
        "resolution_flag": random.random() < 0.82,
        "csat_score": random.randint(1, 5),
    })
write_bronze(call_rows, "customer_service_call", SRC["crm"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

digital_rows, mobile_rows, web_rows = [], [], []
d_seq = m_seq = w_seq = 1
for cid in CUSTOMER_IDS:
    for _ in range(random.randint(0, DIGITAL_ACTIVITY_PER_CUSTOMER)):
        ts = random_ts(dt.date(2025, 7, 1), dt.date(2026, 7, 18))
        channel = weighted({"MOBILE_APP": 65, "WEB": 35})
        digital_rows.append({
            "activity_id": f"DACT{str(d_seq).zfill(9)}",
            "customer_id": cid,
            "activity_date": ts,
            "activity_type": weighted({"LOGIN": 30, "BALANCE_CHECK": 25, "PAYMENT": 20, "STATEMENT_VIEW": 15, "CARD_FREEZE": 10}),
            "channel": channel,
            "device_type": weighted({"IOS": 45, "ANDROID": 40, "DESKTOP": 15}),
            "session_id": fake.uuid4(),
            "session_duration_seconds": random.randint(15, 900),
        })
        d_seq += 1

        if channel == "MOBILE_APP":
            login = ts
            logout = login + dt.timedelta(seconds=random.randint(30, 900))
            mobile_rows.append({
                "mobile_activity_id": f"MACT{str(m_seq).zfill(9)}",
                "customer_id": cid,
                "app_version": pick(["6.4.1", "6.5.0", "6.5.2", "6.6.0"]),
                "os_type": weighted({"IOS": 55, "ANDROID": 45}),
                "login_timestamp": login,
                "logout_timestamp": logout,
                "feature_used": weighted({"CARD_FREEZE": 15, "SPEND_INSIGHTS": 20, "PIN_RESET": 10, "STATEMENT": 30, "TRANSFER": 25}),
                "crash_flag": random.random() < 0.01,
                "push_notification_opt_in": random.random() < 0.7,
            })
            m_seq += 1
        else:
            web_rows.append({
                "web_activity_id": f"WACT{str(w_seq).zfill(9)}",
                "customer_id": cid,
                "session_id": fake.uuid4(),
                "ip_address": fake.ipv4(),
                "browser": weighted({"CHROME": 55, "SAFARI": 25, "EDGE": 15, "FIREFOX": 5}),
                "page_visited": pick(["/dashboard", "/statements", "/cards", "/payments", "/offers"]),
                "visit_timestamp": ts,
                "conversion_flag": random.random() < 0.05,
            })
            w_seq += 1

write_bronze(digital_rows, "digital_banking_activity", SRC["digital"])
write_bronze(mobile_rows, "mobile_app_activity", SRC["digital"])
write_bronze(web_rows, "web_banking_activity", SRC["digital"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 12. Regulatory Reporting

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reg_rows = []
for i, aid in enumerate(random.sample(ACCOUNT_IDS, k=max(1, len(ACCOUNT_IDS) // 5)), start=1):
    reg_rows.append({
        "report_id": f"REG{str(i).zfill(7)}",
        "report_type": weighted({"FCA_PSD2": 30, "CCA_1974": 25, "IFRS9_ECL": 25, "PRA_COREP": 20}),
        "reporting_period": pick(["2026Q1", "2026Q2"]),
        "account_id": aid,
        "regulatory_category": weighted({"STAGE_1": 60, "STAGE_2": 25, "STAGE_3": 15}),
        "reported_amount": round(random.uniform(0, 5000), 2),
        "submission_date": random_date(dt.date(2026, 4, 1), dt.date(2026, 7, 15)),
        "submission_status": weighted({"SUBMITTED": 90, "AMENDED": 8, "REJECTED": 2}),
        "regulator_name": weighted({"FCA": 60, "PRA": 40}),
    })
write_bronze(reg_rows, "regulatory_reporting", SRC["reference"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 13. Run Summary

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=" * 90)
print(f"Synthetic data generation complete | ingestion_date={INGESTION_DATE} | batch_id={BATCH_ID}")
print(f"Customers={len(CUSTOMER_IDS)}  Accounts={len(ACCOUNT_IDS)}  Cards={len(CARD_IDS)}  "
      f"Transactions={len(TRANSACTION_IDS)}  Merchants={len(MERCHANT_IDS)}")
print("All 40 tables written as Delta under Files/bronze/<source_system>/... — see cell output above for paths.")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
