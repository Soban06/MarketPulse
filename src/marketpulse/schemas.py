"""
Explicit schemas for every raw file and every bronze/silver table in MarketPulse.

Nothing in the pipeline infers a schema. Raw JSON is read with the RAW_* schemas
and every table is created from a TableSpec defined here.

Layer conventions
-----------------
Raw     Source field names exactly as they appear in the JSON, and every value read
        as STRING. A type change upstream (say Volume arriving as "1.2M") therefore
        can never fail the read; it is caught by try_cast on the way to silver and
        the row is quarantined.
Bronze  The same string values, renamed to snake_case (Delta column names cannot
        contain spaces), plus lineage columns: batch_id, source_file,
        load_timestamp, and _rescued_data for fields the schema does not know about.
Silver  Real types, one row per natural key (declared as a primary key
        constraint), plus batch_id and load_timestamp.

Each source is described once as (raw name, bronze name, description) tuples, and
the raw read schema, the bronze schema and the rename map are all derived from
that list, so they cannot drift apart.
"""

from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RESCUED_DATA_COLUMN = "_rescued_data"


def _field(name, data_type, comment, nullable=True):
    return StructField(name, data_type, nullable, metadata={"comment": comment})


# ============================================================== source fields
# (raw JSON field name, bronze column name, description)

PRICES_FIELDS = [
    ("Ticker", "ticker", "Yahoo ticker symbol, e.g. BRK-B"),
    ("Date", "date", "Trading day, YYYY-MM-DD"),
    ("Open", "open", "Opening price"),
    ("High", "high", "Intraday high"),
    ("Low", "low", "Intraday low"),
    ("Close", "close", "Closing price (split-adjusted, not dividend-adjusted)"),
    ("Adj Close", "adj_close", "Close adjusted for splits and dividends; Yahoo revises it after each dividend"),
    ("Volume", "volume", "Shares traded"),
    ("Dividends", "dividends", "Cash dividend per share going ex on this day, 0 if none"),
    ("Stock Splits", "stock_splits", "Split ratio effective on this day (4.0 = 4-for-1), 0 if none"),
]

CONSTITUENTS_FIELDS = [
    ("Symbol", "symbol", "Ticker as listed by Wikipedia (class shares use a dot, e.g. BRK.B)"),
    ("Security", "security", "Company name"),
    ("GICS Sector", "gics_sector", "GICS sector"),
    ("GICS Sub-Industry", "gics_sub_industry", "GICS sub-industry"),
    ("Headquarters Location", "headquarters_location", "City and state or country of headquarters"),
    ("Date added", "date_added", "Date the company joined the S&P 500, YYYY-MM-DD"),
    ("CIK", "cik", "SEC Central Index Key"),
    ("Founded", "founded", "Founding year as written on Wikipedia, sometimes a range or note"),
]

# Only the yfinance Ticker.info keys MarketPulse uses. Yahoo returns ~190 keys and
# which ones appear varies by company, so extra keys are expected rather than drift.
COMPANY_INFO_FIELDS = [
    ("_ticker", "ticker", "Ticker the profile was requested for (added by extract/ingest.py)"),
    ("_fetched_at", "fetched_at", "UTC ISO timestamp the profile was fetched (added by extract/ingest.py)"),
    ("longName", "long_name", "Full company name"),
    ("sector", "sector", "Yahoo sector"),
    ("industry", "industry", "Yahoo industry"),
    ("fullExchangeName", "full_exchange_name", "Listing exchange, e.g. NasdaqGS"),
    ("currency", "currency", "Trading currency"),
    ("country", "country", "Country of headquarters"),
    ("marketCap", "market_cap", "Market capitalisation at fetch time"),
    ("sharesOutstanding", "shares_outstanding", "Shares outstanding"),
    ("beta", "beta", "Beta versus the market"),
    ("trailingPE", "trailing_pe", "Trailing twelve-month price/earnings"),
    ("forwardPE", "forward_pe", "Forward price/earnings"),
    ("dividendYield", "dividend_yield", "Dividend yield as reported by Yahoo"),
    ("payoutRatio", "payout_ratio", "Share of earnings paid as dividends"),
    ("fullTimeEmployees", "full_time_employees", "Full-time employees"),
    ("website", "website", "Company website"),
    ("firstTradeDateMilliseconds", "first_trade_date_ms", "First trading day as epoch milliseconds"),
]


def _raw_schema(fields) -> StructType:
    return StructType([_field(raw, StringType(), desc) for raw, _, desc in fields])


def _column_map(fields) -> dict[str, str]:
    return {raw: bronze for raw, bronze, _ in fields}


# ========================================================== raw read schemas
# Used as spark.read.schema(RAW_PRICES).option("multiLine", True).json(path)

RAW_PRICES = _raw_schema(PRICES_FIELDS)
RAW_CONSTITUENTS = _raw_schema(CONSTITUENTS_FIELDS)
RAW_COMPANY_INFO = _raw_schema(COMPANY_INFO_FIELDS)

# raw field name -> bronze column name, for DataFrame.withColumnsRenamed
PRICES_COLUMN_MAP = _column_map(PRICES_FIELDS)
CONSTITUENTS_COLUMN_MAP = _column_map(CONSTITUENTS_FIELDS)
COMPANY_INFO_COLUMN_MAP = _column_map(COMPANY_INFO_FIELDS)


# ============================================================== table specs

@dataclass(frozen=True)
class TableSpec:
    layer: str                       # "bronze" | "silver"
    name: str                        # table name inside the layer's schema
    schema: StructType
    key: tuple[str, ...]             # natural key / grain of the table
    comment: str
    enforce_key: bool = True         # declare key as NOT NULL + PRIMARY KEY constraint

    def full_name(self, cfg) -> str:
        return cfg.bronze(self.name) if self.layer == "bronze" else cfg.silver(self.name)


BATCH_ID = _field("batch_id", StringType(), "Landing batch this row came from, e.g. 20261010T132844Z_full")
SOURCE_FILE = _field("source_file", StringType(), "Full path of the raw JSON file this row was read from")
LOAD_TIMESTAMP = _field("load_timestamp", TimestampType(), "When this row was written to this table")
RESCUED_DATA = _field(
    RESCUED_DATA_COLUMN, StringType(),
    "JSON of any source fields not in the schema; non-null means the source schema drifted",
)


def _bronze_schema(fields, rescued: bool) -> StructType:
    columns = [_field(bronze, StringType(), desc) for _, bronze, desc in fields]
    lineage = [RESCUED_DATA] if rescued else []
    return StructType(columns + lineage + [BATCH_ID, SOURCE_FILE, LOAD_TIMESTAMP])


# ================================================================== bronze
# Bronze accepts whatever arrived, including malformed rows, so keys are documented
# as the grain but not enforced; enforcement happens in silver.

BRONZE_PRICES = TableSpec(
    layer="bronze",
    name="prices",
    schema=_bronze_schema(PRICES_FIELDS, rescued=True),
    key=("batch_id", "ticker", "date"),
    comment="Raw daily prices, dividends and splits per batch, all values as strings",
    enforce_key=False,
)

BRONZE_CONSTITUENTS = TableSpec(
    layer="bronze",
    name="constituents",
    schema=_bronze_schema(CONSTITUENTS_FIELDS, rescued=True),
    key=("batch_id", "symbol"),
    comment="Raw S&P 500 constituents table from Wikipedia per batch, all values as strings",
    enforce_key=False,
)

# No _rescued_data here: the ~170 Yahoo keys we deliberately ignore would fill it on
# every row, so it would never signal real drift.
BRONZE_COMPANY_INFO = TableSpec(
    layer="bronze",
    name="company_info",
    schema=_bronze_schema(COMPANY_INFO_FIELDS, rescued=False),
    key=("batch_id", "ticker"),
    comment="Raw yfinance company profiles per batch (selected fields), all values as strings",
    enforce_key=False,
)


# ================================================================== silver

SILVER_FACT_DAILY_PRICE = TableSpec(
    layer="silver",
    name="fact_daily_price",
    schema=StructType([
        _field("ticker", StringType(), "Yahoo ticker symbol", nullable=False),
        _field("date", DateType(), "Trading day", nullable=False),
        _field("open", DoubleType(), "Opening price"),
        _field("high", DoubleType(), "Intraday high"),
        _field("low", DoubleType(), "Intraday low"),
        _field("close", DoubleType(), "Closing price (split-adjusted)"),
        _field("adj_close", DoubleType(), "Close adjusted for splits and dividends (latest Yahoo revision)"),
        _field("volume", LongType(), "Shares traded"),
        BATCH_ID,
        LOAD_TIMESTAMP,
    ]),
    key=("ticker", "date"),
    comment="One row per ticker per trading day, MERGEd so upstream revisions update in place",
)

SILVER_DIM_TICKER = TableSpec(
    layer="silver",
    name="dim_ticker",
    schema=StructType([
        _field("ticker", StringType(), "Yahoo ticker symbol", nullable=False),
        _field("company_name", StringType(), "Company name (Yahoo long name, else Wikipedia)"),
        _field("gics_sector", StringType(), "GICS sector (Wikipedia)"),
        _field("gics_sub_industry", StringType(), "GICS sub-industry (Wikipedia)"),
        _field("yf_sector", StringType(), "Sector as classified by Yahoo"),
        _field("yf_industry", StringType(), "Industry as classified by Yahoo"),
        _field("exchange", StringType(), "Listing exchange"),
        _field("currency", StringType(), "Trading currency"),
        _field("country", StringType(), "Country of headquarters"),
        _field("headquarters", StringType(), "Headquarters location (Wikipedia)"),
        _field("sp500_date_added", DateType(), "Date added to the S&P 500"),
        _field("cik", LongType(), "SEC Central Index Key"),
        _field("founded", StringType(), "Founding year as written on Wikipedia"),
        _field("market_cap", LongType(), "Market capitalisation at info_as_of"),
        _field("shares_outstanding", LongType(), "Shares outstanding at info_as_of"),
        _field("beta", DoubleType(), "Beta versus the market"),
        _field("trailing_pe", DoubleType(), "Trailing price/earnings"),
        _field("forward_pe", DoubleType(), "Forward price/earnings"),
        _field("dividend_yield", DoubleType(), "Dividend yield as reported by Yahoo"),
        _field("payout_ratio", DoubleType(), "Share of earnings paid as dividends"),
        _field("full_time_employees", LongType(), "Full-time employees"),
        _field("website", StringType(), "Company website"),
        _field("first_trade_date", DateType(), "First trading day on Yahoo"),
        _field("info_as_of", TimestampType(), "When the Yahoo profile fields were fetched"),
        _field("is_sp500_member", BooleanType(), "In the most recently loaded S&P 500 constituents list"),
        BATCH_ID,
        LOAD_TIMESTAMP,
    ]),
    key=("ticker",),
    comment="One row per company: Wikipedia constituents joined with Yahoo company profiles",
)

SILVER_PRICE_REVISION = TableSpec(
    layer="silver",
    name="price_revision",
    schema=StructType([
        _field("ticker", StringType(), "Yahoo ticker symbol", nullable=False),
        _field("date", DateType(), "Trading day whose values changed", nullable=False),
        _field("old_close", DoubleType(), "Close before the revision"),
        _field("new_close", DoubleType(), "Close after the revision"),
        _field("old_adj_close", DoubleType(), "Adjusted close before the revision"),
        _field("new_adj_close", DoubleType(), "Adjusted close after the revision"),
        _field("adj_close_pct", DoubleType(), "Relative change in adjusted close, new / old - 1"),
        _field("old_volume", LongType(), "Volume before the revision"),
        _field("new_volume", LongType(), "Volume after the revision"),
        _field("previous_batch_id", StringType(), "Batch that wrote the old values"),
        _field("batch_id", StringType(), "Batch whose MERGE detected the change", nullable=False),
        LOAD_TIMESTAMP,
    ]),
    key=("ticker", "date", "batch_id"),
    comment="Audit of every upstream price change caught by the fact_daily_price MERGE",
)

REJECT_REASON = _field("reject_reason", StringType(), "Why the row failed validation or casting")

SILVER_QUARANTINE_PRICES = TableSpec(
    layer="silver",
    name="quarantine_prices",
    schema=StructType(BRONZE_PRICES.schema.fields + [REJECT_REASON]),
    key=("batch_id", "ticker", "date"),
    comment="Bronze price rows rejected on the way to silver, with the reason",
    enforce_key=False,
)

SILVER_QUARANTINE_COMPANY_INFO = TableSpec(
    layer="silver",
    name="quarantine_company_info",
    schema=StructType(BRONZE_COMPANY_INFO.schema.fields + [REJECT_REASON]),
    key=("batch_id", "ticker"),
    comment="Bronze company profile rows rejected on the way to silver, with the reason",
    enforce_key=False,
)

BRONZE_TABLES = (BRONZE_PRICES, BRONZE_CONSTITUENTS, BRONZE_COMPANY_INFO)
SILVER_TABLES = (
    SILVER_FACT_DAILY_PRICE,
    SILVER_DIM_TICKER,
    SILVER_PRICE_REVISION,
    SILVER_QUARANTINE_PRICES,
    SILVER_QUARANTINE_COMPANY_INFO,
)
ALL_TABLES = BRONZE_TABLES + SILVER_TABLES


# =================================================================== DDL

def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_table_ddl(spec: TableSpec, cfg) -> str:
    """CREATE TABLE IF NOT EXISTS statement with column comments and, for enforced keys, a PK."""
    key_columns = set(spec.key) if spec.enforce_key else set()
    columns = [
        f"{f.name} {f.dataType.simpleString()}"
        f"{' NOT NULL' if (not f.nullable or f.name in key_columns) else ''}"
        f" COMMENT {_sql_string(f.metadata.get('comment', ''))}"
        for f in spec.schema.fields
    ]
    if spec.enforce_key:
        columns.append(f"CONSTRAINT {spec.layer}_{spec.name}_pk PRIMARY KEY ({', '.join(spec.key)})")
    body = ",\n    ".join(columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {spec.full_name(cfg)} (\n    {body}\n)\n"
        f"COMMENT {_sql_string(spec.comment)}"
    )


def create_table(spark, cfg, spec: TableSpec):
    spark.sql(create_table_ddl(spec, cfg))


def create_all_tables(spark, cfg):
    for spec in ALL_TABLES:
        create_table(spark, cfg, spec)