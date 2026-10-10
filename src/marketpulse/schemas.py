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
    IntegerType,
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
