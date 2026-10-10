"""
MarketPulse -- Silver layer: MERGE bronze runs into a DuckDB star schema.

    python load_silver.py                    # load every bronze run not loaded yet, oldest first
    python load_silver.py --reload RUN_ID    # re-MERGE a run that was already loaded

Bronze runs (see ingest.py) are applied in chronological order, so newer pulls win.
Prices are MERGEd on (ticker, date): new days are inserted, and days whose values
changed upstream -- typically Adj Close after a dividend or split -- are updated
in place. Every such change is recorded in price_revision.

Tables in data/warehouse/marketpulse.duckdb:
    fact_daily_price        (ticker, date)               OHLC, adj_close, volume
    fact_corporate_action   (ticker, date, action_type)  dividend per share / split ratio
    dim_ticker              ticker                       company, GICS + Yahoo sector/industry, market cap, ...
    dim_date                date                         calendar over the price history
    price_revision          audit of every upstream price change caught by a MERGE
    etl_run_log             one row per loaded bronze run with insert/update counts
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BRONZE_ROOT = Path("data/bronze")
DB_PATH = Path("data/warehouse/marketpulse.duckdb")

PRICE_VALUE_COLS = ["open", "high", "low", "close", "adj_close", "volume"]

# yfinance Ticker.info key -> dim_ticker column
INFO_FIELDS = {
    "longName": "company_name",
    "sector": "yf_sector",
    "industry": "yf_industry",
    "fullExchangeName": "exchange",
    "currency": "currency",
    "country": "country",
    "marketCap": "market_cap",
    "sharesOutstanding": "shares_outstanding",
    "beta": "beta",
    "trailingPE": "trailing_pe",
    "forwardPE": "forward_pe",
    "dividendYield": "dividend_yield",
    "payoutRatio": "payout_ratio",
    "fullTimeEmployees": "full_time_employees",
    "website": "website",
    "firstTradeDateMilliseconds": "first_trade_ms",
    "_fetched_at": "info_as_of",
}

DIM_TICKER_COLS = [
    "company_name", "gics_sector", "gics_sub_industry", "yf_sector", "yf_industry",
    "exchange", "currency", "country", "headquarters", "sp500_date_added", "cik", "founded",
    "market_cap", "shares_outstanding", "beta", "trailing_pe", "forward_pe", "dividend_yield",
    "payout_ratio", "full_time_employees", "website", "first_trade_date", "info_as_of",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_daily_price (
    ticker      VARCHAR NOT NULL,
    date        DATE    NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    adj_close   DOUBLE,
    volume      BIGINT,
    source_run  VARCHAR,
    inserted_at TIMESTAMP,
    updated_at  TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS fact_corporate_action (
    ticker      VARCHAR NOT NULL,
    date        DATE    NOT NULL,
    action_type VARCHAR NOT NULL,   -- 'dividend' (cash per share) | 'split' (ratio, e.g. 4.0 = 4-for-1)
    value       DOUBLE,
    source_run  VARCHAR,
    inserted_at TIMESTAMP,
    updated_at  TIMESTAMP,
    PRIMARY KEY (ticker, date, action_type)
);

CREATE TABLE IF NOT EXISTS dim_ticker (
    ticker              VARCHAR PRIMARY KEY,
    company_name        VARCHAR,
    gics_sector         VARCHAR,
    gics_sub_industry   VARCHAR,
    yf_sector           VARCHAR,
    yf_industry         VARCHAR,
    exchange            VARCHAR,
    currency            VARCHAR,
    country             VARCHAR,
    headquarters        VARCHAR,
    sp500_date_added    DATE,
    cik                 BIGINT,
    founded             VARCHAR,
    market_cap          BIGINT,
    shares_outstanding  BIGINT,
    beta                DOUBLE,
    trailing_pe         DOUBLE,
    forward_pe          DOUBLE,
    dividend_yield      DOUBLE,
    payout_ratio        DOUBLE,
    full_time_employees BIGINT,
    website             VARCHAR,
    first_trade_date    DATE,
    info_as_of          TIMESTAMP,
    is_sp500_member     BOOLEAN,
    source_run          VARCHAR,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_revision (
    ticker         VARCHAR,
    date           DATE,
    old_close      DOUBLE,
    new_close      DOUBLE,
    old_adj_close  DOUBLE,
    new_adj_close  DOUBLE,
    adj_close_pct  DOUBLE,
    old_volume     BIGINT,
    new_volume     BIGINT,
    previous_run   VARCHAR,
    detected_run   VARCHAR,
    detected_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id            VARCHAR PRIMARY KEY,
    mode              VARCHAR,
    period            VARCHAR,
    bronze_bytes      BIGINT,
    price_rows_staged BIGINT,
    prices_inserted   BIGINT,
    prices_updated    BIGINT,
    actions_inserted  BIGINT,
    actions_updated   BIGINT,
    tickers_merged    BIGINT,
    loaded_at         TIMESTAMP
);
"""


def changed_condition(cols, left="t", right="s"):
    return " OR ".join(f"{left}.{c} IS DISTINCT FROM {right}.{c}" for c in cols)


# ---------------------------------------------------------------------- staging

def stage_prices(con, run_dir: Path, run_id: str) -> int:
    files = (run_dir / "prices").as_posix() + "/*.json"
    if not list((run_dir / "prices").glob("*.json")):
        con.execute("CREATE OR REPLACE TEMP TABLE stg_raw_price AS SELECT NULL::VARCHAR AS ticker WHERE false")
        con.execute("CREATE OR REPLACE TEMP TABLE stg_price AS SELECT * FROM fact_daily_price WHERE false")
        con.execute("CREATE OR REPLACE TEMP TABLE stg_action AS SELECT * FROM fact_corporate_action WHERE false")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg_raw_price AS
        SELECT * FROM read_json('{files}', format = 'array', columns = {{
            'Ticker': 'VARCHAR', 'Date': 'DATE',
            'Open': 'DOUBLE', 'High': 'DOUBLE', 'Low': 'DOUBLE', 'Close': 'DOUBLE',
            'Adj Close': 'DOUBLE', 'Volume': 'DOUBLE',
            'Dividends': 'DOUBLE', 'Stock Splits': 'DOUBLE'
        }})
        WHERE Close IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY Ticker, Date) = 1
    """)

    # Yahoo serves float32-ish values; rounding keeps unchanged rows byte-identical
    # between pulls so IS DISTINCT FROM only fires on real revisions.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg_price AS
        SELECT Ticker AS ticker, Date AS date,
               round(Open, 6) AS open, round(High, 6) AS high, round(Low, 6) AS low,
               round(Close, 6) AS close, round("Adj Close", 6) AS adj_close,
               CAST(Volume AS BIGINT) AS volume,
               ? AS source_run, now() AS inserted_at, now() AS updated_at
        FROM stg_raw_price
    """, [run_id])

    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg_action AS
        SELECT Ticker AS ticker, Date AS date, 'dividend' AS action_type, round(Dividends, 6) AS value,
               ? AS source_run, now() AS inserted_at, now() AS updated_at
        FROM stg_raw_price WHERE Dividends > 0
        UNION ALL
        SELECT Ticker, Date, 'split', round("Stock Splits", 6), ?, now(), now()
        FROM stg_raw_price WHERE "Stock Splits" > 0
    """, [run_id, run_id])

    return con.execute("SELECT count(*) FROM stg_price").fetchone()[0]


def stage_tickers(con, run_dir: Path, run_id: str) -> bool:
    """Build stg_ticker from Wikipedia constituents + yfinance info. Returns True if constituents exist."""
    constituents_path = run_dir / "sp500_constituents.json"
    wiki = pd.DataFrame(json.loads(constituents_path.read_text(encoding="utf-8"))) \
        if constituents_path.exists() else pd.DataFrame()
    if wiki.empty:
        wiki = pd.DataFrame(columns=["Symbol", "Security", "GICS Sector", "GICS Sub-Industry",
                                     "Headquarters Location", "Date added", "CIK", "Founded"])
    wiki = wiki.astype(object)

    info_rows = []
    for path in sorted((run_dir / "company_info").glob("*.json")):
        info = json.loads(path.read_text(encoding="utf-8"))
        row = {col: info.get(key) for key, col in INFO_FIELDS.items()}
        row["ticker"] = info.get("_ticker", path.stem)
        info_rows.append(row)
    info_df = pd.DataFrame(info_rows, columns=["ticker", *INFO_FIELDS.values()]).astype(object)

    con.register("wiki_df", wiki)
    con.register("info_df", info_df)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg_wiki AS
        SELECT upper(replace(trim("Symbol"), '.', '-')) AS ticker,
               "Security"                          AS security,
               "GICS Sector"                       AS gics_sector,
               "GICS Sub-Industry"                 AS gics_sub_industry,
               "Headquarters Location"             AS headquarters,
               TRY_CAST("Date added" AS DATE)      AS sp500_date_added,
               TRY_CAST("CIK" AS BIGINT)           AS cik,
               CAST("Founded" AS VARCHAR)          AS founded
        FROM wiki_df
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg_info AS
        SELECT ticker,
               CAST(company_name AS VARCHAR)            AS company_name,
               CAST(yf_sector AS VARCHAR)               AS yf_sector,
               CAST(yf_industry AS VARCHAR)             AS yf_industry,
               CAST(exchange AS VARCHAR)                AS exchange,
               CAST(currency AS VARCHAR)                AS currency,
               CAST(country AS VARCHAR)                 AS country,
               TRY_CAST(market_cap AS BIGINT)           AS market_cap,
               TRY_CAST(shares_outstanding AS BIGINT)   AS shares_outstanding,
               TRY_CAST(beta AS DOUBLE)                 AS beta,
               TRY_CAST(trailing_pe AS DOUBLE)          AS trailing_pe,
               TRY_CAST(forward_pe AS DOUBLE)           AS forward_pe,
               TRY_CAST(dividend_yield AS DOUBLE)       AS dividend_yield,
               TRY_CAST(payout_ratio AS DOUBLE)         AS payout_ratio,
               TRY_CAST(full_time_employees AS BIGINT)  AS full_time_employees,
               CAST(website AS VARCHAR)                 AS website,
               CAST(to_timestamp(TRY_CAST(first_trade_ms AS BIGINT) / 1000) AS DATE) AS first_trade_date,
               TRY_CAST(info_as_of AS TIMESTAMPTZ)::TIMESTAMP AS info_as_of
        FROM info_df
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg_ticker AS
        SELECT coalesce(w.ticker, i.ticker)          AS ticker,
               coalesce(i.company_name, w.security)  AS company_name,
               w.gics_sector, w.gics_sub_industry, i.yf_sector, i.yf_industry,
               i.exchange, i.currency, i.country, w.headquarters, w.sp500_date_added, w.cik, w.founded,
               i.market_cap, i.shares_outstanding, i.beta, i.trailing_pe, i.forward_pe,
               i.dividend_yield, i.payout_ratio, i.full_time_employees, i.website,
               i.first_trade_date, i.info_as_of
        FROM stg_wiki w FULL OUTER JOIN stg_info i ON w.ticker = i.ticker
    """)
    con.unregister("wiki_df")
    con.unregister("info_df")
    return len(wiki) > 0


# ------------------------------------------------------------------------ merges

def merge_prices(con) -> tuple[int, int]:
    cond = changed_condition(PRICE_VALUE_COLS)
    inserted, updated = con.execute(f"""
        SELECT count(*) FILTER (WHERE t.ticker IS NULL),
               count(*) FILTER (WHERE t.ticker IS NOT NULL AND ({cond}))
        FROM stg_price s LEFT JOIN fact_daily_price t ON t.ticker = s.ticker AND t.date = s.date
    """).fetchone()

    con.execute(f"""
        INSERT INTO price_revision
        SELECT s.ticker, s.date, t.close, s.close, t.adj_close, s.adj_close,
               s.adj_close / nullif(t.adj_close, 0) - 1,
               t.volume, s.volume, t.source_run, s.source_run, now()
        FROM stg_price s JOIN fact_daily_price t ON t.ticker = s.ticker AND t.date = s.date
        WHERE {cond}
    """)

    set_clause = ", ".join(f"{c} = s.{c}" for c in PRICE_VALUE_COLS)
    con.execute(f"""
        MERGE INTO fact_daily_price AS t
        USING stg_price AS s
        ON t.ticker = s.ticker AND t.date = s.date
        WHEN MATCHED AND ({cond}) THEN
            UPDATE SET {set_clause}, source_run = s.source_run, updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (ticker, date, {", ".join(PRICE_VALUE_COLS)}, source_run, inserted_at, updated_at)
            VALUES (s.ticker, s.date, {", ".join("s." + c for c in PRICE_VALUE_COLS)},
                    s.source_run, s.inserted_at, s.updated_at)
    """)
    return inserted, updated


def merge_actions(con) -> tuple[int, int]:
    inserted, updated = con.execute("""
        SELECT count(*) FILTER (WHERE t.ticker IS NULL),
               count(*) FILTER (WHERE t.ticker IS NOT NULL AND t.value IS DISTINCT FROM s.value)
        FROM stg_action s LEFT JOIN fact_corporate_action t
          ON t.ticker = s.ticker AND t.date = s.date AND t.action_type = s.action_type
    """).fetchone()
    con.execute("""
        MERGE INTO fact_corporate_action AS t
        USING stg_action AS s
        ON t.ticker = s.ticker AND t.date = s.date AND t.action_type = s.action_type
        WHEN MATCHED AND t.value IS DISTINCT FROM s.value THEN
            UPDATE SET value = s.value, source_run = s.source_run, updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (ticker, date, action_type, value, source_run, inserted_at, updated_at)
            VALUES (s.ticker, s.date, s.action_type, s.value, s.source_run, s.inserted_at, s.updated_at)
    """)
    return inserted, updated


def merge_tickers(con, run_id: str, has_constituents: bool) -> int:
    # coalesce so a run without company info (--skip-info) doesn't blank out existing attributes
    set_clause = ", ".join(f"{c} = coalesce(s.{c}, t.{c})" for c in DIM_TICKER_COLS)
    con.execute(f"""
        MERGE INTO dim_ticker AS t
        USING stg_ticker AS s
        ON t.ticker = s.ticker
        WHEN MATCHED THEN
            UPDATE SET {set_clause}, source_run = '{run_id}', updated_at = now()
        WHEN NOT MATCHED THEN
            INSERT (ticker, {", ".join(DIM_TICKER_COLS)}, source_run, updated_at)
            VALUES (s.ticker, {", ".join("s." + c for c in DIM_TICKER_COLS)}, '{run_id}', now())
    """)
    if has_constituents:
        con.execute("UPDATE dim_ticker SET is_sp500_member = ticker IN (SELECT ticker FROM stg_wiki)")
    return con.execute("SELECT count(*) FROM stg_ticker").fetchone()[0]


def rebuild_dim_date(con):
    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH bounds AS (SELECT min(date) AS lo, max(date) AS hi FROM fact_daily_price),
        cal AS (
            SELECT CAST(unnest(generate_series(lo, hi, INTERVAL 1 DAY)) AS DATE) AS date
            FROM bounds WHERE lo IS NOT NULL
        ),
        trading AS (SELECT DISTINCT date FROM fact_daily_price)
        SELECT CAST(strftime(c.date, '%Y%m%d') AS INTEGER) AS date_key,
               c.date,
               year(c.date)              AS year,
               quarter(c.date)           AS quarter,
               month(c.date)             AS month,
               monthname(c.date)         AS month_name,
               weekofyear(c.date)        AS iso_week,
               isodow(c.date)            AS day_of_week,
               dayname(c.date)           AS day_name,
               isodow(c.date) >= 6       AS is_weekend,
               tr.date IS NOT NULL       AS is_trading_day,
               c.date = last_day(c.date) AS is_month_end
        FROM cal c LEFT JOIN trading tr ON tr.date = c.date
        ORDER BY c.date
    """)


# --------------------------------------------------------------------------- run

def load_run(con, run_dir: Path):
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    run_id = manifest["run_id"]

    con.execute("BEGIN TRANSACTION")
    try:
        staged = stage_prices(con, run_dir, run_id)
        p_ins, p_upd = merge_prices(con)
        a_ins, a_upd = merge_actions(con)
        has_constituents = stage_tickers(con, run_dir, run_id)
        n_tickers = merge_tickers(con, run_id, has_constituents)

        con.execute("DELETE FROM etl_run_log WHERE run_id = ?", [run_id])
        con.execute(
            "INSERT INTO etl_run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, manifest["mode"], manifest["period"], manifest.get("size_bytes"),
             staged, p_ins, p_upd, a_ins, a_upd, n_tickers, datetime.now(timezone.utc)],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    print(f"  {run_id}: staged {staged:,} price rows -> {p_ins:,} inserted, {p_upd:,} updated "
          f"| corporate actions {a_ins:,} new, {a_upd:,} changed | {n_tickers} tickers in dim")


def main(reload_run: str | None):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(SCHEMA)

    complete_runs = sorted(p.parent for p in BRONZE_ROOT.glob("*/manifest.json"))
    if reload_run:
        todo = [r for r in complete_runs if r.name == reload_run]
        if not todo:
            raise SystemExit(f"No complete bronze run named {reload_run}")
    else:
        loaded = {r for (r,) in con.execute("SELECT run_id FROM etl_run_log").fetchall()}
        todo = [r for r in complete_runs if r.name not in loaded]

    if not todo:
        print("Nothing to load -- every bronze run is already in the warehouse.")
    else:
        print(f"Loading {len(todo)} bronze run(s) into {DB_PATH}")
        for run_dir in todo:
            load_run(con, run_dir)
        rebuild_dim_date(con)

    for table in ["fact_daily_price", "fact_corporate_action", "dim_ticker", "dim_date", "price_revision"]:
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table:<22} {n:>12,} rows")
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MERGE bronze runs into the DuckDB silver layer")
    parser.add_argument("--reload", metavar="RUN_ID", help="Re-apply one bronze run even if already loaded")
    args = parser.parse_args()
    main(args.reload)
