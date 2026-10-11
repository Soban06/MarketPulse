# MarketPulse

A lakehouse pipeline for daily S&P 500 market data, built on Databricks (Free Edition)
with PySpark and Delta Lake.

- **Sources:** Yahoo Finance via `yfinance` (daily OHLCV, adjusted close, dividends, splits,
  company profiles) and Wikipedia's S&P 500 constituents table (GICS sector, sub-industry,
  date added).
- **Scale:** ~503 tickers × 10 years ≈ 1.2M daily price rows (~367 MB raw JSON per full load,
  ~8 MB per daily incremental batch).

## Architecture

```
 LAPTOP (internet access)                       DATABRICKS FREE EDITION (PySpark + Delta)
 extract/ingest.py ──► data/landing/<batch_id>/
 extract/upload.py ─────────── upload ────────► /Volumes/workspace/raw/landing/<batch_id>/
                                                        │  01_raw_to_bronze
                                                        ▼
                                                 workspace.bronze.*   raw values, explicit STRING schemas
                                                        │  02_bronze_to_silver
                                                        ▼
                                                 workspace.silver.*   typed, MERGEd on natural keys
                                                 workspace.ops.pipeline_execution_logs
```

Extraction runs outside Databricks because Free Edition serverless compute cannot resolve
external hosts (`yfinance` fails with `Could not resolve host: query2.finance.yahoo.com`).
The extractor lands each run as a self-contained batch folder; everything from the raw
landing zone onwards runs in Spark.

## Repository layout

```
extract/            local Source -> Raw extraction and upload to the raw Volume
src/marketpulse/    pipeline modules (schemas, audit logging, drift handling, layer logic)
notebooks/          thin Databricks notebooks that read parameters and call src/marketpulse
data/landing/       local raw batches (git-ignored; each member extracts their own)
```

## Raw landing batch format

```
data/landing/<batch_id>/              batch_id = <UTC stamp>_<mode>, e.g. 20261010T132844Z_full
    sp500_constituents.json           Wikipedia S&P 500 table
    prices/<TICKER>.json              daily OHLC, Adj Close, Volume, Dividends, Stock Splits
    company_info/<TICKER>.json        yfinance Ticker.info payload
    manifest.json                     written last; a batch without it is incomplete
```

## Data models

Every table lives in Unity Catalog as `<catalog>.<schema>.<table>`; the catalog is a pipeline
parameter and defaults to `workspace`. The column-level tables below are generated from
`src/marketpulse/schemas.py` by `scripts/generate_data_dictionary.py`, so they always match what
the pipeline creates.

### Layer conventions

| Layer | Location | Holds | Types | Keys |
|---|---|---|---|---|
| Raw | `/Volumes/<catalog>/raw/landing/<batch_id>/` | JSON files exactly as extracted, one folder per batch | as in the source | none |
| Bronze | `<catalog>.bronze.*` | One row per source record per batch | every source value is `STRING` | grain documented, not enforced |
| Silver | `<catalog>.silver.*` | Cleaned, typed, deduplicated tables, MERGEd on natural keys | real types (`DATE`, `DOUBLE`, `BIGINT`, ...) | enforced: `NOT NULL` + `PRIMARY KEY` |

- **Schema-on-read.** Every raw file is read with an explicit `StructType`; Spark schema inference is never used.
- **Bronze values are strings.** A type change at the source (for example `Volume` arriving as `"1.2M"`) can never fail a bronze load. Values are converted with `try_cast` on the way to silver, and rows that fail land in a `quarantine_*` table with a `reject_reason`.
- **Keys are enforced from silver onwards.** Bronze must accept whatever arrived, including malformed rows, so its keys describe the grain only.
- **Lineage.** Every bronze row carries `batch_id`, `source_file` and `load_timestamp`. Every silver row carries `load_timestamp` and the `batch_id` that last wrote it.
- **Schema drift.** Bronze `prices` and `constituents` have a `_rescued_data` column that captures any field the schema does not know. `company_info` deliberately has none: Yahoo returns about 190 profile fields and MarketPulse keeps 18, so the ignored fields would fill it on every row and it would never signal real drift.

### Relationships

```mermaid
erDiagram
    dim_ticker ||--o{ fact_daily_price : "ticker"
    fact_daily_price ||--o{ price_revision : "ticker, date"
```

- `fact_daily_price` joins to `dim_ticker` on `ticker`.
- Dividends and stock splits are kept in `bronze.prices` (`dividends`, `stock_splits`); a dedicated silver table for them is planned for a later phase.
- `price_revision` records every change the silver MERGE applied to a `fact_daily_price` row, keyed by the batch that caused it.
- `quarantine_prices` and `quarantine_company_info` hold bronze rows rejected on the way to silver, with the bronze columns plus `reject_reason`.

<!-- data-dictionary:start -->

### Raw → bronze column mapping

Raw JSON field names are renamed to snake_case in bronze (Delta column names cannot contain spaces).

**prices/<TICKER>.json**

| Raw JSON field | Bronze column |
|---|---|
| `Ticker` | `ticker` |
| `Date` | `date` |
| `Open` | `open` |
| `High` | `high` |
| `Low` | `low` |
| `Close` | `close` |
| `Adj Close` | `adj_close` |
| `Volume` | `volume` |
| `Dividends` | `dividends` |
| `Stock Splits` | `stock_splits` |

**sp500_constituents.json**

| Raw JSON field | Bronze column |
|---|---|
| `Symbol` | `symbol` |
| `Security` | `security` |
| `GICS Sector` | `gics_sector` |
| `GICS Sub-Industry` | `gics_sub_industry` |
| `Headquarters Location` | `headquarters_location` |
| `Date added` | `date_added` |
| `CIK` | `cik` |
| `Founded` | `founded` |

**company_info/<TICKER>.json**

| Raw JSON field | Bronze column |
|---|---|
| `_ticker` | `ticker` |
| `_fetched_at` | `fetched_at` |
| `longName` | `long_name` |
| `sector` | `sector` |
| `industry` | `industry` |
| `fullExchangeName` | `full_exchange_name` |
| `currency` | `currency` |
| `country` | `country` |
| `marketCap` | `market_cap` |
| `sharesOutstanding` | `shares_outstanding` |
| `beta` | `beta` |
| `trailingPE` | `trailing_pe` |
| `forwardPE` | `forward_pe` |
| `dividendYield` | `dividend_yield` |
| `payoutRatio` | `payout_ratio` |
| `fullTimeEmployees` | `full_time_employees` |
| `website` | `website` |
| `firstTradeDateMilliseconds` | `first_trade_date_ms` |

### Bronze tables

#### `bronze.prices`

Raw daily prices, dividends and splits per batch, all values as strings

**Grain:** `batch_id`, `ticker`, `date` (documented, not enforced)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | yes | Yahoo ticker symbol, e.g. BRK-B |
| `date` | `string` | yes | Trading day, YYYY-MM-DD |
| `open` | `string` | yes | Opening price |
| `high` | `string` | yes | Intraday high |
| `low` | `string` | yes | Intraday low |
| `close` | `string` | yes | Closing price (split-adjusted, not dividend-adjusted) |
| `adj_close` | `string` | yes | Close adjusted for splits and dividends; Yahoo revises it after each dividend |
| `volume` | `string` | yes | Shares traded |
| `dividends` | `string` | yes | Cash dividend per share going ex on this day, 0 if none |
| `stock_splits` | `string` | yes | Split ratio effective on this day (4.0 = 4-for-1), 0 if none |
| `_rescued_data` | `string` | yes | JSON of any source fields not in the schema; non-null means the source schema drifted |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `source_file` | `string` | yes | Full path of the raw JSON file this row was read from |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |

#### `bronze.constituents`

Raw S&P 500 constituents table from Wikipedia per batch, all values as strings

**Grain:** `batch_id`, `symbol` (documented, not enforced)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `symbol` | `string` | yes | Ticker as listed by Wikipedia (class shares use a dot, e.g. BRK.B) |
| `security` | `string` | yes | Company name |
| `gics_sector` | `string` | yes | GICS sector |
| `gics_sub_industry` | `string` | yes | GICS sub-industry |
| `headquarters_location` | `string` | yes | City and state or country of headquarters |
| `date_added` | `string` | yes | Date the company joined the S&P 500, YYYY-MM-DD |
| `cik` | `string` | yes | SEC Central Index Key |
| `founded` | `string` | yes | Founding year as written on Wikipedia, sometimes a range or note |
| `_rescued_data` | `string` | yes | JSON of any source fields not in the schema; non-null means the source schema drifted |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `source_file` | `string` | yes | Full path of the raw JSON file this row was read from |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |

#### `bronze.company_info`

Raw yfinance company profiles per batch (selected fields), all values as strings

**Grain:** `batch_id`, `ticker` (documented, not enforced)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | yes | Ticker the profile was requested for (added by extract/ingest.py) |
| `fetched_at` | `string` | yes | UTC ISO timestamp the profile was fetched (added by extract/ingest.py) |
| `long_name` | `string` | yes | Full company name |
| `sector` | `string` | yes | Yahoo sector |
| `industry` | `string` | yes | Yahoo industry |
| `full_exchange_name` | `string` | yes | Listing exchange, e.g. NasdaqGS |
| `currency` | `string` | yes | Trading currency |
| `country` | `string` | yes | Country of headquarters |
| `market_cap` | `string` | yes | Market capitalisation at fetch time |
| `shares_outstanding` | `string` | yes | Shares outstanding |
| `beta` | `string` | yes | Beta versus the market |
| `trailing_pe` | `string` | yes | Trailing twelve-month price/earnings |
| `forward_pe` | `string` | yes | Forward price/earnings |
| `dividend_yield` | `string` | yes | Dividend yield as reported by Yahoo |
| `payout_ratio` | `string` | yes | Share of earnings paid as dividends |
| `full_time_employees` | `string` | yes | Full-time employees |
| `website` | `string` | yes | Company website |
| `first_trade_date_ms` | `string` | yes | First trading day as epoch milliseconds |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `source_file` | `string` | yes | Full path of the raw JSON file this row was read from |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |

### Silver tables

#### `silver.fact_daily_price`

One row per ticker per trading day, MERGEd so upstream revisions update in place

**Primary key:** `ticker`, `date` (enforced: `NOT NULL` + `PRIMARY KEY` constraint)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | no | Yahoo ticker symbol |
| `date` | `date` | no | Trading day |
| `open` | `double` | yes | Opening price |
| `high` | `double` | yes | Intraday high |
| `low` | `double` | yes | Intraday low |
| `close` | `double` | yes | Closing price (split-adjusted) |
| `adj_close` | `double` | yes | Close adjusted for splits and dividends (latest Yahoo revision) |
| `volume` | `bigint` | yes | Shares traded |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |

#### `silver.dim_ticker`

One row per company: Wikipedia constituents joined with Yahoo company profiles

**Primary key:** `ticker` (enforced: `NOT NULL` + `PRIMARY KEY` constraint)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | no | Yahoo ticker symbol |
| `company_name` | `string` | yes | Company name (Yahoo long name, else Wikipedia) |
| `gics_sector` | `string` | yes | GICS sector (Wikipedia) |
| `gics_sub_industry` | `string` | yes | GICS sub-industry (Wikipedia) |
| `yf_sector` | `string` | yes | Sector as classified by Yahoo |
| `yf_industry` | `string` | yes | Industry as classified by Yahoo |
| `exchange` | `string` | yes | Listing exchange |
| `currency` | `string` | yes | Trading currency |
| `country` | `string` | yes | Country of headquarters |
| `headquarters` | `string` | yes | Headquarters location (Wikipedia) |
| `sp500_date_added` | `date` | yes | Date added to the S&P 500 |
| `cik` | `bigint` | yes | SEC Central Index Key |
| `founded` | `string` | yes | Founding year as written on Wikipedia |
| `market_cap` | `bigint` | yes | Market capitalisation at info_as_of |
| `shares_outstanding` | `bigint` | yes | Shares outstanding at info_as_of |
| `beta` | `double` | yes | Beta versus the market |
| `trailing_pe` | `double` | yes | Trailing price/earnings |
| `forward_pe` | `double` | yes | Forward price/earnings |
| `dividend_yield` | `double` | yes | Dividend yield as reported by Yahoo |
| `payout_ratio` | `double` | yes | Share of earnings paid as dividends |
| `full_time_employees` | `bigint` | yes | Full-time employees |
| `website` | `string` | yes | Company website |
| `first_trade_date` | `date` | yes | First trading day on Yahoo |
| `info_as_of` | `timestamp` | yes | When the Yahoo profile fields were fetched |
| `is_sp500_member` | `boolean` | yes | In the most recently loaded S&P 500 constituents list |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |

#### `silver.price_revision`

Audit of every upstream price change caught by the fact_daily_price MERGE

**Primary key:** `ticker`, `date`, `batch_id` (enforced: `NOT NULL` + `PRIMARY KEY` constraint)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | no | Yahoo ticker symbol |
| `date` | `date` | no | Trading day whose values changed |
| `old_close` | `double` | yes | Close before the revision |
| `new_close` | `double` | yes | Close after the revision |
| `old_adj_close` | `double` | yes | Adjusted close before the revision |
| `new_adj_close` | `double` | yes | Adjusted close after the revision |
| `adj_close_pct` | `double` | yes | Relative change in adjusted close, new / old - 1 |
| `old_volume` | `bigint` | yes | Volume before the revision |
| `new_volume` | `bigint` | yes | Volume after the revision |
| `previous_batch_id` | `string` | yes | Batch that wrote the old values |
| `batch_id` | `string` | no | Batch whose MERGE detected the change |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |

#### `silver.quarantine_prices`

Bronze price rows rejected on the way to silver, with the reason

**Grain:** `batch_id`, `ticker`, `date` (documented, not enforced)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | yes | Yahoo ticker symbol, e.g. BRK-B |
| `date` | `string` | yes | Trading day, YYYY-MM-DD |
| `open` | `string` | yes | Opening price |
| `high` | `string` | yes | Intraday high |
| `low` | `string` | yes | Intraday low |
| `close` | `string` | yes | Closing price (split-adjusted, not dividend-adjusted) |
| `adj_close` | `string` | yes | Close adjusted for splits and dividends; Yahoo revises it after each dividend |
| `volume` | `string` | yes | Shares traded |
| `dividends` | `string` | yes | Cash dividend per share going ex on this day, 0 if none |
| `stock_splits` | `string` | yes | Split ratio effective on this day (4.0 = 4-for-1), 0 if none |
| `_rescued_data` | `string` | yes | JSON of any source fields not in the schema; non-null means the source schema drifted |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `source_file` | `string` | yes | Full path of the raw JSON file this row was read from |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |
| `reject_reason` | `string` | yes | Why the row failed validation or casting |

#### `silver.quarantine_company_info`

Bronze company profile rows rejected on the way to silver, with the reason

**Grain:** `batch_id`, `ticker` (documented, not enforced)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ticker` | `string` | yes | Ticker the profile was requested for (added by extract/ingest.py) |
| `fetched_at` | `string` | yes | UTC ISO timestamp the profile was fetched (added by extract/ingest.py) |
| `long_name` | `string` | yes | Full company name |
| `sector` | `string` | yes | Yahoo sector |
| `industry` | `string` | yes | Yahoo industry |
| `full_exchange_name` | `string` | yes | Listing exchange, e.g. NasdaqGS |
| `currency` | `string` | yes | Trading currency |
| `country` | `string` | yes | Country of headquarters |
| `market_cap` | `string` | yes | Market capitalisation at fetch time |
| `shares_outstanding` | `string` | yes | Shares outstanding |
| `beta` | `string` | yes | Beta versus the market |
| `trailing_pe` | `string` | yes | Trailing twelve-month price/earnings |
| `forward_pe` | `string` | yes | Forward price/earnings |
| `dividend_yield` | `string` | yes | Dividend yield as reported by Yahoo |
| `payout_ratio` | `string` | yes | Share of earnings paid as dividends |
| `full_time_employees` | `string` | yes | Full-time employees |
| `website` | `string` | yes | Company website |
| `first_trade_date_ms` | `string` | yes | First trading day as epoch milliseconds |
| `batch_id` | `string` | yes | Landing batch this row came from, e.g. 20261010T132844Z_full |
| `source_file` | `string` | yes | Full path of the raw JSON file this row was read from |
| `load_timestamp` | `timestamp` | yes | When this row was written to this table |
| `reject_reason` | `string` | yes | Why the row failed validation or casting |

### Operational tables

#### `ops.pipeline_execution_logs`

One row per pipeline step (batch × source → target table) across every layer.

**Primary key:** `log_id` (enforced)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `log_id` | `string` | no | Unique id of this log entry (primary key) |
| `pipeline_run_id` | `string` | yes | Groups every entry written by one notebook or job run |
| `layer` | `string` | yes | source_to_raw \| raw_to_bronze \| bronze_to_silver |
| `load_type` | `string` | yes | full \| incremental \| backfill |
| `batch_id` | `string` | yes | Landing batch processed, e.g. 20261010T132844Z_full |
| `parameter` | `string` | yes | Exact input processed: a Volume path or the backfill parameters |
| `target_table` | `string` | yes | Fully qualified table (or Volume) written to |
| `start_time` | `timestamp` | yes | When the step started |
| `end_time` | `timestamp` | yes | When the step finished; null while RUNNING |
| `duration_seconds` | `double` | yes | end_time minus start_time |
| `status` | `string` | yes | RUNNING \| SUCCESS \| FAILED \| SKIPPED |
| `rows_read` | `bigint` | yes | Rows read from the source of this step |
| `rows_inserted` | `bigint` | yes | Rows inserted into the target |
| `rows_updated` | `bigint` | yes | Existing target rows updated |
| `rows_quarantined` | `bigint` | yes | Rows rejected to a quarantine table |
| `error_message` | `string` | yes | Exception (FAILED) or warning text |
| `executed_by` | `string` | yes | Databricks identity that ran the step |
| `load_timestamp` | `timestamp` | yes | When this log row was last written |

<!-- data-dictionary:end -->

## Execution guide

The pipeline has three steps. Each one takes parameters, so the same code runs the daily load and
any backfill; nothing is hard-coded to "today".

| Step | Runs on | Entry point | Parameters |
|---|---|---|---|
| Source → Raw | laptop | `extract/ingest.py`, then `extract/upload.py` | `--mode`, `--period`, `--tickers`; `--batch-id`, `--overwrite` |
| Raw → Bronze | Databricks | `notebooks/01_raw_to_bronze` | `catalog`, `batch_ids`, `from_batch`, `to_batch`, `sources`, `pipeline_run_id` |
| Bronze → Silver | Databricks | `notebooks/02_bronze_to_silver` | `catalog`, `batch_ids`, `from_batch`, `to_batch`, `start_date`, `end_date`, `pipeline_run_id` |

Notebook parameters are widgets: input boxes at the top of the notebook once it has run once. They
are also the parameter names to use when the notebooks run as Databricks Job tasks.

### One-time setup

**Databricks**

1. Create a Git folder from this repository (Workspace → your user folder → Create → Git folder).
2. Open `notebooks/00_setup`, attach it to **Serverless** and run all. It creates the `raw`,
   `bronze`, `silver` and `ops` schemas, the `raw.landing` Volume, every bronze and silver table
   and `ops.pipeline_execution_logs`. The `catalog` widget defaults to `workspace`. Safe to re-run.

**Laptop** (extraction and upload)

```bash
pip install -r requirements.txt
winget install Databricks.DatabricksCLI
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile marketpulse
```

`auth login` opens a browser to sign in and stores the `marketpulse` profile in
`~/.databrickscfg`; `extract/upload.py` uses it by default.

### Standard load (initial full load and daily incremental)

1. Extract a batch on the laptop and upload it:

   ```bash
   python extract/ingest.py --mode full          # first time: 10-year baseline for all S&P 500 tickers
   python extract/ingest.py --mode incremental   # every day after: re-pull the last month
   python extract/upload.py                      # send every new local batch to the raw Volume
   ```

2. In Databricks, run `01_raw_to_bronze` with **all batch parameters empty**.
3. Then run `02_bronze_to_silver` with **all batch and date parameters empty**.

With empty parameters each notebook processes only the batches it has not yet completed, oldest
first, judged from `ops.pipeline_execution_logs`. A second run with nothing new prints
"Nothing to load" / "Nothing to merge". In silver, new trading days are inserted, days whose values
Yahoo revised (typically `adj_close` after a dividend) are updated and recorded in
`silver.price_revision`, and unchanged days are left alone. Log entries are typed `full` or
`incremental` from the batch's extraction mode.

### Backfills

Fill in the widgets **before** clicking Run all. Any batch or date parameter makes the run a
backfill (`load_type = backfill` in the log) and processes exactly what was asked for, whether or
not it was processed before.

| Goal | Notebook | Widgets |
|---|---|---|
| Re-load one or more batches from raw into bronze | `01` | `batch_ids` = `20261010T133403Z_incremental` (comma-separate several) |
| Re-load every batch in a time window | `01` | `from_batch` = `20261001T000000Z`, `to_batch` = `20261031T235959Z` |
| Load a newly added source into batches already in bronze | `01` | `sources` = `constituents,company_info`, plus `batch_ids` or `from_batch`/`to_batch` |
| Re-merge one or more batches into silver | `02` | `batch_ids` = `20261010T133403Z_incremental` |
| Re-merge every batch in a time window | `02` | `from_batch` / `to_batch` as above |
| Re-process a range of trading days from every batch (prices) | `02` | `start_date` = `2018-01-01`, `end_date` = `2018-12-31` |
| Re-process a range of trading days from specific batches | `02` | `batch_ids` plus `start_date` / `end_date` |
| Extract a longer history from the source | laptop | `python extract/ingest.py --mode full --period max` (yfinance periods: `1y`, `5y`, `10y`, `max`, ...), upload, then a standard run picks the new batch up |

Batch ids are the folder names under `/Volumes/<catalog>/raw/landing/` (Catalog → workspace → raw →
Volumes → landing), and sort by time because they start with their UTC timestamp. Batch ranges
compare ids as text, so a bare timestamp such as `20261001T000000Z` works as a bound.

What makes backfills safe to repeat:

- **Raw → Bronze** writes each batch with `replaceWhere batch_id = …`: re-loading a batch replaces
  its rows, it never appends a second copy.
- **Bronze → Silver** `MERGE`s on the natural key and only updates rows whose values changed, so
  re-merging unchanged data reports 0 inserted / 0 updated. Quarantine tables are rebuilt per batch.
- A silver row is only updated by a batch at least as new as the one that last wrote it, so
  backfilling an old batch fills gaps but never overwrites newer prices.
- `start_date` / `end_date` apply to prices only; `dim_ticker` has no dates.

Widget values are remembered between runs: **clear them after a backfill**, or the next "standard"
run will be another backfill.

### Running as a Databricks Job

Jobs & Pipelines → Create job, with two notebook tasks on Serverless: `01_raw_to_bronze`, then
`02_bronze_to_silver` depending on it. Give both tasks the parameter
`pipeline_run_id` = `{{job.run_id}}` so one run's log entries share an id; leave the batch
parameters empty for the daily run, or set them on "Run now with different parameters" for a backfill.

### Checking a run

Every step writes one row per batch × source → target table:

```sql
SELECT layer, load_type, batch_id, target_table, status,
       rows_read, rows_inserted, rows_updated, rows_quarantined,
       start_time, end_time, duration_seconds, error_message
FROM workspace.ops.pipeline_execution_logs
ORDER BY start_time DESC;
```

```sql
-- rejected rows and why
SELECT batch_id, reject_reason, count(*) FROM workspace.silver.quarantine_prices GROUP BY ALL;

-- upstream price revisions applied by the MERGE
SELECT * FROM workspace.silver.price_revision ORDER BY load_timestamp DESC;
```

### When something fails

- **Raw → Bronze:** a failed batch is logged `FAILED` with its error, the remaining batches still
  load, and the notebook fails at the end listing every failure. Fix the cause and run again with
  empty parameters: batches whose latest run failed are picked up again.
- **Bronze → Silver:** batches must apply in order, so the first failure stops the run and every
  later batch is logged `SKIPPED`. Fix the cause and run again with empty parameters.
- A row stuck at `RUNNING` in the log means the process died mid-step; the batch is retried on
  the next run.
- After pulling new code or switching the Git folder branch, notebooks pick up changed modules
  automatically (`%autoreload`); if a module seems stale, restart Python
  (`dbutils.library.restartPython()`).

### Tests and demos

All three run against throwaway schemas and drop them afterwards, so the real tables are never touched.

| Notebook | Shows |
|---|---|
| `80_test_audit_logging` | SUCCESS / FAILED / SKIPPED logging, Delta row counts (incl. 0/0 on an identical MERGE), batch tracking |
| `81_test_schemas` | every table created exactly as `schemas.py` defines it; real raw files read with the explicit schemas |
| `91_demo_schema_drift` | a batch with a new field and `Volume = "abc"`: bronze rescues the field into `_rescued_data`, silver quarantines the bad row and merges the rest |
