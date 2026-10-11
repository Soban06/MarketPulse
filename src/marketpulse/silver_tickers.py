"""
Bronze -> Silver for companies: bronze.constituents + bronze.company_info -> silver.dim_ticker.

For each batch, oldest first:

  1. stage       Wikipedia constituents: ticker normalised to Yahoo form (BRK.B -> BRK-B),
                 date added and CIK cast to DATE / BIGINT.
                 Yahoo company info: try_cast every numeric field, epoch milliseconds ->
                 first_trade_date, fetched_at -> info_as_of; work out a reject_reason
  2. quarantine  rejected company_info rows -> silver.quarantine_company_info, replaced per batch
  3. merge       FULL OUTER JOIN the two sources on ticker and MERGE into silver.dim_ticker:
                 - a new ticker is inserted
                 - an existing ticker is updated only where this batch has a value
                   (coalesce(new, old)), so a batch with only some companies' profiles
                   never blanks out the others
                 - when the batch has a constituents list, tickers missing from it are
                   flagged is_sp500_member = false rather than deleted

Like prices, a row is only updated by a batch at least as new as the one that last
wrote it, and a re-run that changes nothing updates nothing (0 inserted, 0 updated).

Batch selection:
    batch_ids / from_batch..to_batch    backfill those bronze batches
    nothing                             bronze batches not yet merged into dim_ticker
"""

from marketpulse.audit import AuditLogger
from marketpulse.schemas import (
    BRONZE_COMPANY_INFO,
    BRONZE_CONSTITUENTS,
    SILVER_DIM_TICKER,
    SILVER_QUARANTINE_COMPANY_INFO,
    create_table,
)
from marketpulse.silver_prices import LAYER, SilverLoadError, _check_batch_id, _load_type

BIGINT_FIELDS = ["market_cap", "shares_outstanding", "full_time_employees"]
DOUBLE_FIELDS = ["beta", "trailing_pe", "forward_pe", "dividend_yield", "payout_ratio"]

# dim_ticker columns written from the staged batch (everything except the key and lineage)
DIM_COLUMNS = [c for c in SILVER_DIM_TICKER.schema.fieldNames() if c not in ("ticker", "batch_id", "load_timestamp")]


# ------------------------------------------------------------------- selection

def bronze_batches(spark, cfg) -> list[str]:
    rows = spark.sql(f"""
        SELECT batch_id FROM {BRONZE_CONSTITUENTS.full_name(cfg)}
        UNION
        SELECT batch_id FROM {BRONZE_COMPANY_INFO.full_name(cfg)}
    """).collect()
    return sorted(r["batch_id"] for r in rows if r["batch_id"])


def select_batches(spark, cfg, audit: AuditLogger, batch_ids=None, from_batch=None,
                   to_batch=None) -> tuple[list[str], bool]:
    """Return (batches to merge, is_backfill)."""
    available = bronze_batches(spark, cfg)
    if batch_ids:
        missing = [b for b in batch_ids if b not in available]
        if missing:
            raise ValueError(f"Batch(es) have no constituents or company info in bronze: {missing}")
        return sorted(batch_ids), True
    if from_batch or to_batch:
        return [b for b in available
                if (not from_batch or b >= from_batch) and (not to_batch or b <= to_batch)], True
    target = SILVER_DIM_TICKER.full_name(cfg)
    return audit.pending_batches(LAYER, available, target_table=target), False


# --------------------------------------------------------------------- staging

def stage_company_info(spark, cfg, batch_id: str):
    """Typed company profiles of one batch plus reject_reason. Registered as `stg_company_info`."""
    numeric = BIGINT_FIELDS + DOUBLE_FIELDS
    casts = ",\n                   ".join(
        [f"try_cast(try_cast({c} AS DOUBLE) AS BIGINT) AS {c}_v" for c in BIGINT_FIELDS]
        + [f"try_cast({c} AS DOUBLE) AS {c}_v" for c in DOUBLE_FIELDS]
    )
    reasons = ",\n                       ".join([
        "CASE WHEN ticker IS NULL OR trim(ticker) = '' THEN 'missing ticker' END",
        *[f"CASE WHEN {c} IS NOT NULL AND try_cast({c} AS DOUBLE) IS NULL THEN 'unparseable {c}' END"
          for c in numeric],
        "CASE WHEN first_trade_date_ms IS NOT NULL AND try_cast(first_trade_date_ms AS BIGINT) IS NULL "
        "THEN 'unparseable first_trade_date_ms' END",
        "CASE WHEN fetched_at IS NOT NULL AND try_cast(fetched_at AS TIMESTAMP) IS NULL "
        "THEN 'unparseable fetched_at' END",
    ])
    staged = spark.sql(f"""
        WITH typed AS (
            SELECT b.*,
                   trim(ticker) AS ticker_v,
                   {casts},
                   CAST(timestamp_millis(try_cast(first_trade_date_ms AS BIGINT)) AS DATE) AS first_trade_date_v,
                   try_cast(fetched_at AS TIMESTAMP) AS info_as_of_v,
                   nullif(concat_ws('; ',
                       {reasons}
                   ), '') AS invalid_reason
            FROM {BRONZE_COMPANY_INFO.full_name(cfg)} b
            WHERE batch_id = '{_check_batch_id(batch_id)}'
        )
        SELECT *,
               coalesce(invalid_reason,
                        CASE WHEN row_number() OVER (PARTITION BY ticker_v
                                                     ORDER BY invalid_reason IS NULL DESC, source_file) > 1
                             THEN 'duplicate ticker in batch' END) AS reject_reason
        FROM typed
    """)
    staged.createOrReplaceTempView("stg_company_info")
    return staged


def stage_constituents(spark, cfg, batch_id: str):
    """Typed S&P 500 list of one batch, one row per ticker. Registered as `stg_constituents`."""
    ticker = "upper(replace(trim(symbol), '.', '-'))"  # Wikipedia BRK.B -> Yahoo BRK-B
    staged = spark.sql(f"""
        SELECT {ticker}                         AS ticker,
               security,
               gics_sector,
               gics_sub_industry,
               headquarters_location            AS headquarters,
               try_cast(date_added AS DATE)     AS sp500_date_added,
               try_cast(cik AS BIGINT)          AS cik,
               founded
        FROM {BRONZE_CONSTITUENTS.full_name(cfg)}
        WHERE batch_id = '{_check_batch_id(batch_id)}' AND symbol IS NOT NULL AND trim(symbol) <> ''
        QUALIFY row_number() OVER (PARTITION BY {ticker} ORDER BY source_file) = 1
    """)
    staged.createOrReplaceTempView("stg_constituents")
    return staged


def stage_dim_ticker(spark, batch_id: str, has_constituents: bool):
    """`stg_dim_ticker`: both sources joined on ticker, in dim_ticker's shape."""
    membership = "w.ticker IS NOT NULL" if has_constituents else "CAST(NULL AS BOOLEAN)"
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW stg_dim_ticker AS
        SELECT coalesce(w.ticker, i.ticker_v)       AS ticker,
               coalesce(i.long_name, w.security)    AS company_name,
               w.gics_sector,
               w.gics_sub_industry,
               i.sector                             AS yf_sector,
               i.industry                           AS yf_industry,
               i.full_exchange_name                 AS exchange,
               i.currency,
               i.country,
               w.headquarters,
               w.sp500_date_added,
               w.cik,
               w.founded,
               i.market_cap_v                       AS market_cap,
               i.shares_outstanding_v               AS shares_outstanding,
               i.beta_v                             AS beta,
               i.trailing_pe_v                      AS trailing_pe,
               i.forward_pe_v                       AS forward_pe,
               i.dividend_yield_v                   AS dividend_yield,
               i.payout_ratio_v                     AS payout_ratio,
               i.full_time_employees_v              AS full_time_employees,
               i.website,
               i.first_trade_date_v                 AS first_trade_date,
               i.info_as_of_v                       AS info_as_of,
               {membership}                         AS is_sp500_member,
               '{batch_id}'                         AS batch_id
        FROM stg_constituents w
        FULL OUTER JOIN (SELECT * FROM stg_company_info WHERE reject_reason IS NULL) i
          ON w.ticker = i.ticker_v
    """)


# -------------------------------------------------------------------- one batch

def merge_batch(spark, cfg, audit: AuditLogger, batch_id: str, load_type: str):
    dim = SILVER_DIM_TICKER.full_name(cfg)
    quarantine = SILVER_QUARANTINE_COMPANY_INFO.full_name(cfg)
    parameter = f"batch_id={batch_id}"

    company_info = stage_company_info(spark, cfg, batch_id)
    has_constituents = stage_constituents(spark, cfg, batch_id).count() > 0
    stage_dim_ticker(spark, batch_id, has_constituents)

    # Quarantine is rebuilt for the whole batch, so re-runs never duplicate rejects.
    bronze_columns = [c for c in BRONZE_COMPANY_INFO.schema.fieldNames() if c != "load_timestamp"]
    with audit.step(LAYER, load_type=load_type, batch_id=batch_id,
                    parameter=parameter, target_table=quarantine) as log:
        (
            company_info.where("reject_reason IS NOT NULL")
            .selectExpr(*bronze_columns, "current_timestamp() AS load_timestamp", "reject_reason")
            .select(*SILVER_QUARANTINE_COMPANY_INFO.schema.fieldNames())
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", f"batch_id = '{batch_id}'")
            .saveAsTable(quarantine)
        )
        log.record_delta_metrics()
        quarantined = log.rows_inserted
        log.record(rows_read=company_info.count(), rows_quarantined=quarantined)

    newer = "s.batch_id >= t.batch_id"
    changed = " OR ".join(f"coalesce(s.{c}, t.{c}) IS DISTINCT FROM t.{c}" for c in DIM_COLUMNS)
    updates = ", ".join(f"{c} = coalesce(s.{c}, t.{c})" for c in DIM_COLUMNS)
    # Only a batch that carries the full constituents list can say a company left the index.
    left_index = f"""
        WHEN NOT MATCHED BY SOURCE AND t.is_sp500_member AND t.batch_id <= '{batch_id}' THEN UPDATE SET
            is_sp500_member = false, batch_id = '{batch_id}', load_timestamp = current_timestamp()
    """ if has_constituents else ""

    with audit.step(LAYER, load_type=load_type, batch_id=batch_id,
                    parameter=parameter, target_table=dim) as log:
        spark.sql(f"""
            MERGE INTO {dim} t
            USING stg_dim_ticker s
            ON t.ticker = s.ticker
            WHEN MATCHED AND {newer} AND ({changed}) THEN UPDATE SET
                {updates}, batch_id = s.batch_id, load_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                (ticker, {", ".join(DIM_COLUMNS)}, batch_id, load_timestamp)
            VALUES
                (s.ticker, {", ".join("s." + c for c in DIM_COLUMNS)}, s.batch_id, current_timestamp())
            {left_index}
        """)
        log.record_delta_metrics()
        log.record(rows_quarantined=quarantined)
    return log


# ------------------------------------------------------------------------- run

def run(spark, cfg, *, batch_ids=None, from_batch=None, to_batch=None, pipeline_run_id=None) -> list:
    """Merge the selected bronze batches into dim_ticker. Returns the dim_ticker StepLogs."""
    for spec in (SILVER_DIM_TICKER, SILVER_QUARANTINE_COMPANY_INFO,
                 BRONZE_CONSTITUENTS, BRONZE_COMPANY_INFO):
        create_table(spark, cfg, spec)

    audit = AuditLogger(spark, cfg, pipeline_run_id=pipeline_run_id)
    batches, is_backfill = select_batches(spark, cfg, audit, batch_ids, from_batch, to_batch)
    print(f"{LAYER} tickers: {len(batches)} batch(es), {'backfill' if is_backfill else 'new batches only'}")

    logs = []
    for i, batch_id in enumerate(batches):
        load_type = "backfill" if is_backfill else _load_type(batch_id)
        try:
            log = merge_batch(spark, cfg, audit, batch_id, load_type)
        except Exception as error:
            for later in batches[i + 1:]:
                audit.log_skipped(LAYER, load_type="backfill" if is_backfill else _load_type(later),
                                  batch_id=later, parameter=f"batch_id={later}",
                                  target_table=SILVER_DIM_TICKER.full_name(cfg),
                                  reason=f"earlier batch {batch_id} failed")
            raise SilverLoadError(
                f"{batch_id} failed ({type(error).__name__}: {error}); "
                f"{len(batches) - i - 1} later batch(es) skipped -- see {audit.table}"
            ) from error
        logs.append(log)
        print(f"  {batch_id}: {log.rows_inserted:>6,} inserted  {log.rows_updated:>6,} updated  "
              f"{log.rows_quarantined:>4,} quarantined")
    return logs