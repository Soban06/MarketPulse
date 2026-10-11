"""
Bronze -> Silver for prices: bronze.prices -> silver.fact_daily_price.

For each batch, oldest first:

  1. stage       try_cast every bronze string to its silver type, round prices to
                 6 decimals (Yahoo's float noise would otherwise look like revisions)
                 and work out a reject_reason for rows that can't be trusted
  2. quarantine  rejected rows -> silver.quarantine_prices, replaced per batch
  3. revisions   rows whose values differ from what silver already holds ->
                 silver.price_revision (old and new values)
  4. merge       MERGE into silver.fact_daily_price on (ticker, date): new days are
                 inserted, changed days updated, identical days left alone

A row is only ever updated by a batch at least as new as the one that last wrote
it (batch ids start with their UTC timestamp, so they sort by time). Backfilling
an old batch can therefore fill gaps but never overwrite newer prices.

Batch selection:
    batch_ids / from_batch..to_batch    backfill those bronze batches
    start_date / end_date               only merge trading days in that range; with no
                                        batch parameters this re-applies every batch
    nothing                             bronze batches not yet merged, per the audit log

Batches must apply in order, so the first failure stops the run: it is logged as
FAILED and every later batch as SKIPPED.
"""

import re
from datetime import date

from marketpulse.audit import AuditLogger
from marketpulse.schemas import (
    BRONZE_PRICES,
    SILVER_FACT_DAILY_PRICE,
    SILVER_PRICE_REVISION,
    SILVER_QUARANTINE_PRICES,
    create_table,
)

LAYER = "bronze_to_silver"
PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close"]
VALUE_COLUMNS = PRICE_COLUMNS + ["volume"]
_BATCH_ID = re.compile(r"^\d{8}T\d{6}Z_[a-z]+$")


class SilverLoadError(RuntimeError):
    """A batch failed; it and the batches skipped after it are in the audit log."""


def _load_type(batch_id: str) -> str:
    mode = batch_id.rsplit("_", 1)[-1]
    return mode if mode in ("full", "incremental") else "incremental"


def _check_batch_id(batch_id: str) -> str:
    # batch ids are interpolated into SQL, so only accept the extractor's format
    if not _BATCH_ID.match(batch_id):
        raise ValueError(f"Not a valid batch id: {batch_id!r}")
    return batch_id


# ------------------------------------------------------------------- selection

def bronze_batches(spark, cfg) -> list[str]:
    rows = spark.sql(f"SELECT DISTINCT batch_id FROM {BRONZE_PRICES.full_name(cfg)}").collect()
    return sorted(r["batch_id"] for r in rows if r["batch_id"])


def select_batches(spark, cfg, audit: AuditLogger, batch_ids=None, from_batch=None,
                   to_batch=None, date_range: bool = False) -> tuple[list[str], bool]:
    """Return (batches to merge, is_backfill)."""
    available = bronze_batches(spark, cfg)
    if batch_ids:
        missing = [b for b in batch_ids if b not in available]
        if missing:
            raise ValueError(f"Batch(es) not in {BRONZE_PRICES.full_name(cfg)}: {missing}")
        return sorted(batch_ids), True
    if from_batch or to_batch:
        return [b for b in available
                if (not from_batch or b >= from_batch) and (not to_batch or b <= to_batch)], True
    if date_range:
        return available, True
    target = SILVER_FACT_DAILY_PRICE.full_name(cfg)
    return audit.pending_batches(LAYER, available, target_table=target), False


# --------------------------------------------------------------------- staging

def stage_batch(spark, cfg, batch_id: str):
    """Typed rows of one bronze batch plus reject_reason (null = row is good). Registered as `stg_prices`."""
    numeric_checks = [
        f"CASE WHEN {c} IS NOT NULL AND try_cast({c} AS DOUBLE) IS NULL THEN 'unparseable {c}' END"
        for c in VALUE_COLUMNS
    ]
    reasons = ",\n               ".join([
        "CASE WHEN ticker IS NULL OR trim(ticker) = '' THEN 'missing ticker' END",
        "CASE WHEN date IS NULL THEN 'missing date' "
        "WHEN try_cast(date AS DATE) IS NULL THEN 'unparseable date' END",
        *numeric_checks,
        "CASE WHEN close IS NULL THEN 'missing close' END",
    ])
    typed_prices = ",\n               ".join(
        f"round(try_cast({c} AS DOUBLE), 6) AS {c}_v" for c in PRICE_COLUMNS
    )
    staged = spark.sql(f"""
        WITH typed AS (
            SELECT b.*,
                   trim(ticker)                                   AS ticker_v,
                   try_cast(date AS DATE)                         AS date_v,
                   {typed_prices},
                   try_cast(try_cast(volume AS DOUBLE) AS BIGINT) AS volume_v,
                   nullif(concat_ws('; ',
                       {reasons}
                   ), '')                                         AS invalid_reason
            FROM {BRONZE_PRICES.full_name(cfg)} b
            WHERE batch_id = '{_check_batch_id(batch_id)}'
        )
        SELECT *,
               coalesce(invalid_reason,
                        CASE WHEN row_number() OVER (PARTITION BY ticker_v, date_v
                                                     ORDER BY invalid_reason IS NULL DESC, source_file) > 1
                             THEN 'duplicate ticker and date in batch' END) AS reject_reason
        FROM typed
    """)
    staged.createOrReplaceTempView("stg_prices")
    return staged


def _good_rows_view(start_date: str | None, end_date: str | None):
    """`stg_good_prices`: accepted rows in silver shape, limited to the date range if given."""
    window = ""
    if start_date:
        window += f" AND date_v >= DATE'{date.fromisoformat(start_date)}'"
    if end_date:
        window += f" AND date_v <= DATE'{date.fromisoformat(end_date)}'"
    return f"""
        CREATE OR REPLACE TEMP VIEW stg_good_prices AS
        SELECT ticker_v AS ticker, date_v AS date,
               {", ".join(f"{c}_v AS {c}" for c in VALUE_COLUMNS)},
               batch_id
        FROM stg_prices
        WHERE reject_reason IS NULL{window}
    """


# -------------------------------------------------------------------- one batch

def merge_batch(spark, cfg, audit: AuditLogger, batch_id: str, load_type: str,
                start_date: str | None, end_date: str | None):
    fact = SILVER_FACT_DAILY_PRICE.full_name(cfg)
    revisions = SILVER_PRICE_REVISION.full_name(cfg)
    quarantine = SILVER_QUARANTINE_PRICES.full_name(cfg)
    parameter = f"batch_id={batch_id}"
    if start_date or end_date:
        parameter += f", start_date={start_date or ''}, end_date={end_date or ''}"

    staged = stage_batch(spark, cfg, batch_id)
    spark.sql(_good_rows_view(start_date, end_date))

    # Quarantine is always rebuilt for the whole batch, so re-runs never duplicate rejects.
    bronze_columns = [c for c in BRONZE_PRICES.schema.fieldNames() if c != "load_timestamp"]
    with audit.step(LAYER, load_type=load_type, batch_id=batch_id,
                    parameter=parameter, target_table=quarantine) as log:
        (
            staged.where("reject_reason IS NOT NULL")
            .selectExpr(*bronze_columns, "current_timestamp() AS load_timestamp", "reject_reason")
            .select(*SILVER_QUARANTINE_PRICES.schema.fieldNames())
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", f"batch_id = '{batch_id}'")
            .saveAsTable(quarantine)
        )
        log.record_delta_metrics()
        quarantined = log.rows_inserted
        log.record(rows_read=staged.count(), rows_quarantined=quarantined)

    changed = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in VALUE_COLUMNS)
    newer = "s.batch_id >= t.batch_id"

    with audit.step(LAYER, load_type=load_type, batch_id=batch_id,
                    parameter=parameter, target_table=revisions) as log:
        spark.sql(f"""
            MERGE INTO {revisions} r
            USING (
                SELECT s.ticker, s.date,
                       t.close AS old_close, s.close AS new_close,
                       t.adj_close AS old_adj_close, s.adj_close AS new_adj_close,
                       s.adj_close / nullif(t.adj_close, 0) - 1 AS adj_close_pct,
                       t.volume AS old_volume, s.volume AS new_volume,
                       t.batch_id AS previous_batch_id, s.batch_id
                FROM stg_good_prices s
                JOIN {fact} t ON t.ticker = s.ticker AND t.date = s.date
                WHERE {newer} AND ({changed})
            ) c
            ON r.ticker = c.ticker AND r.date = c.date AND r.batch_id = c.batch_id
            WHEN NOT MATCHED THEN INSERT
                (ticker, date, old_close, new_close, old_adj_close, new_adj_close, adj_close_pct,
                 old_volume, new_volume, previous_batch_id, batch_id, load_timestamp)
            VALUES
                (c.ticker, c.date, c.old_close, c.new_close, c.old_adj_close, c.new_adj_close,
                 c.adj_close_pct, c.old_volume, c.new_volume, c.previous_batch_id, c.batch_id,
                 current_timestamp())
        """)
        log.record_delta_metrics()

    with audit.step(LAYER, load_type=load_type, batch_id=batch_id,
                    parameter=parameter, target_table=fact) as log:
        spark.sql(f"""
            MERGE INTO {fact} t
            USING stg_good_prices s
            ON t.ticker = s.ticker AND t.date = s.date
            WHEN MATCHED AND {newer} AND ({changed}) THEN UPDATE SET
                {", ".join(f"{c} = s.{c}" for c in VALUE_COLUMNS)},
                batch_id = s.batch_id,
                load_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                (ticker, date, {", ".join(VALUE_COLUMNS)}, batch_id, load_timestamp)
            VALUES
                (s.ticker, s.date, {", ".join("s." + c for c in VALUE_COLUMNS)}, s.batch_id,
                 current_timestamp())
        """)
        log.record_delta_metrics()
        log.record(rows_quarantined=quarantined)
    return log


# ------------------------------------------------------------------------- run

def run(spark, cfg, *, batch_ids=None, from_batch=None, to_batch=None,
        start_date=None, end_date=None, pipeline_run_id=None) -> list:
    """Merge the selected bronze batches into silver. Returns the fact-table StepLogs."""
    for value in (start_date, end_date):
        if value:
            date.fromisoformat(value)  # fail fast on a malformed date parameter
    for spec in (SILVER_FACT_DAILY_PRICE, SILVER_PRICE_REVISION, SILVER_QUARANTINE_PRICES):
        create_table(spark, cfg, spec)

    audit = AuditLogger(spark, cfg, pipeline_run_id=pipeline_run_id)
    batches, is_backfill = select_batches(
        spark, cfg, audit, batch_ids, from_batch, to_batch,
        date_range=bool(start_date or end_date),
    )
    dates = f", dates {start_date or 'start'} to {end_date or 'end'}" if (start_date or end_date) else ""
    print(f"{LAYER} prices: {len(batches)} batch(es), "
          f"{'backfill' if is_backfill else 'new batches only'}{dates}")

    logs = []
    for i, batch_id in enumerate(batches):
        load_type = "backfill" if is_backfill else _load_type(batch_id)
        try:
            log = merge_batch(spark, cfg, audit, batch_id, load_type, start_date, end_date)
        except Exception as error:
            for later in batches[i + 1:]:
                audit.log_skipped(LAYER, load_type="backfill" if is_backfill else _load_type(later),
                                  batch_id=later, parameter=f"batch_id={later}",
                                  target_table=SILVER_FACT_DAILY_PRICE.full_name(cfg),
                                  reason=f"earlier batch {batch_id} failed")
            raise SilverLoadError(
                f"{batch_id} failed ({type(error).__name__}: {error}); "
                f"{len(batches) - i - 1} later batch(es) skipped -- see {audit.table}"
            ) from error
        logs.append(log)
        print(f"  {batch_id}: {log.rows_inserted:>10,} inserted  {log.rows_updated:>8,} updated  "
              f"{log.rows_quarantined:>6,} quarantined")
    return logs
