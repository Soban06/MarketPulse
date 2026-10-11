# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Demo: idempotent MERGE and backfills
# MAGIC
# MAGIC Runs against the **real** bronze and silver tables to show that re-running the pipeline on input
# MAGIC it has already processed leaves the data exactly as it was:
# MAGIC
# MAGIC 1. **Fingerprint** bronze and silver: row counts, distinct keys, a hash of every value and the latest `load_timestamp`
# MAGIC 2. **Bronze → Silver re-run:** re-MERGE every batch already in silver → 0 inserted, 0 updated, no duplicates
# MAGIC 3. **Backfill of an older period:** re-process the 2018 trading days from every batch → 0 inserted, 0 updated
# MAGIC 4. **Raw → Bronze backfill:** re-load the newest batch from the raw Volume → its rows are replaced, not duplicated
# MAGIC 5. **Fingerprint again** → identical; then show every step of this run in `ops.pipeline_execution_logs`
# MAGIC
# MAGIC Every step is an ordinary pipeline call with backfill parameters, so the run is safe to repeat.
# MAGIC It re-reads the full history, so expect a few minutes.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog |
# MAGIC | `start_date` / `end_date` | `2018-01-01` / `2018-12-31` | The older period to backfill in step 3 |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("start_date", "2018-01-01", "Backfill start date")
dbutils.widgets.text("end_date", "2018-12-31", "Backfill end date")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))

from marketpulse import raw_to_bronze, silver_prices, silver_tickers
from marketpulse.config import PipelineConfig

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"))
run_id = f"demo-{uuid.uuid4()}"  # groups every log entry this notebook writes
batches = silver_prices.bronze_batches(spark, cfg)
assert batches, "bronze.prices is empty -- run 01_raw_to_bronze and 02_bronze_to_silver first"
print(f"run id {run_id}\nbatches in bronze: {batches}")


def fingerprint():
    """Counts plus a hash of every value, so 'unchanged' means unchanged, not just the same row count."""
    fact, dim, bronze = cfg.silver("fact_daily_price"), cfg.silver("dim_ticker"), cfg.bronze("prices")

    # Sum of per-row hashes; DECIMAL(38,0) because a BIGINT sum overflows under ANSI SQL.
    def value_hash(table):
        return (f"(SELECT sum(CAST(xxhash64(struct(*)) AS DECIMAL(38,0))) "
                f"FROM (SELECT * EXCEPT (load_timestamp) FROM {table}))")

    return spark.sql(f"""
        SELECT
          (SELECT count(*) FROM {fact})                                    AS fact_rows,
          (SELECT count(*) FROM (SELECT DISTINCT ticker, date FROM {fact})) AS fact_distinct_keys,
          {value_hash(fact)}                                               AS fact_value_hash,
          (SELECT max(load_timestamp) FROM {fact})                         AS fact_last_written,
          (SELECT count(*) FROM {dim})                                     AS dim_ticker_rows,
          {value_hash(dim)}                                                AS dim_ticker_value_hash,
          (SELECT count(*) FROM {cfg.silver('quarantine_prices')})         AS quarantined_prices,
          (SELECT count(*) FROM {cfg.silver('price_revision')})            AS price_revisions,
          (SELECT count(*) FROM {bronze})                                  AS bronze_price_rows,
          {value_hash(bronze)}                                             AS bronze_price_value_hash
    """).first().asDict()


def summarise(logs):
    for log in logs:
        print(f"  {log.batch_id}  {log.target_table:<36} inserted {log.rows_inserted:>9,}  "
              f"updated {log.rows_updated:>6,}  quarantined {log.rows_quarantined or 0:>4,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Fingerprint before

# COMMAND ----------

before = fingerprint()
display(spark.createDataFrame([{"when": "before", **before}]))
assert before["fact_rows"] == before["fact_distinct_keys"], "fact_daily_price already has duplicate keys"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Bronze → Silver: re-MERGE every batch that is already in silver
# MAGIC
# MAGIC Same input, second time. The MERGE only updates rows whose values changed, so nothing is
# MAGIC inserted or updated; quarantine is rebuilt per batch, so its row count stays the same.

# COMMAND ----------

rerun = silver_prices.run(spark, cfg, batch_ids=batches, pipeline_run_id=run_id)
rerun += silver_tickers.run(spark, cfg, batch_ids=silver_tickers.bronze_batches(spark, cfg),
                            pipeline_run_id=run_id)
summarise(rerun)
assert all(log.rows_inserted == 0 and log.rows_updated == 0 for log in rerun), "re-run changed silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Backfill an older period: re-process the chosen trading days from every batch

# COMMAND ----------

start_date, end_date = dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
backfill = silver_prices.run(spark, cfg, start_date=start_date, end_date=end_date, pipeline_run_id=run_id)
summarise(backfill)
assert all(log.rows_inserted == 0 and log.rows_updated == 0 for log in backfill), "backfill changed silver"
display(spark.sql(f"""
    SELECT count(*) AS trading_rows_in_period, count(DISTINCT ticker) AS tickers,
           min(date) AS first_day, max(date) AS last_day
    FROM {cfg.silver('fact_daily_price')}
    WHERE date BETWEEN DATE'{start_date}' AND DATE'{end_date}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · Raw → Bronze backfill: re-load the newest batch from the raw Volume
# MAGIC
# MAGIC The batch is written with `replaceWhere batch_id = …`, so its rows are swapped for a fresh copy
# MAGIC (new `load_timestamp`) instead of being appended a second time.

# COMMAND ----------

newest = batches[-1]
bronze_rows_before = spark.table(cfg.bronze("prices")).where(f"batch_id = '{newest}'").count()
reload = raw_to_bronze.run(spark, cfg, batch_ids=[newest], sources=["prices"], pipeline_run_id=run_id)
bronze_rows_after = spark.table(cfg.bronze("prices")).where(f"batch_id = '{newest}'").count()
print(f"bronze.prices rows for {newest}: before {bronze_rows_before:,}, after {bronze_rows_after:,}")
assert bronze_rows_before == bronze_rows_after, "bronze backfill duplicated or lost rows"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 · Fingerprint after: identical
# MAGIC
# MAGIC Same counts, no duplicate keys, same hash of every value, and `fact_last_written` unchanged:
# MAGIC no silver row was rewritten, let alone duplicated.

# COMMAND ----------

after = fingerprint()
display(spark.createDataFrame([{"when": "before", **before}, {"when": "after", **after}]))
changed = {k: (before[k], after[k]) for k in before if before[k] != after[k]}
assert not changed, f"tables changed: {changed}"
assert after["fact_rows"] == after["fact_distinct_keys"], "duplicate keys in fact_daily_price"
print("Idempotent: bronze and silver are identical after re-running every step")

# COMMAND ----------

# MAGIC %md
# MAGIC ### This demo in the execution log
# MAGIC
# MAGIC Every step above wrote its own row: layer, the parameter it processed, start and end time,
# MAGIC status and rows inserted / updated. All are `backfill` because they were given explicit parameters.

# COMMAND ----------

display(
    spark.table(cfg.ops("pipeline_execution_logs"))
    .where(f"pipeline_run_id = '{run_id}'")
    .orderBy("start_time")
    .select("layer", "load_type", "batch_id", "parameter", "target_table", "start_time", "end_time",
            "status", "rows_read", "rows_inserted", "rows_updated", "rows_quarantined")
)
