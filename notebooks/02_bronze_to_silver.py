# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze → Silver
# MAGIC
# MAGIC Casts bronze strings to typed silver tables and MERGEs them on their natural keys:
# MAGIC
# MAGIC - **prices** -> `silver.fact_daily_price` (every value change recorded in `silver.price_revision`)
# MAGIC - **tickers** -> `silver.dim_ticker` (Wikipedia constituents joined with Yahoo company profiles)
# MAGIC
# MAGIC Rows that fail validation go to a quarantine table. Re-running on the same data changes nothing
# MAGIC (0 inserted, 0 updated). Every step is logged to `ops.pipeline_execution_logs`.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog |
# MAGIC | `batch_ids` | *(empty)* | Comma-separated bronze batch ids to re-merge — a **backfill** |
# MAGIC | `from_batch` / `to_batch` | *(empty)* | Inclusive range of batch ids — a **backfill** |
# MAGIC | `start_date` / `end_date` | *(empty)* | Prices only: merge trading days in this range (`YYYY-MM-DD`); with no batch parameters, re-applies every batch for those days |
# MAGIC | `pipeline_run_id` | *(empty)* | Groups log entries; a Job passes `{{job.run_id}}` |
# MAGIC
# MAGIC **Standard run:** leave everything empty — bronze batches not yet merged are applied, oldest first.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_ids", "", "Batch ids (backfill)")
dbutils.widgets.text("from_batch", "", "From batch (backfill)")
dbutils.widgets.text("to_batch", "", "To batch (backfill)")
dbutils.widgets.text("start_date", "", "Start date YYYY-MM-DD")
dbutils.widgets.text("end_date", "", "End date YYYY-MM-DD")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline run id")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))

from marketpulse import silver_prices, silver_tickers
from marketpulse.config import PipelineConfig


def widget(name):
    return dbutils.widgets.get(name).strip() or None


def widget_list(name):
    return [v.strip() for v in dbutils.widgets.get(name).split(",") if v.strip()] or None


cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"))
run_id = widget("pipeline_run_id") or str(uuid.uuid4())  # one id groups prices and tickers in the log
batches = dict(batch_ids=widget_list("batch_ids"), from_batch=widget("from_batch"), to_batch=widget("to_batch"))

logs = silver_prices.run(
    spark, cfg, **batches,
    start_date=widget("start_date"),
    end_date=widget("end_date"),
    pipeline_run_id=run_id,
)
logs += silver_tickers.run(spark, cfg, **batches, pipeline_run_id=run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### This run in the audit log

# COMMAND ----------

if logs:
    display(
        spark.table(cfg.ops("pipeline_execution_logs"))
        .where(f"pipeline_run_id = '{run_id}'")
        .orderBy("start_time")
        .select("load_type", "batch_id", "target_table", "status", "rows_read", "rows_inserted",
                "rows_updated", "rows_quarantined", "duration_seconds", "error_message")
    )
else:
    print("Nothing to merge: every bronze batch is already in silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver contents

# COMMAND ----------

display(spark.sql(f"""
    SELECT count(*) AS rows, count(DISTINCT ticker) AS tickers,
           min(date) AS first_day, max(date) AS last_day, max(load_timestamp) AS last_loaded
    FROM {cfg.silver('fact_daily_price')}
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT batch_id, reject_reason, count(*) AS rows
    FROM {cfg.silver('quarantine_prices')}
    GROUP BY batch_id, reject_reason ORDER BY batch_id, rows DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT batch_id, count(*) AS revised_rows,
           round(avg(abs(adj_close_pct)) * 100, 4) AS avg_abs_adj_close_change_pct
    FROM {cfg.silver('price_revision')}
    GROUP BY batch_id ORDER BY batch_id
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT count(*) AS tickers,
           count_if(is_sp500_member) AS sp500_members,
           count(gics_sector) AS with_gics_sector,
           count(yf_industry) AS with_yahoo_profile,
           count(market_cap) AS with_market_cap,
           max(load_timestamp) AS last_loaded
    FROM {cfg.silver('dim_ticker')}
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT batch_id, reject_reason, count(*) AS rows
    FROM {cfg.silver('quarantine_company_info')}
    GROUP BY batch_id, reject_reason ORDER BY batch_id, rows DESC
"""))