# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver to Gold
# MAGIC Run this first for Phase 3 when Bronze/Silver are already populated.
# MAGIC It creates the Gold schema, migrates audit metrics, builds the star schema,
# MAGIC and validates it. Run on Serverless. Refresh Power BI only after SUCCESS.
# MAGIC All rolling windows use full Silver history, including backfills/corrections.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("source_root", "", "Absolute src directory (empty in Git folder)")
dbutils.widgets.text("pipeline_run_id", "", "Run id (optional)")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental", "backfill"])

# COMMAND ----------
import os
import sys

source_root = dbutils.widgets.get("source_root").strip() or os.path.abspath("../src")
if not os.path.isdir(source_root):
    raise ValueError("Set source_root to the absolute /Workspace/.../MarketPulse/src directory")
sys.path.insert(0, source_root)

from marketpulse.config import PipelineConfig
from marketpulse import gold

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"))
logs = gold.run(spark, cfg, pipeline_run_id=dbutils.widgets.get("pipeline_run_id") or None,
                load_type=dbutils.widgets.get("load_type"))
run_id = logs[0].pipeline_run_id
print(f"SUCCESS. Gold is ready for Power BI. Run id: {run_id}")

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {cfg.ops('pipeline_execution_logs')} WHERE pipeline_run_id = :run_id", args={"run_id": run_id})
        .select("layer", "target_table", "status", "rows_read", "rows_inserted", "rows_updated",
                "rows_deleted", "rows_aggregated", "duration_seconds").orderBy("target_table"))

# COMMAND ----------
display(spark.sql(f"""
    SELECT t.ticker, d.calendar_date, f.close, f.ma_7, f.ma_30,
           f.daily_return, f.volatility_30, f.relative_volume
    FROM {cfg.gold('fact_daily_market')} f
    JOIN {cfg.gold('dim_ticker')} t USING (ticker_key)
    JOIN {cfg.gold('dim_date')} d USING (date_key)
    WHERE t.ticker = 'AAPL' ORDER BY d.calendar_date DESC LIMIT 35
"""))

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {cfg.gold('vw_data_quality')} ORDER BY rejected_rows DESC"))
