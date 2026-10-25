# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - End-to-end Raw -> Bronze -> Silver -> Gold
# MAGIC Use this for every new uploaded batch. One run id groups all layers.
# MAGIC No Gold task runs after an upstream failure. Gold retries even if Silver
# MAGIC already succeeded on an earlier attempt. No overlapping runs are allowed.

# COMMAND ----------
for name, default in [("catalog", "workspace"), ("source_root", ""), ("pipeline_run_id", ""),
                      ("batch_ids", ""), ("from_batch", ""), ("to_batch", ""),
                      ("start_date", ""), ("end_date", "")]:
    dbutils.widgets.text(name, default)

# COMMAND ----------
import os
import sys

source_root = dbutils.widgets.get("source_root").strip() or os.path.abspath("../src")
if not os.path.isdir(source_root):
    raise ValueError("Set source_root to the absolute /Workspace/.../MarketPulse/src directory")
sys.path.insert(0, source_root)

from marketpulse.config import PipelineConfig
from marketpulse import pipeline

def widget(name):
    return dbutils.widgets.get(name).strip() or None

cfg = PipelineConfig(catalog=widget("catalog"))
logs = pipeline.run(
    spark, cfg, batch_ids=[v.strip() for v in widget("batch_ids").split(",") if v.strip()] if widget("batch_ids") else None,
    from_batch=widget("from_batch"), to_batch=widget("to_batch"),
    start_date=widget("start_date"), end_date=widget("end_date"), pipeline_run_id=widget("pipeline_run_id"),
)
run_id = logs[-1].pipeline_run_id
print(f"SUCCESS: all layers and Gold validation finished. Run id: {run_id}")

# COMMAND ----------
display(spark.sql(f"""
    SELECT layer, target_table, status, rows_read, rows_inserted, rows_updated,
           rows_deleted, rows_aggregated, duration_seconds
    FROM {cfg.ops('pipeline_execution_logs')} WHERE pipeline_run_id = :run_id
    ORDER BY start_time
""", args={"run_id": run_id}))
