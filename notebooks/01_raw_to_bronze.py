# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Raw → Bronze
# MAGIC
# MAGIC Loads landed JSON batches from `/Volumes/<catalog>/raw/landing/<batch_id>/` into bronze Delta tables
# MAGIC with explicit schemas. Each batch is written with `replaceWhere batch_id = …`, so re-running any batch
# MAGIC replaces its rows rather than duplicating them. Every source × batch is logged to `ops.pipeline_execution_logs`.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog |
# MAGIC | `batch_ids` | *(empty)* | Comma-separated batch ids to (re)load — a **backfill** |
# MAGIC | `from_batch` / `to_batch` | *(empty)* | Inclusive range of batch ids to (re)load — a **backfill** |
# MAGIC | `sources` | *(empty = all)* | Comma-separated sources, e.g. `prices` |
# MAGIC | `pipeline_run_id` | *(empty)* | Groups log entries; a Job passes `{{job.run_id}}` |
# MAGIC
# MAGIC **Standard run:** leave the batch parameters empty — every landed batch not yet loaded is picked up, oldest first.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_ids", "", "Batch ids (backfill)")
dbutils.widgets.text("from_batch", "", "From batch (backfill)")
dbutils.widgets.text("to_batch", "", "To batch (backfill)")
dbutils.widgets.text("sources", "", "Sources (empty = all)")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline run id")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from marketpulse import raw_to_bronze
from marketpulse.config import PipelineConfig


def widget_list(name):
    return [v.strip() for v in dbutils.widgets.get(name).split(",") if v.strip()] or None


cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"))
logs = raw_to_bronze.run(
    spark,
    cfg,
    batch_ids=widget_list("batch_ids"),
    from_batch=dbutils.widgets.get("from_batch") or None,
    to_batch=dbutils.widgets.get("to_batch") or None,
    sources=widget_list("sources"),
    pipeline_run_id=dbutils.widgets.get("pipeline_run_id") or None,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### This run in the audit log

# COMMAND ----------

if logs:
    display(
        spark.table(cfg.ops("pipeline_execution_logs"))
        .where(f"pipeline_run_id = '{logs[0].pipeline_run_id}'")
        .orderBy("start_time")
        .select("layer", "load_type", "batch_id", "target_table", "status",
                "rows_read", "rows_inserted", "duration_seconds", "error_message")
    )
else:
    print("Nothing to load: every landed batch is already in bronze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze contents by batch

# COMMAND ----------

for name, source in raw_to_bronze.SOURCES.items():
    rescued = "count(_rescued_data)" if source.rescued else "CAST(NULL AS BIGINT)"
    display(spark.sql(f"""
        SELECT '{name}' AS source, batch_id, count(*) AS rows,
               {rescued} AS rows_with_rescued_data, min(load_timestamp) AS loaded_at
        FROM {source.table.full_name(cfg)}
        GROUP BY batch_id ORDER BY batch_id
    """))
