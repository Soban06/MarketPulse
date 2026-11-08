# Databricks notebook source
# MAGIC %md
# MAGIC # 93 - Phase 3 evidence and optional optimization
# MAGIC Run after 03 (or 04) succeeds. Export with outputs. Take screenshots of
# MAGIC the actual Gold query profile, job success and Power BI model. This notebook
# MAGIC reports measurements; it does not invent cloud billing or mark tests passed.
# MAGIC `optimize=true` changes the Gold daily fact's layout and consumes compute.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("source_root", "")
dbutils.widgets.text("pipeline_run_id", "", "Normal incremental job run id to measure")
dbutils.widgets.dropdown("optimize", "false", ["false", "true"])

# COMMAND ----------
import os
import sys
import json
import time
sys.path.insert(0, dbutils.widgets.get("source_root").strip() or os.path.abspath("../src"))
from marketpulse.config import PipelineConfig
from marketpulse import gold
from marketpulse.gold_sql import monthly_query
cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"))
counts = gold.validate(spark, cfg)
display(spark.createDataFrame([counts]))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Gold aggregation DAG
# MAGIC The following is the actual monthly Gold aggregation SELECT. Open **See performance**
# MAGIC / **Query Profile** under this cell after it runs. Save its graph screenshot.
# MAGIC Explain only operators present: scan, aggregate, exchange/shuffle, etc.
# MAGIC Free Edition has no Spark UI; obtain instructor acceptance of Query Profile.

# COMMAND ----------
aggregation = monthly_query(cfg.gold("fact_daily_market"))
display(spark.sql(aggregation))

# COMMAND ----------
print("PHYSICAL PLAN (planned execution, not proof of actual runtime operators)")
print(spark.sql("EXPLAIN FORMATTED " + aggregation).first()[0])

# COMMAND ----------
benchmark = f"""
    SELECT ticker_key, count(*) AS observations, sum(volume) AS volume,
           avg(abs_daily_return) AS avg_abs_return
    FROM {cfg.gold('fact_daily_market')}
    WHERE trade_date >= date_sub((SELECT max(trade_date) FROM {cfg.gold('fact_daily_market')}), 90)
    GROUP BY ticker_key
"""

def benchmark_three_runs():
    values = []
    for _ in range(3):
        started = time.perf_counter()
        spark.sql(benchmark).collect()
        values.append(round(time.perf_counter()-started, 3))
    return values

before = benchmark_three_runs()
print("Before (seconds; caching may affect repeated queries):", before)
display(spark.sql(f"DESCRIBE DETAIL {cfg.gold('fact_daily_market')}"))

# COMMAND ----------
after = None
if dbutils.widgets.get("optimize") == "true":
    spark.sql(f"ALTER TABLE {cfg.gold('fact_daily_market')} CLUSTER BY (trade_date, ticker_key)")
    display(spark.sql(f"OPTIMIZE {cfg.gold('fact_daily_market')}"))
    after = benchmark_three_runs()
    print("After (seconds; caching may affect repeated queries):", after)
    display(spark.sql(f"DESCRIBE DETAIL {cfg.gold('fact_daily_market')}"))
else:
    print("Optimization was not requested. Set optimize=true only when collecting the optional evidence.")

# COMMAND ----------
run_id = dbutils.widgets.get("pipeline_run_id").strip()
timings = None
if run_id:
    row = spark.sql(f"""
        SELECT min(start_time) AS started_at, max(end_time) AS finished_at,
               timestampdiff(MICROSECOND, min(start_time), max(end_time))/1000000.0 AS wall_seconds,
               sum(duration_seconds) AS summed_step_seconds,
               count_if(status <> 'SUCCESS') AS unsuccessful_steps,
               count_if(layer='silver_to_gold' AND parameter LIKE 'phase=publication%'
                        AND status='SUCCESS') AS successful_publications
        FROM {cfg.ops('pipeline_execution_logs')}
        WHERE pipeline_run_id=:run_id AND layer <> 'source_to_raw'
    """, args={"run_id":run_id}).first().asDict()
    timings = {k:str(v) if hasattr(v, 'isoformat') else v for k,v in row.items()}
    display(spark.sql(f"SELECT * FROM {cfg.ops('pipeline_execution_logs')} WHERE pipeline_run_id=:run_id ORDER BY start_time",
                      args={"run_id":run_id}))
    if row['started_at'] is None or row['unsuccessful_steps'] or not row['successful_publications']:
        raise ValueError("Choose a successful completed pipeline run; this run is missing or incomplete")
print(json.dumps({"gold_row_counts":counts, "incremental_run_id":run_id or None,
                  "spark_timings":timings, "benchmark_before_seconds":before,
                  "benchmark_after_seconds":after}, indent=2, default=str))
print("Wall/step time is NOT billed DBUs. Copy the real values into docs/report/evidence.json.")
