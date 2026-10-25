# Databricks notebook source
# MAGIC %md
# MAGIC # 91 · Demo: schema drift
# MAGIC
# MAGIC Shows what happens when the source changes shape without warning. The batch in
# MAGIC `tests/fixtures/drift/20260101T000000Z_incremental/` is hand-made:
# MAGIC
# MAGIC | Drift | In the fixture | Expected handling |
# MAGIC |---|---|---|
# MAGIC | **New column** | every row has an `Exchange` field the schema doesn't know | captured in bronze `_rescued_data`, nothing lost, nothing fails |
# MAGIC | **Type change** | `Volume` is `"abc"` on 2026-01-05 | bronze stores it as a string; silver can't cast it and quarantines that row |
# MAGIC
# MAGIC Runs the real pipeline code against throwaway `drift_demo_*` schemas, so the real tables
# MAGIC are never touched.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Catalog to run the demo in |
# MAGIC | `cleanup` | `true` | Drop the demo schemas at the end |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("cleanup", "true", ["true", "false"], "Drop demo schemas afterwards")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import shutil
import sys

sys.path.insert(0, os.path.abspath("../src"))

from pyspark.sql import functions as F

from marketpulse import raw_to_bronze, silver_prices
from marketpulse.config import PipelineConfig

BATCH = "20260101T000000Z_incremental"
FIXTURE = os.path.abspath(f"../tests/fixtures/drift/{BATCH}")

cfg = PipelineConfig(
    catalog=dbutils.widgets.get("catalog"),
    raw_schema="drift_demo_raw",
    bronze_schema="drift_demo_bronze",
    silver_schema="drift_demo_silver",
    ops_schema="drift_demo_ops",
    gold_schema="drift_demo_gold",
)
for schema in cfg.schemas:
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
spark.sql(f"CREATE VOLUME {cfg.landing_volume_name}")

# Land the fixture batch in the demo Volume as upload.py would: data files first, manifest last.
# Plain file copies only -- Volumes don't accept copying permissions/timestamps (copytree's default).
fixture_files = sorted(
    os.path.relpath(os.path.join(folder, name), FIXTURE)
    for folder, _, names in os.walk(FIXTURE) for name in names
)
for rel in sorted(fixture_files, key=lambda r: r == "manifest.json"):
    target = os.path.join(cfg.batch_path(BATCH), rel)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    shutil.copyfile(os.path.join(FIXTURE, rel), target)
print(f"landed {fixture_files}\n   -> {cfg.batch_path(BATCH)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Raw → Bronze: the drifted batch loads without failing

# COMMAND ----------

raw_to_bronze.run(spark, cfg, batch_ids=[BATCH], sources=["prices"])

bronze = spark.table(cfg.bronze("prices"))
display(bronze.select("ticker", "date", "close", "volume", "_rescued_data", "batch_id", "load_timestamp"))

rows = bronze.count()
rescued = bronze.where(F.col("_rescued_data").isNotNull()).count()
assert rows == 3, f"expected all 3 fixture rows in bronze, got {rows}"
assert rescued == 3, f"expected the unknown Exchange field rescued on all 3 rows, got {rescued}"
assert bronze.where("_rescued_data LIKE '%Exchange%'").count() == 3
assert bronze.where("volume = 'abc'").count() == 1, "the bad Volume should be kept as the string 'abc'"
print("Bronze: all 3 rows loaded; Exchange captured in _rescued_data; Volume 'abc' kept as a string")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Drift is visible per batch
# MAGIC
# MAGIC A non-zero `rows_with_rescued_data` for a batch is the signal that the source schema changed.

# COMMAND ----------

display(spark.sql(f"""
    SELECT batch_id, count(*) AS rows, count(_rescued_data) AS rows_with_rescued_data,
           collect_set(map_keys(from_json(_rescued_data, 'map<string,string>'))) AS rescued_fields
    FROM {cfg.bronze('prices')}
    GROUP BY batch_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Bronze → Silver: the bad value is quarantined, the rest merge
# MAGIC
# MAGIC `try_cast(volume)` returns null for `"abc"`, so that row is rejected with a reason instead of
# MAGIC failing the batch. The other two rows still have the unknown `Exchange` field, but every column
# MAGIC silver needs is valid, so they merge; the extra field stays on record in bronze `_rescued_data`.

# COMMAND ----------

silver_prices.run(spark, cfg, batch_ids=[BATCH])

fact = spark.table(cfg.silver("fact_daily_price"))
quarantine = spark.table(cfg.silver("quarantine_prices"))
display(fact.select("ticker", "date", "close", "volume", "batch_id", "load_timestamp").orderBy("date"))
display(quarantine.select("ticker", "date", "volume", "reject_reason", "_rescued_data"))

assert fact.count() == 2, f"expected the 2 valid rows in silver, got {fact.count()}"
assert quarantine.count() == 1, f"expected 1 quarantined row, got {quarantine.count()}"
assert quarantine.first()["reject_reason"] == "unparseable volume", quarantine.first()["reject_reason"]
print("Silver: 2 rows merged, 1 row quarantined with reason 'unparseable volume'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · Every step of the drifted batch is in the audit log

# COMMAND ----------

display(
    spark.table(cfg.ops("pipeline_execution_logs"))
    .orderBy("start_time")
    .select("layer", "load_type", "target_table", "status", "rows_read",
            "rows_inserted", "rows_quarantined", "error_message")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "true":
    for schema in cfg.schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    print("Dropped the drift_demo_* schemas")
