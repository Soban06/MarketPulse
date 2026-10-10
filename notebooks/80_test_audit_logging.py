# Databricks notebook source
# MAGIC %md
# MAGIC # 80 · Test: audit logging
# MAGIC
# MAGIC Exercises `marketpulse.audit` end to end against a throwaway `ops_test` schema, so the real
# MAGIC `ops.pipeline_execution_logs` is never touched. Each check asserts, so **Run all** either
# MAGIC finishes with "All audit logging checks passed" or stops at the first broken behaviour.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Catalog to test in |
# MAGIC | `cleanup` | `true` | Drop the `ops_test` schema at the end (set `false` to inspect it in Catalog) |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("cleanup", "true", ["true", "false"], "Drop ops_test afterwards")

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from marketpulse.audit import EXECUTION_LOG_TABLE, AuditLogger
from marketpulse.config import PipelineConfig

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"), ops_schema="ops_test")
spark.sql(f"DROP SCHEMA IF EXISTS {cfg.catalog}.{cfg.ops_schema} CASCADE")
spark.sql(f"CREATE SCHEMA {cfg.catalog}.{cfg.ops_schema}")

audit = AuditLogger(spark, cfg)
log_table = cfg.ops(EXECUTION_LOG_TABLE)
scratch = cfg.ops("audit_scratch")
spark.sql(f"CREATE TABLE {scratch} (id INT, value STRING)")
print(f"log table: {log_table}\nscratch:   {scratch}\nrun id:    {audit.pipeline_run_id}")


def merge_into_scratch(rows):
    values = ", ".join(f"({i}, '{v}')" for i, v in rows)
    spark.sql(f"""
        MERGE INTO {scratch} t
        USING (SELECT * FROM VALUES {values} AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED AND t.value <> s.value THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def run_merge_step(batch_id, rows, description):
    with audit.step("bronze_to_silver", load_type="incremental", batch_id=batch_id,
                    parameter=description, target_table=scratch) as log:
        merge_into_scratch(rows)
        log.record_delta_metrics()
    return log

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Successful steps record Delta MERGE counts — including a no-change re-run

# COMMAND ----------

first = run_merge_step("test_batch_1", [(1, "a"), (2, "b"), (3, "c")], "insert 3 new rows")
assert (first.status, first.rows_inserted, first.rows_updated) == ("SUCCESS", 3, 0), first

second = run_merge_step("test_batch_1", [(1, "a"), (2, "B"), (3, "c")], "change 1 value")
assert (second.rows_inserted, second.rows_updated) == (0, 1), second

third = run_merge_step("test_batch_1", [(1, "a"), (2, "B"), (3, "c")], "identical re-run")
assert (third.rows_inserted, third.rows_updated) == (0, 0), third

print("inserted/updated:", [(s.rows_inserted, s.rows_updated) for s in (first, second, third)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · A failing step is logged as FAILED and the error still propagates

# COMMAND ----------

try:
    with audit.step("bronze_to_silver", load_type="incremental", batch_id="test_batch_2",
                    parameter="deliberate failure", target_table=scratch):
        raise RuntimeError("deliberate failure for testing")
    raise AssertionError("the error should have propagated out of audit.step")
except RuntimeError:
    pass

failed = spark.table(log_table).where("batch_id = 'test_batch_2'").first()
assert failed["status"] == "FAILED", failed
assert "deliberate failure for testing" in failed["error_message"], failed
assert failed["end_time"] is not None
print("FAILED row:", failed["error_message"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Batch tracking: only fully successful batches count as completed

# COMMAND ----------

completed = audit.completed_batches("bronze_to_silver")
pending = audit.pending_batches("bronze_to_silver", ["test_batch_3", "test_batch_1", "test_batch_2"])
assert completed == {"test_batch_1"}, completed
assert pending == ["test_batch_2", "test_batch_3"], pending
print("completed:", completed, "| pending:", pending)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · The laptop extraction step is logged from a real manifest, exactly once

# COMMAND ----------

batches = sorted(e.name.rstrip("/") for e in dbutils.fs.ls(cfg.landing_root))
assert batches, f"No batches in {cfg.landing_root} -- upload one with extract/upload.py first"
batch_path = cfg.batch_path(batches[-1])
with open(f"{batch_path}/manifest.json") as f:
    manifest = json.load(f)

audit.log_source_to_raw(manifest, parameter=batch_path)
audit.log_source_to_raw(manifest, parameter=batch_path)  # second call must be a no-op

extract_rows = spark.table(log_table).where("layer = 'source_to_raw'").collect()
assert len(extract_rows) == 1, extract_rows
assert extract_rows[0]["rows_inserted"] == manifest["prices"]["rows"]
print(f"source_to_raw logged once for {manifest['run_id']}: {extract_rows[0]['rows_inserted']:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 · Skipped steps are recorded with their reason

# COMMAND ----------

audit.log_skipped("bronze_to_silver", load_type="incremental", batch_id="test_batch_3",
                  parameter="batch_id=test_batch_3", target_table=scratch,
                  reason="earlier batch test_batch_2 failed")
skipped = spark.table(log_table).where("batch_id = 'test_batch_3'").first()
assert skipped["status"] == "SKIPPED", skipped
assert "test_batch_3" not in audit.completed_batches("bronze_to_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Result

# COMMAND ----------

assert spark.table(log_table).where("status = 'RUNNING'").count() == 0, "a step was left RUNNING"
display(
    spark.table(log_table)
    .orderBy("start_time")
    .select("layer", "load_type", "batch_id", "parameter", "status", "rows_read",
            "rows_inserted", "rows_updated", "duration_seconds", "error_message")
)
print("All audit logging checks passed")

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "true":
    spark.sql(f"DROP SCHEMA {cfg.catalog}.{cfg.ops_schema} CASCADE")
    print(f"Dropped {cfg.catalog}.{cfg.ops_schema}")
