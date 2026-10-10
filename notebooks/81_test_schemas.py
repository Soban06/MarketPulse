# Databricks notebook source
# MAGIC %md
# MAGIC # 81 · Test: schemas
# MAGIC
# MAGIC Checks `marketpulse.schemas` against Databricks and against real landed data:
# MAGIC every table can be created from its spec with the declared types and comments, and every
# MAGIC raw file reads cleanly with its explicit schema (no inference, no rescued columns).
# MAGIC Tables are created in throwaway `bronze_test` / `silver_test` schemas.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Catalog to test in |
# MAGIC | `cleanup` | `true` | Drop the test schemas at the end |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("cleanup", "true", ["true", "false"], "Drop test schemas afterwards")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from pyspark.sql import functions as F

from marketpulse import schemas as s
from marketpulse.config import PipelineConfig

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"),
                     bronze_schema="bronze_test", silver_schema="silver_test")
for schema in (cfg.bronze_schema, cfg.silver_schema):
    spark.sql(f"DROP SCHEMA IF EXISTS {cfg.catalog}.{schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {cfg.catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Every table is created exactly as specified

# COMMAND ----------

s.create_all_tables(spark, cfg)

for spec in s.ALL_TABLES:
    table = spec.full_name(cfg)
    actual = {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}
    expected = {f.name: f.dataType.simpleString() for f in spec.schema.fields}
    assert actual == expected, f"{table}: {actual} != {expected}"
    comments = spark.sql(f"DESCRIBE TABLE {table}").where("col_name NOT LIKE '#%' AND col_name <> ''")
    assert comments.where("comment IS NULL OR comment = ''").count() == 0, f"{table} has uncommented columns"
    print(f"ok  {table:<45} {len(expected)} columns, key {spec.key}")
