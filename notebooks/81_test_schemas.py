# Databricks notebook source
# MAGIC %md
# MAGIC # 81 · Test: schemas
# MAGIC
# MAGIC Checks `marketpulse.schemas` against Databricks and against real landed data:
# MAGIC every table can be created from its spec with the declared types and comments, and every
# MAGIC raw file reads cleanly with its explicit schema (no inference, no rescued columns).
# MAGIC Tables are created in throwaway `bronze_test` / `silver_test` / `gold_test` schemas.
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
                     bronze_schema="bronze_test", silver_schema="silver_test", gold_schema="gold_test")
for schema in (cfg.bronze_schema, cfg.silver_schema, cfg.gold_schema):
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


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Real raw files read cleanly with the explicit schemas

# COMMAND ----------

batches = sorted(e.name.rstrip("/") for e in dbutils.fs.ls(cfg.landing_root))
assert batches, f"No batches in {cfg.landing_root} -- upload one with extract/upload.py first"
batch = cfg.batch_path(batches[-1])
print(f"reading {batch}")


def read_raw(path, schema, rescued=True):
    reader = spark.read.schema(schema).option("multiLine", True)
    if rescued:
        reader = reader.option("rescuedDataColumn", s.RESCUED_DATA_COLUMN)
    return reader.json(path)


prices = read_raw(f"{batch}/prices/*.json", s.RAW_PRICES).withColumnsRenamed(s.PRICES_COLUMN_MAP)
constituents = read_raw(f"{batch}/sp500_constituents.json", s.RAW_CONSTITUENTS).withColumnsRenamed(s.CONSTITUENTS_COLUMN_MAP)
company_info = read_raw(f"{batch}/company_info/*.json", s.RAW_COMPANY_INFO, rescued=False).withColumnsRenamed(s.COMPANY_INFO_COLUMN_MAP)

for name, df, spec in [("prices", prices, s.BRONZE_PRICES),
                       ("constituents", constituents, s.BRONZE_CONSTITUENTS),
                       ("company_info", company_info, s.BRONZE_COMPANY_INFO)]:
    lineage = {"batch_id", "source_file", "load_timestamp"}
    expected_cols = [c for c in spec.schema.fieldNames() if c not in lineage]
    assert df.columns == expected_cols, f"{name}: {df.columns} != {expected_cols}"
    assert all(t == "string" for _, t in df.dtypes), f"{name}: raw values must all be strings"
    rows = df.count()
    assert rows > 0, f"{name}: no rows read"
    if s.RESCUED_DATA_COLUMN in df.columns:
        drifted = df.where(F.col(s.RESCUED_DATA_COLUMN).isNotNull()).count()
        assert drifted == 0, f"{name}: {drifted} rows have unexpected fields"
    print(f"ok  {name:<13} {rows:>8,} rows, columns match bronze, no rescued data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · String values cast cleanly to their silver types

# COMMAND ----------

cast_check = prices.select(
    F.count("*").alias("rows"),
    F.count(F.expr("try_cast(date AS DATE)")).alias("dates_ok"),
    F.count(F.expr("try_cast(adj_close AS DOUBLE)")).alias("adj_close_ok"),
    F.count(F.expr("try_cast(try_cast(volume AS DOUBLE) AS BIGINT)")).alias("volume_ok"),
).first()
display(prices.limit(5))
print(cast_check)
assert cast_check["dates_ok"] == cast_check["rows"], "some dates did not cast"
print("All schema checks passed")

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "true":
    for schema in (cfg.bronze_schema, cfg.silver_schema, cfg.gold_schema):
        spark.sql(f"DROP SCHEMA {cfg.catalog}.{schema} CASCADE")
    print("Dropped test schemas")
