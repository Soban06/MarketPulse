# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC Creates the Unity Catalog schemas and the raw landing Volume the MarketPulse pipeline runs on.
# MAGIC Every statement is `IF NOT EXISTS`, so re-running this notebook is always safe.
# MAGIC
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog to build in |
# MAGIC
# MAGIC Creates:
# MAGIC - `raw` schema + `landing` Volume: where `extract/upload.py` drops JSON batches
# MAGIC - `bronze`, `silver`, `ops` schemas: tables are created by the pipeline steps that own them

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")

# COMMAND ----------

import os
import sys

# Git folder notebooks run with their own directory as cwd; make src/ importable
sys.path.insert(0, os.path.abspath("../src"))

from marketpulse.config import PipelineConfig

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"))
print(f"Setting up MarketPulse in catalog '{cfg.catalog}'")

# COMMAND ----------

for schema, comment in cfg.schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comment}'")
    print(f"schema  {schema}")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {cfg.landing_volume_name} "
    "COMMENT 'Raw JSON batches uploaded by extract/upload.py, one folder per batch_id'"
)
print(f"volume  {cfg.landing_volume_name}  ->  {cfg.landing_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}"))

# COMMAND ----------

display(dbutils.fs.ls(cfg.landing_root))
