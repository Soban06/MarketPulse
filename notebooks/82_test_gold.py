# Databricks notebook source
# MAGIC %md
# MAGIC # 82 - Gold integration checks (synthetic data, isolated schemas)
# MAGIC Run all on Serverless. Uses unique gold_test_<id>_* schemas and never changes
# MAGIC production tables. Proves Delta key stability, windows, correction propagation,
# MAGIC idempotent MERGE, source deletion reconciliation, audit migration and retry.
# MAGIC Export this notebook WITH OUTPUTS after it passes.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("source_root", "")
dbutils.widgets.dropdown("cleanup", "true", ["true", "false"])

# COMMAND ----------
import os
import sys
import uuid
import math
import statistics
from datetime import date, datetime, timedelta

sys.path.insert(0, dbutils.widgets.get("source_root").strip() or os.path.abspath("../src"))
from marketpulse import gold
from marketpulse.config import PipelineConfig
from marketpulse.schemas import ALL_TABLES, SILVER_FACT_DAILY_PRICE, SILVER_DIM_TICKER, create_all_tables
from marketpulse.audit import EXECUTION_LOG_SCHEMA, create_execution_log_table

prefix = f"gold_test_{uuid.uuid4().hex[:8]}"
cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog"),
                     raw_schema=f"{prefix}_raw", bronze_schema=f"{prefix}_bronze",
                     silver_schema=f"{prefix}_silver", gold_schema=f"{prefix}_gold", ops_schema=f"{prefix}_ops")
assert all(name.split('.')[-1].startswith(prefix) for name in cfg.schemas)
for schema in cfg.schemas:
    spark.sql(f"CREATE SCHEMA {schema}")
create_all_tables(spark, cfg)
create_execution_log_table(spark, cfg)
print(f"Scratch schemas: {prefix}_* (cleanup={dbutils.widgets.get('cleanup')})")

# COMMAND ----------
# Simulate the Phase 2 audit schema, then verify the additive migration.
spark.sql(f"DROP TABLE {cfg.ops('pipeline_execution_logs')}")
legacy_columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in EXECUTION_LOG_SCHEMA.fields
                           if f.name not in ('rows_aggregated', 'rows_deleted'))
spark.sql(f"CREATE TABLE {cfg.ops('pipeline_execution_logs')} ({legacy_columns})")
create_execution_log_table(spark, cfg)
assert set(EXECUTION_LOG_SCHEMA.fieldNames()) == set(spark.table(cfg.ops('pipeline_execution_logs')).columns)

# COMMAND ----------
days = []
day = date(2026, 1, 5)
while len(days) < 65:
    if day.weekday() < 5:
        days.append(day)
    day += timedelta(days=1)
batch = "20260401T000000Z_full"
now = datetime(2026, 4, 1)
prices = [(t, d, 100.0+i, 102.0+i, 99.0+i, 101.0+i, 80.0+i,
           (i+1)*100, batch, now) for t in ("AAA", "BBB") for i, d in enumerate(days)]
spark.createDataFrame(prices, SILVER_FACT_DAILY_PRICE.schema).write.mode("append").insertInto(cfg.silver("fact_daily_price"))
profile = {"ticker":"AAA", "company_name":"Acme", "gics_sector":"Technology", "is_sp500_member":True,
           "batch_id":batch, "load_timestamp":now}
spark.createDataFrame([tuple(profile.get(f.name) for f in SILVER_DIM_TICKER.schema.fields)],
                      SILVER_DIM_TICKER.schema).write.mode("append").insertInto(cfg.silver("dim_ticker"))
first = gold.run(spark, cfg, load_type="full")
assert gold.validate(spark, cfg)["daily_rows"] == 130

# COMMAND ----------
rows = spark.sql(f"""
    SELECT f.* FROM {cfg.gold('fact_daily_market')} f
    JOIN {cfg.gold('dim_ticker')} t USING (ticker_key)
    WHERE t.ticker='AAA' ORDER BY trade_date
""").collect()
assert rows[5]["ma_7"] is None and abs(rows[6]["ma_7"] - 104) < 1e-10
assert rows[28]["ma_30"] is None and abs(rows[29]["ma_30"] - 115.5) < 1e-10
assert rows[29]["volatility_30"] is None
expected = statistics.stdev((80+i)/(79+i)-1 for i in range(1, 31))
assert math.isclose(rows[30]["volatility_30"], expected, rel_tol=1e-8)
assert abs(rows[30]["relative_volume"] - 2) < 1e-10
assert spark.sql(f"SELECT sector FROM {cfg.gold('dim_ticker')} WHERE ticker='BBB'").first()[0] == "Unknown"
print("PASS: full windows, adjusted returns, volatility, prior-volume baseline and missing-profile fallback")

# COMMAND ----------
def fingerprints():
    return {s.name: spark.sql(f"""
        SELECT count(*) AS rows,
               CAST(sum(CAST(xxhash64(struct(*)) AS DECIMAL(38,0))) AS STRING) AS hash
        FROM {s.full_name(cfg)}
    """).first().asDict() for s in ALL_TABLES if s.layer == "gold"}

before = fingerprints()
second = gold.run(spark, cfg)
assert fingerprints() == before, "A no-change rerun changed values or timestamps"
assert all(l.rows_inserted == 0 and l.rows_updated == 0 and l.rows_deleted == 0
           for l in second if l.target_table)
print("PASS: identical rerun leaves Gold values, keys and write timestamps unchanged")

# COMMAND ----------
keys_before = {r['ticker']:r['ticker_key'] for r in spark.table(cfg.gold('dim_ticker')).collect()}
affected_day = days[35]
spark.sql(f"""UPDATE {cfg.silver('fact_daily_price')} SET close=200, adj_close=150,
    batch_id='20260402T000000Z_incremental' WHERE ticker='AAA' AND date=DATE'{affected_day}'""")
spark.sql(f"UPDATE {cfg.silver('dim_ticker')} SET gics_sector='Reclassified' WHERE ticker='AAA'")
# Add a symbol that sorts before AAA. Existing surrogate keys must stay stable.
one = list(prices[0]); one[0] = "A0"
spark.createDataFrame([tuple(one)], SILVER_FACT_DAILY_PRICE.schema).write.mode("append").insertInto(cfg.silver("fact_daily_price"))
third = gold.run(spark, cfg, load_type="backfill")
keys_after = {r['ticker']:r['ticker_key'] for r in spark.table(cfg.gold('dim_ticker')).collect()}
assert all(keys_after[t] == k for t,k in keys_before.items())
assert keys_after['A0'] > max(keys_before.values())
updated = spark.sql(f"""SELECT f.daily_return, f.ma_7 FROM {cfg.gold('fact_daily_market')} f
    JOIN {cfg.gold('dim_ticker')} t USING(ticker_key)
    WHERE t.ticker='AAA' AND f.trade_date=DATE'{days[36]}'""").first()
assert abs(updated['daily_return'] - (116/150-1)) < 1e-10
assert updated['ma_7'] != rows[36]['ma_7']
print("PASS: historical correction updates later metrics, Type 1 attributes update, allocated keys stay stable")

# COMMAND ----------
spark.sql(f"DELETE FROM {cfg.silver('fact_daily_price')} WHERE ticker='A0'")
gold.run(spark, cfg)
assert gold.validate(spark, cfg)['daily_rows'] == 130
assert spark.sql(f"SELECT count(*) FROM {cfg.gold('dim_ticker')} WHERE ticker='A0'").first()[0] == 1
assert spark.table(cfg.gold('vw_data_quality')).count() == 0
print("PASS: deleted source fact reconciles; allocated dimension key is retained; empty quarantine is valid")

# COMMAND ----------
# A failed preflight must preserve existing business data and be retryable.
saved = spark.table(cfg.silver('fact_daily_price'))
saved_table = cfg.silver('saved_prices')
saved.write.format('delta').saveAsTable(saved_table)
spark.sql(f"DELETE FROM {cfg.silver('fact_daily_price')}")
before = fingerprints()
try:
    gold.run(spark, cfg)
    raise AssertionError("Empty source should fail")
except ValueError as error:
    assert "no price rows" in str(error)
assert fingerprints() == before
assert spark.sql(f"SELECT count(*) FROM {cfg.ops('pipeline_execution_logs')} WHERE status='FAILED'").first()[0] >= 1
spark.sql(f"INSERT INTO {cfg.silver('fact_daily_price')} SELECT * FROM {saved_table}")
gold.run(spark, cfg)
assert fingerprints() == before
print("PASS: failure is logged, prior Gold survives, retry succeeds")

# COMMAND ----------
display(spark.sql(f"""SELECT layer, target_table, status, rows_read, rows_inserted,
    rows_updated, rows_deleted, rows_aggregated, duration_seconds
    FROM {cfg.ops('pipeline_execution_logs')} ORDER BY start_time"""))
print("ALL GOLD INTEGRATION CHECKS PASSED")

# COMMAND ----------
if dbutils.widgets.get("cleanup") == "true":
    # Drop Gold first because its informational FKs reference Gold dimensions.
    for schema in [cfg.gold_schema, cfg.silver_schema, cfg.bronze_schema, cfg.ops_schema, cfg.raw_schema]:
        assert schema.startswith(prefix)
        spark.sql(f"DROP SCHEMA {cfg.catalog}.{schema} CASCADE")
    print("Removed only this test run's scratch schemas")
