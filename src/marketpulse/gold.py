"""Silver -> Gold, full-history calculation with conditional Delta MERGEs.

Run serially (job max_concurrent_runs=1). Source Delta versions are pinned for
the whole refresh. Recompute all windows so backfills and corrections propagate
to later dates; MERGE only changed business values. A single fact MERGE is atomic,
but publication across all Gold tables is not. Refresh Power BI only after the
entire job succeeds. Failed runs can safely be repeated.
"""

import uuid

from marketpulse.audit import AuditLogger
from marketpulse.gold_sql import daily_query, monthly_query, ticker_query
from marketpulse.schemas import GOLD_TABLES, create_table

LAYER = "silver_to_gold"


def assert_unique(spark, relation, keys):
    nulls = " OR ".join(f"{c} IS NULL" for c in keys)
    if spark.sql(f"SELECT 1 FROM {relation} WHERE {nulls} LIMIT 1").take(1):
        raise ValueError(f"Null key in {relation}: {keys}")
    if spark.sql(
        f"SELECT 1 FROM {relation} GROUP BY {', '.join(keys)} HAVING count(*) > 1 LIMIT 1"
    ).take(1):
        raise ValueError(f"Duplicate key in {relation}: {keys}")


def merge_snapshot(spark, cfg, spec, staged, *, delete_missing=False):
    """Preserve table metadata/constraints and timestamps of unchanged rows."""
    columns = [c for c in spec.schema.fieldNames() if c != "load_timestamp"]
    values = [c for c in columns if c not in spec.key]
    on = " AND ".join(f"t.{c} = s.{c}" for c in spec.key)
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in values)
    timestamped = "load_timestamp" in spec.schema.fieldNames()
    update = ", ".join(f"{c} = s.{c}" for c in values)
    if timestamped:
        update += ", load_timestamp = current_timestamp()"
    inserted = columns + (["load_timestamp"] if timestamped else [])
    insert_values = [f"s.{c}" for c in columns] + (["current_timestamp()"] if timestamped else [])
    delete = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_missing else ""
    spark.sql(f"""
        MERGE INTO {spec.full_name(cfg)} t USING {staged} s ON {on}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET {update}
        WHEN NOT MATCHED THEN INSERT ({', '.join(inserted)}) VALUES ({', '.join(insert_values)})
        {delete}
    """)


def date_query(prices, logs):
    return f"""
        WITH dates AS (
            SELECT date AS d FROM {prices}
            UNION ALL SELECT CAST(start_time AS DATE) FROM {logs}
            UNION ALL SELECT current_date()
        ), bounds AS (
            SELECT CAST(date_trunc('MONTH', min(d)) AS DATE) AS lo, max(d) AS hi FROM dates
        ), calendar AS (
            SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS calendar_date FROM bounds
        ), observed AS (SELECT DISTINCT date FROM {prices})
        SELECT CAST(year(c.calendar_date)*10000 + month(c.calendar_date)*100 + day(c.calendar_date) AS BIGINT) AS date_key,
               c.calendar_date, CAST(year(c.calendar_date) AS BIGINT) AS year,
               CAST(quarter(c.calendar_date) AS BIGINT) AS quarter,
               CAST(month(c.calendar_date) AS BIGINT) AS month,
               date_format(c.calendar_date, 'MMMM') AS month_name,
               date_format(c.calendar_date, 'yyyy-MM') AS year_month,
               CAST(day(c.calendar_date) AS BIGINT) AS day_of_month,
               CAST(pmod(dayofweek(c.calendar_date) + 5, 7) + 1 AS BIGINT) AS day_of_week,
               dayofweek(c.calendar_date) IN (1, 7) AS is_weekend,
               o.date IS NOT NULL AS is_observed_trading_day
        FROM calendar c LEFT JOIN observed o ON c.calendar_date = o.date
    """


def create_views(spark, cfg):
    """Expose operational analytics without copying or double-counting audit data."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {cfg.gold('vw_pipeline_execution')} AS
        SELECT log_id, pipeline_run_id, layer, load_type, batch_id, target_table,
               CAST(year(start_time)*10000 + month(start_time)*100 + day(start_time) AS BIGINT) AS date_key,
               start_time, end_time, duration_seconds, status,
               rows_read, rows_inserted, rows_updated, rows_deleted, rows_quarantined, rows_aggregated,
               error_message
        FROM {cfg.ops('pipeline_execution_logs')}
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {cfg.gold('vw_data_quality')} AS
        WITH rejected AS (
            SELECT batch_id, 'prices' AS source_table, reject_reason FROM {cfg.silver('quarantine_prices')}
            UNION ALL
            SELECT batch_id, 'company_info' AS source_table, reject_reason FROM {cfg.silver('quarantine_company_info')}
        )
        SELECT batch_id, source_table, reject_reason, count(*) AS rejected_rows
        FROM rejected GROUP BY batch_id, source_table, reject_reason
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {cfg.gold('vw_price_revisions')} AS
        SELECT d.ticker_key,
               CAST(year(r.date)*10000 + month(r.date)*100 + day(r.date) AS BIGINT) AS date_key,
               r.date AS trade_date, r.batch_id, r.old_adj_close, r.new_adj_close, r.adj_close_pct,
               r.load_timestamp AS detected_at
        FROM {cfg.silver('price_revision')} r
        JOIN {cfg.gold('dim_ticker')} d ON r.ticker = d.ticker
    """)


def validate(spark, cfg):
    """Fail before reporting success if Gold cannot safely be consumed by Power BI."""
    for spec in GOLD_TABLES:
        assert_unique(spark, spec.full_name(cfg), spec.key)
    assert_unique(spark, cfg.gold("dim_ticker"), ("ticker",))
    assert_unique(spark, cfg.gold("dim_date"), ("calendar_date",))
    for fact in ("fact_daily_market", "fact_monthly_market"):
        if spark.sql(f"""
            SELECT 1 FROM {cfg.gold(fact)} f
            LEFT JOIN {cfg.gold('dim_ticker')} t ON f.ticker_key = t.ticker_key
            LEFT JOIN {cfg.gold('dim_date')} d ON f.date_key = d.date_key
            WHERE t.ticker_key IS NULL OR d.date_key IS NULL LIMIT 1
        """).take(1):
            raise ValueError(f"Orphan dimension reference in {fact}")
    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM {cfg.gold('fact_daily_market')}) AS daily_rows,
               (SELECT count(*) FROM {cfg.gold('fact_monthly_market')}) AS monthly_rows,
               (SELECT coalesce(sum(observation_count), 0) FROM {cfg.gold('fact_monthly_market')}) AS aggregated_rows
    """).first().asDict()
    if counts["daily_rows"] == 0 or counts["daily_rows"] != counts["aggregated_rows"]:
        raise ValueError(f"Daily/monthly reconciliation failed: {counts}")
    return counts


def run(spark, cfg, *, pipeline_run_id=None, load_type="incremental"):
    """Refresh current Silver state, including earlier corrections. No batch watermark.

    Gold audit batch_id is NULL intentionally: each refresh combines many source
    batches. parameter records the exact Silver Delta versions and strategy.
    """
    audit = AuditLogger(spark, cfg, pipeline_run_id=pipeline_run_id)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.gold_schema}")
    for spec in GOLD_TABLES:
        create_table(spark, cfg, spec)
    suffix = uuid.uuid4().hex
    sources = {}
    stages = []
    logs = []
    versions = {}
    try:
        with audit.step(LAYER, load_type=load_type, batch_id=None,
                        parameter="phase=preflight", target_table=None) as log:
            for name in ("fact_daily_price", "dim_ticker"):
                table = cfg.silver(name)
                version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
                versions[name] = version
                view = f"_gold_source_{name}_{suffix}"
                spark.sql(f"SELECT * FROM {table} VERSION AS OF {version}").createOrReplaceTempView(view)
                sources[name] = view
            prices, profiles = sources["fact_daily_price"], sources["dim_ticker"]
            assert_unique(spark, prices, ("ticker", "date"))
            assert_unique(spark, profiles, ("ticker",))
            rows = spark.sql(f"SELECT count(*) AS n FROM {prices}").first()["n"]
            if not rows:
                raise ValueError("Silver has no price rows. Run Bronze and Silver first; existing Gold is preserved.")
            log.record(rows_read=rows)
        logs.append(log)
        parameter = f"strategy=full_history_conditional_merge; silver_versions={versions}"
        queries = {}
        for spec in GOLD_TABLES:
            target = spec.full_name(cfg)
            with audit.step(LAYER, load_type=load_type, batch_id=None,
                            parameter=parameter, target_table=target) as log:
                if spec.name == "dim_date":
                    query = date_query(prices, cfg.ops("pipeline_execution_logs"))
                elif spec.name == "dim_ticker":
                    query = ticker_query(prices, profiles, target)
                elif spec.name == "fact_daily_market":
                    query = daily_query(prices, queries["dim_ticker"])
                else:
                    query = monthly_query(queries["fact_daily_market"])
                # Persist once: subsequent validation/MERGE/aggregation read the same
                # staged data and do not re-run expensive price windows.
                stage = cfg.gold(f"_stage_{spec.name}_{suffix}")
                stages.append(stage)
                # SQL CTAS fails if this UUID-named stage already exists and works
                # on Spark Connect runtimes that reject the errorifexists writer alias.
                spark.sql(f"CREATE TABLE {stage} USING DELTA AS {query}")
                queries[spec.name] = stage
                assert_unique(spark, stage, spec.key)
                if spec.name == "dim_ticker":
                    assert_unique(spark, stage, ("ticker",))
                staged_rows = spark.table(stage).count()
                if spec.name == "fact_daily_market" and staged_rows != rows:
                    raise ValueError("Daily Gold would lose or multiply Silver rows")
                merge_snapshot(spark, cfg, spec, stage, delete_missing=spec.name.startswith("fact_"))
                log.record_delta_metrics()
                # rows_read is logical input, not physical scan count; a window may
                # read source data multiple times internally.
                log.record(rows_read=rows if spec.name.startswith("fact_") else staged_rows)
                if spec.name == "fact_monthly_market":
                    log.record(rows_aggregated=staged_rows)
            logs.append(log)
            print(f"Gold {spec.name}: inserted={log.rows_inserted}, updated={log.rows_updated}, deleted={log.rows_deleted}")
        with audit.step(LAYER, load_type=load_type, batch_id=None,
                        parameter=f"phase=publication; {parameter}", target_table=None) as log:
            create_views(spark, cfg)
            counts = validate(spark, cfg)
            if counts["daily_rows"] != rows:
                raise ValueError("Published Gold row count differs from pinned Silver snapshot")
            log.record(rows_read=rows, rows_aggregated=counts["monthly_rows"])
        logs.append(log)
        return logs
    finally:
        for stage in reversed(stages):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {stage}")
            except Exception as error:
                print(f"Staging cleanup needed for {stage}: {type(error).__name__}")
        for view in sources.values():
            spark.catalog.dropTempView(view)
