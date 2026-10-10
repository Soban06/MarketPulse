"""
Audit logging for every pipeline step -> <catalog>.ops.pipeline_execution_logs.

One log row is written per unit of work: one batch x source -> one target table,
in every layer (source_to_raw, raw_to_bronze, bronze_to_silver), for full,
incremental and backfill loads alike.
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EXECUTION_LOG_TABLE = "pipeline_execution_logs"

LAYERS = ("source_to_raw", "raw_to_bronze", "bronze_to_silver")
LOAD_TYPES = ("full", "incremental", "backfill")
STATUSES = ("RUNNING", "SUCCESS", "FAILED", "SKIPPED")


def _field(name, data_type, comment, nullable=True):
    return StructField(name, data_type, nullable, metadata={"comment": comment})


EXECUTION_LOG_SCHEMA = StructType([
    _field("log_id", StringType(), "Unique id of this log entry (primary key)", nullable=False),
    _field("pipeline_run_id", StringType(), "Groups every entry written by one notebook or job run"),
    _field("layer", StringType(), "source_to_raw | raw_to_bronze | bronze_to_silver"),
    _field("load_type", StringType(), "full | incremental | backfill"),
    _field("batch_id", StringType(), "Landing batch processed, e.g. 20261010T132844Z_full"),
    _field("parameter", StringType(), "Exact input processed: a Volume path or the backfill parameters"),
    _field("target_table", StringType(), "Fully qualified table (or Volume) written to"),
    _field("start_time", TimestampType(), "When the step started"),
    _field("end_time", TimestampType(), "When the step finished; null while RUNNING"),
    _field("duration_seconds", DoubleType(), "end_time minus start_time"),
    _field("status", StringType(), "RUNNING | SUCCESS | FAILED | SKIPPED"),
    _field("rows_read", LongType(), "Rows read from the source of this step"),
    _field("rows_inserted", LongType(), "Rows inserted into the target"),
    _field("rows_updated", LongType(), "Existing target rows updated"),
    _field("rows_quarantined", LongType(), "Rows rejected to a quarantine table"),
    _field("error_message", StringType(), "Exception (FAILED) or warning text"),
    _field("executed_by", StringType(), "Databricks identity that ran the step"),
    _field("load_timestamp", TimestampType(), "When this log row was last written"),
])


def create_execution_log_table(spark, cfg):
    """Create the log table if missing, with column comments and an informational primary key."""
    columns = ",\n    ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        f" COMMENT '{f.metadata['comment']}'"
        for f in EXECUTION_LOG_SCHEMA.fields
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.ops(EXECUTION_LOG_TABLE)} (
            {columns},
            CONSTRAINT {EXECUTION_LOG_TABLE}_pk PRIMARY KEY (log_id)
        )
        COMMENT 'One row per pipeline step: layer, batch, timings, status and row counts'
    """)
