"""
Audit logging for every pipeline step -> <catalog>.ops.pipeline_execution_logs.

One log row is written per unit of work: one batch x source -> one target table,
in every layer (source_to_raw, raw_to_bronze, bronze_to_silver), for full,
incremental and backfill loads alike.

    audit = AuditLogger(spark, cfg)
    with audit.step("bronze_to_silver", load_type="incremental", batch_id=b,
                    parameter=f"batch_id={b}", target_table=cfg.silver("fact_daily_price")) as log:
        merge_prices(...)
        log.record_delta_metrics()     # inserted/updated counts from the Delta commit(s)

On entry the step is written as RUNNING. On exit the same row (keyed by log_id)
becomes SUCCESS with its row counts, or FAILED with the error -- and the error
is re-raised so the job still fails. A row stuck in RUNNING means the process
died mid-step.
"""

import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

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


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _format_error(error: BaseException, limit: int = 4000) -> str:
    return f"{type(error).__name__}: {error}"[:limit]


# Delta operations whose operationMetrics we know how to read
_MERGE_OPS = {"MERGE"}
_WRITE_OPS = {"WRITE", "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"}


@dataclass
class StepLog:
    """The in-progress log row for one step. Pipeline code fills in the counts."""

    log_id: str
    pipeline_run_id: str
    layer: str
    load_type: str
    batch_id: str | None
    parameter: str | None
    target_table: str | None
    start_time: datetime
    executed_by: str | None
    end_time: datetime | None = None
    status: str = "RUNNING"
    rows_read: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_quarantined: int | None = None
    error_message: str | None = None
    _logger: Any = field(default=None, repr=False)
    _start_version: int | None = field(default=None, repr=False)

    def record(self, **counts: int):
        """Set row counts, e.g. log.record(rows_read=n, rows_quarantined=q)."""
        allowed = {"rows_read", "rows_inserted", "rows_updated", "rows_quarantined"}
        unknown = set(counts) - allowed
        if unknown:
            raise ValueError(f"Unknown count(s) {sorted(unknown)}; expected {sorted(allowed)}")
        for name, value in counts.items():
            setattr(self, name, None if value is None else int(value))

    def record_delta_metrics(self, table: str | None = None):
        """Fill inserted/updated (and rows_read for MERGE) from Delta commits made during this step."""
        table = table or self.target_table
        start_version = self._start_version if table == self.target_table else None
        self.record(**self._logger.delta_metrics_since(table, start_version))

    def as_row(self) -> tuple:
        end = self.end_time
        values = {
            **{f: getattr(self, f) for f in self.__dataclass_fields__ if not f.startswith("_")},
            "duration_seconds": (end - self.start_time).total_seconds() if end else None,
            "load_timestamp": _now(),
        }
        return tuple(values[f.name] for f in EXECUTION_LOG_SCHEMA.fields)


class AuditLogger:
    def __init__(self, spark, cfg, pipeline_run_id: str | None = None):
        self.spark = spark
        self.cfg = cfg
        self.table = cfg.ops(EXECUTION_LOG_TABLE)
        self.pipeline_run_id = pipeline_run_id or str(uuid.uuid4())
        self.executed_by = spark.sql("SELECT current_user()").first()[0]
        self._view = f"_audit_row_{uuid.uuid4().hex}"
        create_execution_log_table(spark, cfg)

    @contextmanager
    def step(self, layer: str, *, load_type: str, batch_id: str | None,
             parameter: str | None, target_table: str | None):
        self._validate(layer, load_type)
        log = StepLog(
            log_id=str(uuid.uuid4()),
            pipeline_run_id=self.pipeline_run_id,
            layer=layer,
            load_type=load_type,
            batch_id=batch_id,
            parameter=parameter,
            target_table=target_table,
            start_time=_now(),
            executed_by=self.executed_by,
            _logger=self,
            _start_version=self._table_version(target_table),
        )
        self._write(log)  # RUNNING; if even this fails, the step should not run unlogged

        try:
            yield log
            log.status = "SUCCESS"
        except BaseException as error:
            log.status = "FAILED"
            log.error_message = _format_error(error)
            raise
        finally:
            log.end_time = _now()
            try:
                self._write(log)
            except Exception as write_error:  # never mask the step's own outcome
                print(f"WARNING: could not update audit log {log.log_id}: {_format_error(write_error)}")

    # ------------------------------------------------------------ delta metrics

    def _table_version(self, table: str | None) -> int | None:
        """Current Delta version of `table`, -1 if it doesn't exist yet, None if it isn't a table."""
        if not table:
            return None
        if not self.spark.catalog.tableExists(table):
            return -1
        try:
            return self.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
        except Exception:  # e.g. a Volume rather than a Delta table
            return None

    def delta_metrics_since(self, table: str, start_version: int | None) -> dict[str, int]:
        """Sum row metrics over the Delta commits made to `table` after `start_version`.

        Reading every commit since the step began (not just the latest) means a step
        that writes twice is fully counted, and a step that commits nothing -- e.g. a
        MERGE with no changes -- correctly reports zero instead of an older commit's
        numbers.
        """
        if start_version is None:
            raise ValueError(f"No starting Delta version recorded for {table}")
        commits = (
            self.spark.sql(f"DESCRIBE HISTORY {table}")
            .where(f"version > {start_version}")
            .select("version", "operation", "operationMetrics")
            .collect()
        )
        totals = {"rows_inserted": 0, "rows_updated": 0}
        for commit in commits:
            metrics = {k: int(v) for k, v in (commit["operationMetrics"] or {}).items() if str(v).isdigit()}
            if commit["operation"] in _MERGE_OPS:
                totals["rows_inserted"] += metrics.get("numTargetRowsInserted", 0)
                totals["rows_updated"] += metrics.get("numTargetRowsUpdated", 0)
                totals["rows_read"] = totals.get("rows_read", 0) + metrics.get("numSourceRows", 0)
            elif commit["operation"] in _WRITE_OPS:
                totals["rows_inserted"] += metrics.get("numOutputRows", 0)
        return totals

    # ------------------------------------------------------------------ writes

    def _validate(self, layer: str, load_type: str):
        if layer not in LAYERS:
            raise ValueError(f"layer must be one of {LAYERS}, got {layer!r}")
        if load_type not in LOAD_TYPES:
            raise ValueError(f"load_type must be one of {LOAD_TYPES}, got {load_type!r}")

    def _write(self, log: StepLog):
        """Upsert the log row by log_id, so RUNNING and the final status share one row."""
        self.spark.createDataFrame([log.as_row()], EXECUTION_LOG_SCHEMA).createOrReplaceTempView(self._view)
        self.spark.sql(f"""
            MERGE INTO {self.table} AS t
            USING {self._view} AS s
            ON t.log_id = s.log_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
