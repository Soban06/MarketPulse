"""
Raw -> Bronze: load landed JSON batches into bronze Delta tables.

Each source is one RawSource entry: where its files sit inside a batch folder,
the explicit raw schema to read them with, the rename map to bronze columns and
the bronze table spec. Loading a source for a batch:

    read     spark.read.schema(<RAW schema>)       explicit, every value a STRING
               .option("multiLine", True)          each file is one JSON document
               .option("rescuedDataColumn", ...)   unknown fields land in _rescued_data
    shape    rename to snake_case, add batch_id, source_file, load_timestamp
    write    overwrite WHERE batch_id = <batch>    re-loading a batch replaces its rows

Batch selection (see select_batches):
    batch_ids / from_batch..to_batch   explicit backfill of any landed batch(es)
    nothing                            every landed batch the audit log has not
                                       completed for this layer, oldest first

Every source x batch is one audit step. A failed batch is logged and the run
moves on to the next batch -- bronze batches are independent -- and the failures
are raised together at the end so the job still reports failure.
"""

import json
import os
from dataclasses import dataclass

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from marketpulse.audit import AuditLogger
from marketpulse.schemas import (
    BRONZE_COMPANY_INFO,
    BRONZE_CONSTITUENTS,
    BRONZE_PRICES,
    COMPANY_INFO_COLUMN_MAP,
    CONSTITUENTS_COLUMN_MAP,
    PRICES_COLUMN_MAP,
    RAW_COMPANY_INFO,
    RAW_CONSTITUENTS,
    RAW_PRICES,
    RESCUED_DATA_COLUMN,
    TableSpec,
    create_table,
)

LAYER = "raw_to_bronze"
MANIFEST = "manifest.json"


@dataclass(frozen=True)
class RawSource:
    name: str
    files: str                  # glob relative to the batch folder
    raw_schema: StructType
    column_map: dict[str, str]  # raw field name -> bronze column name
    table: TableSpec
    rescued: bool = True        # capture unknown fields in _rescued_data


SOURCES: dict[str, RawSource] = {
    "prices": RawSource(
        name="prices",
        files="prices/*.json",
        raw_schema=RAW_PRICES,
        column_map=PRICES_COLUMN_MAP,
        table=BRONZE_PRICES,
    ),
    "constituents": RawSource(
        name="constituents",
        files="sp500_constituents.json",
        raw_schema=RAW_CONSTITUENTS,
        column_map=CONSTITUENTS_COLUMN_MAP,
        table=BRONZE_CONSTITUENTS,
    ),
    "company_info": RawSource(
        name="company_info",
        files="company_info/*.json",
        raw_schema=RAW_COMPANY_INFO,
        column_map=COMPANY_INFO_COLUMN_MAP,
        table=BRONZE_COMPANY_INFO,
        rescued=False,  # Yahoo sends ~170 fields we ignore; rescuing them would hide real drift
    ),
}


class BatchLoadError(RuntimeError):
    """One or more batches failed; each failure is already in the audit log."""


# --------------------------------------------------------------- batch discovery

def landed_batches(cfg) -> list[str]:
    """Batch folders in the raw Volume whose manifest exists (i.e. fully uploaded), oldest first."""
    root = cfg.landing_root
    return sorted(
        name for name in os.listdir(root)
        if os.path.isfile(os.path.join(root, name, MANIFEST))
    )


def read_manifest(cfg, batch_id: str) -> dict:
    with open(os.path.join(cfg.batch_path(batch_id), MANIFEST), encoding="utf-8") as f:
        return json.load(f)


def select_batches(audit: AuditLogger, cfg, batch_ids: list[str] | None = None,
                   from_batch: str | None = None, to_batch: str | None = None) -> tuple[list[str], bool]:
    """Return (batches to load, is_backfill)."""
    landed = landed_batches(cfg)
    if batch_ids:
        missing = [b for b in batch_ids if b not in landed]
        if missing:
            raise ValueError(f"Not complete landed batches in {cfg.landing_root}: {missing}")
        return sorted(batch_ids), True
    if from_batch or to_batch:
        return [b for b in landed if (not from_batch or b >= from_batch) and (not to_batch or b <= to_batch)], True
    return audit.pending_batches(LAYER, landed), False


# ------------------------------------------------------------------- loading

def read_source(spark, cfg, batch_id: str, source: RawSource):
    """Read one source of one batch into the bronze shape (not yet written)."""
    reader = spark.read.schema(source.raw_schema).option("multiLine", True)
    if source.rescued:
        reader = reader.option("rescuedDataColumn", RESCUED_DATA_COLUMN)
    return (
        reader.json(f"{cfg.batch_path(batch_id)}/{source.files}")
        .withColumnsRenamed(source.column_map)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(*source.table.schema.fieldNames())
    )


def load_source(spark, cfg, audit: AuditLogger, batch_id: str, source: RawSource, load_type: str):
    table = source.table.full_name(cfg)
    folder = f"{cfg.batch_path(batch_id)}/{os.path.dirname(source.files) or source.files}"
    with audit.step(LAYER, load_type=load_type, batch_id=batch_id,
                    parameter=folder, target_table=table) as log:
        (
            read_source(spark, cfg, batch_id, source)
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"batch_id = '{batch_id}'")
            .saveAsTable(table)
        )
        log.record_delta_metrics()
        log.record(rows_read=log.rows_inserted)  # bronze keeps every row it reads
    return log


def run(spark, cfg, *, batch_ids: list[str] | None = None, from_batch: str | None = None,
        to_batch: str | None = None, sources: list[str] | None = None,
        pipeline_run_id: str | None = None) -> list:
    """Load the selected batches and sources into bronze. Returns the StepLogs written."""
    unknown = set(sources or []) - set(SOURCES)
    if unknown:
        raise ValueError(f"Unknown source(s) {sorted(unknown)}; available: {sorted(SOURCES)}")
    selected = [SOURCES[name] for name in (sources or SOURCES)]

    audit = AuditLogger(spark, cfg, pipeline_run_id=pipeline_run_id)
    for source in selected:
        create_table(spark, cfg, source.table)

    batches, is_backfill = select_batches(audit, cfg, batch_ids, from_batch, to_batch)
    print(f"{LAYER}: {len(batches)} batch(es), sources {[s.name for s in selected]}, "
          f"{'backfill' if is_backfill else 'new batches only'}")

    logs, failures = [], []
    for batch_id in batches:
        manifest = read_manifest(cfg, batch_id)
        audit.log_source_to_raw(manifest, parameter=cfg.batch_path(batch_id))
        load_type = "backfill" if is_backfill else manifest["mode"]
        for source in selected:
            try:
                log = load_source(spark, cfg, audit, batch_id, source, load_type)
                logs.append(log)
                print(f"  {batch_id} {source.name:<13} {log.rows_inserted:>10,} rows  ({log.status})")
            except Exception as error:
                failures.append(f"{batch_id}/{source.name}: {type(error).__name__}: {error}")
                print(f"  {batch_id} {source.name:<13} FAILED -- continuing with remaining batches")

    if failures:
        raise BatchLoadError(f"{len(failures)} load(s) failed (see {audit.table}):\n" + "\n".join(failures))
    return logs
