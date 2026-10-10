"""
Every catalog, schema, volume and table location the pipeline uses.

Nothing else in the codebase hard-codes a name or path. Notebooks build a
PipelineConfig from their `catalog` widget, so the same code runs in any
member's workspace, or against a scratch catalog for testing, without edits.

This module is plain Python (no pyspark import) so the local extractor can
share it with the Databricks side.
"""

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"
    raw_schema: str = "raw"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    ops_schema: str = "ops"
    landing_volume: str = "landing"

    def __post_init__(self):
        # names are interpolated into SQL, so reject anything that isn't a plain identifier
        for field, value in self.__dict__.items():
            if not _IDENTIFIER.match(value):
                raise ValueError(f"{field}={value!r} is not a valid Unity Catalog identifier")

    # ------------------------------------------------------------- raw landing

    @property
    def landing_volume_name(self) -> str:
        return f"{self.catalog}.{self.raw_schema}.{self.landing_volume}"

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.raw_schema}/{self.landing_volume}"

    def batch_path(self, batch_id: str) -> str:
        return f"{self.landing_root}/{batch_id}"

    # ------------------------------------------------------------------ tables

    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def ops(self, table: str) -> str:
        return f"{self.catalog}.{self.ops_schema}.{table}"

    @property
    def schemas(self) -> dict[str, str]:
        """Fully qualified schema name -> what it holds."""
        return {
            f"{self.catalog}.{self.raw_schema}": "Raw landing zone: JSON batches exactly as extracted",
            f"{self.catalog}.{self.bronze_schema}": "Bronze: raw records as Delta tables, explicit STRING schemas",
            f"{self.catalog}.{self.silver_schema}": "Silver: typed, deduplicated tables MERGEd on natural keys",
            f"{self.catalog}.{self.ops_schema}": "Operational tables: pipeline execution logs",
        }
