"""
Generate the bronze/silver data dictionary in README.md from the code.

Every table, column, type, description and key comes from
src/marketpulse/schemas.py (plus the audit log table in audit.py), so the
README can never disagree with what the pipeline actually creates.

    python scripts/generate_data_dictionary.py

Rewrites everything between these two lines in README.md:

    <!-- data-dictionary:start -->
    <!-- data-dictionary:end -->
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
START = "<!-- data-dictionary:start -->"
END = "<!-- data-dictionary:end -->"

sys.path.insert(0, str(REPO_ROOT / "src"))

from marketpulse.audit import EXECUTION_LOG_SCHEMA, EXECUTION_LOG_TABLE  # noqa: E402
from marketpulse.schemas import (  # noqa: E402
    BRONZE_TABLES,
    COMPANY_INFO_FIELDS,
    CONSTITUENTS_FIELDS,
    PRICES_FIELDS,
    SILVER_TABLES,
)


def _cell(text) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def column_table(schema) -> list[str]:
    lines = ["| Column | Type | Nullable | Description |", "|---|---|---|---|"]
    for f in schema.fields:
        lines.append(
            f"| `{f.name}` | `{f.dataType.simpleString()}` | {'yes' if f.nullable else 'no'} "
            f"| {_cell(f.metadata.get('comment', ''))} |"
        )
    return lines


def table_section(spec) -> list[str]:
    key = ", ".join(f"`{k}`" for k in spec.key)
    if spec.enforce_key:
        key_line = f"**Primary key:** {key} (enforced: `NOT NULL` + `PRIMARY KEY` constraint)"
    else:
        key_line = f"**Grain:** {key} (documented, not enforced)"
    return [
        f"#### `{spec.layer}.{spec.name}`",
        "",
        _cell(spec.comment),
        "",
        key_line,
        "",
        *column_table(spec.schema),
        "",
    ]


def mapping_section(title, fields) -> list[str]:
    lines = [f"**{title}**", "", "| Raw JSON field | Bronze column |", "|---|---|"]
    lines += [f"| `{raw}` | `{bronze}` |" for raw, bronze, _ in fields]
    return lines + [""]


def build() -> str:
    lines = ["### Raw → bronze column mapping", ""]
    lines += [
        "Raw JSON field names are renamed to snake_case in bronze "
        "(Delta column names cannot contain spaces).",
        "",
    ]
    lines += mapping_section("prices/<TICKER>.json", PRICES_FIELDS)
    lines += mapping_section("sp500_constituents.json", CONSTITUENTS_FIELDS)
    lines += mapping_section("company_info/<TICKER>.json", COMPANY_INFO_FIELDS)

    lines += ["### Bronze tables", ""]
    for spec in BRONZE_TABLES:
        lines += table_section(spec)

    lines += ["### Silver tables", ""]
    for spec in SILVER_TABLES:
        lines += table_section(spec)

    lines += [
        "### Operational tables",
        "",
        f"#### `ops.{EXECUTION_LOG_TABLE}`",
        "",
        "One row per pipeline step (batch × source → target table) across every layer.",
        "",
        "**Primary key:** `log_id` (enforced)",
        "",
        *column_table(EXECUTION_LOG_SCHEMA),
        "",
    ]
    return "\n".join(lines)


def main():
    text = README.read_text(encoding="utf-8")
    if START not in text or END not in text:
        raise SystemExit(f"README.md needs the marker lines {START} and {END} where the tables should go.")
    before, rest = text.split(START, 1)
    _, after = rest.split(END, 1)
    README.write_text(f"{before}{START}\n\n{build()}\n{END}{after}", encoding="utf-8")
    print(f"Updated {README.name}: {len(BRONZE_TABLES)} bronze, {len(SILVER_TABLES)} silver, 1 ops table")


if __name__ == "__main__":
    main()