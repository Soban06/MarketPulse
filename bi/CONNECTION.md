# Saad's Databricks connection

Use **Import** mode in Power BI Desktop.

- Server hostname: `dbc-43683058-46b3.cloud.databricks.com`
- HTTP path: `/sql/1.0/warehouses/ce6164dbb634f66f`
- Warehouse: `Serverless Starter Warehouse` (2X-Small)
- Catalog: `workspace`
- Schema: `gold`
- Authentication: your own authorized Databricks account; no credential is stored here.

Follow [the dashboard guide](../docs/phase3/POWER_BI_HANDOFF.md). Load only the named
Gold facts, dimensions and views. Refresh after a successful complete pipeline run.
These are connection identifiers, not passwords or access tokens.
