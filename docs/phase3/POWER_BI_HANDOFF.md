# Saad - Power BI Desktop instructions

Soban builds and validates Gold. Saad owns the Power BI model, pages and PBIX.
Do this after notebook 03 or the full job completes successfully.

## 1. Connect to Databricks

1. In Databricks **SQL Warehouses**, open the warehouse and its connection details.
   Copy **Server hostname** and **HTTP path** (not an access token).
2. Power BI Desktop -> **Get Data** -> Databricks connector -> enter those details.
   Use **Import** mode. For an AWS SQL warehouse with OAuth use **Databricks**;
   for Azure Databricks or a supported token-based connection use **Azure Databricks**.
   Use your own authorized account. Test this against the actual Free Edition workspace.
3. Select these objects from `workspace.gold`:
   `dim_date`, `dim_ticker`, `fact_daily_market`, `vw_pipeline_execution`,
   `vw_data_quality`, `vw_price_revisions`.
4. The optional `fact_monthly_market` is a separate aggregate. It is not required
   for the three main charts. Never join daily and monthly facts directly.
5. Exclude `_stage_*` objects if a pipeline run is in progress. Refresh after it succeeds.

Official connector details: https://learn.microsoft.com/en-us/power-query/connectors/databricks

## 2. Model view

Set **one-to-many, single-direction** relationships:

| One side | Many side |
|---|---|
| dim_ticker[ticker_key] | fact_daily_market[ticker_key] |
| dim_date[date_key] | fact_daily_market[date_key] |
| dim_ticker[ticker_key] | vw_price_revisions[ticker_key] |
| dim_date[date_key] | vw_price_revisions[date_key] |

If importing monthly facts, relate their keys to those same dimensions.
Mark dim_date as the Date table using **calendar_date**, set its type to Date,
sort month_name by month, and sort year_month chronologically. Hide surrogate
keys and lineage fields. Disable automatic date hierarchies for this file.

Keep `vw_pipeline_execution` and `vw_data_quality` disconnected from the market
model initially. Use execution `start_time` slicers on the operational page;
quarantine batch ids are extraction batches, not market dates. A shared dim_date
relationship to execution date is optional, but label it clearly if configured.

## 3. Measures

Create the measures in [measures.dax](../../bi/measures.dax). These expect the
table names above. Format return/volatility measures as percentages (do not
multiply by 100 again). Set price, moving-average and risk columns to **Don't
summarize** when used directly. Only volume/counts are naturally additive.

## 4. Main report page: the three Phase 1 questions

**Price and moving averages:** line chart with dim_date[calendar_date] on X;
Closing Price, MA 7 and MA 30 on Y. Add a **single-select** ticker slicer, default
AAPL. Use matching close-based averages. Show full history or a useful date window.

**Relative risk:** bar chart with dim_ticker[ticker] on Y and Latest Volatility 30
on X. Sort descending; use Top N if 503 symbols overcrowd it. This measure uses
each ticker's latest observation within the selected dates. Put Latest Observation
in tooltips so stale observations are visible. It is not an average of daily risks.

**Volume versus movement:** scatter chart with Trading Volume on X and Absolute
Daily Return on Y. Each point must be one **ticker + trade_date** observation,
using date as Details/Values and ticker as legend/category, or a Power Query
observation-label column combining ticker_key and trade_date. Verify the point
count against source rows; do not collapse all dates into one dot per stock.
Filter blank returns out. Add Relative Volume to tooltips or a second scatter
with Relative Volume on X. Label the result as association, not causation.

Add KPI cards for Latest Close, Latest Volatility 30 and Trading Volume. Use a date
range slicer and sector slicer. Ensure the single-ticker price control does not
accidentally restrict the cross-ticker risk chart (Edit interactions or separate pages).
Do not fabricate values during MA/volatility warm-up; blanks are expected.

## 5. Pipeline health page

- Status chart: execution **step counts** by status/layer from vw_pipeline_execution.
- Duration trend: execution date vs Summed Step Seconds, with run-id/layer filters.
  This is summed step time, not billed compute or complete laptop-to-dashboard latency.
- Error chart/table: vw_data_quality[source_table], reject_reason, Rejected Rows.
- Revision card/trend: count vw_price_revisions, optionally by detecting batch/date.
- Show the data's latest observed date and import refresh time.

Do not sum rows_quarantined across execution steps: the same reject count can be
reported on quarantine and fact log entries. Use vw_data_quality for reject totals.
Zero actual rejects is a legitimate result. If illustrating synthetic errors from
82, keep that screenshot separate and explicitly label it **synthetic test data**.

## 6. Deliver to Soban

- Save `bi/MarketPulse.pbix` (the repository is public; review what is embedded).
- Dashboard screenshot(s), including all three proposal visuals and operations.
- Model-view screenshot showing one-to-many relationships.
- Confirm slicers work and refresh succeeds directly from Gold.
- Do not embed passwords, tokens or screenshots of credential dialogs.

Publishing a public dashboard is optional; the PBIX satisfies the file deliverable.
Soban needs the screenshots for the architecture report.
