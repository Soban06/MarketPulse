# FinOps: measured project costs and an explicit enterprise scenario

## Actual project

The deployed workspace uses **Databricks Free Edition on AWS**, not a measured
Azure for Students subscription. Databricks describes Free Edition as a no-cost
offering with usage quotas. Compute quota consumption is real even when the
project's monetary charge is $0. Neither elapsed seconds nor summed log durations
is a measurement of billed DBUs.

Notebook 93 and `scripts/collect_workspace_evidence.py` collect actual run times
and row counts. Report those with the run id and batch size. Keep the first Gold
build separate from an ordinary later refresh. The initial Phase 3 batch catches
up data since Phase 2; it is not evidence that exactly one trading day was added.
The Free Edition billing list-price query returned no SKU rows; it provides no
billed-DBU conversion for this project. Do not invent Azure credit consumption.

## Paid hourly scenario (assumptions, not a bill)

For a comparable **Azure East US serverless** deployment, Microsoft's public
Retail Prices API returned these USD consumption rates on **2026-10-25**:

| Meter | Retail rate | Note |
|---|---:|---|
| Premium Automated Serverless Compute DBU | $0.45 per DBU | Job-compute scenario; effectiveStartDate 2025-05-01 |
| Premium Serverless SQL DBU | $0.70 per DBU | Separate serving workload; effectiveStartDate 2024-02-01 |

Reproduce the lookup with the official API:

```python
import requests
result = requests.get(
    'https://prices.azure.com/api/retail/prices',
    params={'api-version': '2023-01-01-preview',
            '$filter': "serviceName eq 'Azure Databricks' and armRegionName eq 'eastus'"},
    timeout=30,
).json()
for row in result['Items']:
    if row['meterName'] in ('Premium Automated Serverless Compute DBU',
                            'Premium Serverless SQL DBU'):
        print(row)
```

Sources: [Microsoft Retail Prices API documentation](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices),
[official retail endpoint](https://prices.azure.com/api/retail/prices),
[Azure Databricks pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/).
These are list rates, not a negotiated quote or the price of the current AWS Free workspace.

Assume **1 billed job DBU per end-to-end run**, purely for planning. A paid pilot
must measure it; Free Edition wall time cannot prove this assumption. Add:

- $5/month storage budget (including retained batches, Delta history and logs).
- Two Power BI Premium Per User licenses at $24/user/month, paid yearly: $48/month.
  This supports an hourly Import scenario within the advertised 48 refreshes/day.
  Current project submission uses free Desktop and does not purchase these licenses.
- $100/month budget for serving SQL-warehouse compute, requests and transfer.
  This requires short-lived serving compute; long idle time can exceed the budget.
  At $0.70 per serving DBU, $100 buys about 142.9 DBUs before request/transfer costs.

The license rate/refresh allowance comes from
[Microsoft's Power BI pricing](https://www.microsoft.com/en-us/power-platform/products/power-bi/pricing).
Storage and serving budgets above are assumptions, not falsely precise unit-price quotes.

Hourly 24/7 ETL = 24 * 365 = **8,760 runs/year**:

| Job usage assumption | ETL per year at $0.45/DBU | Fixed budgets per year | Total per year |
|---|---:|---:|---:|
| 0.5 DBU/run | $1,971 | $1,836 | **$3,807** |
| 1 DBU/run | $3,942 | $1,836 | **$5,778** |
| 2 DBU/run | $7,884 | $1,836 | **$9,720** |

The arithmetic excludes tax, growth, source licensing, cloud commitments and unusual
retries. Serverless already includes its underlying compute, so do not add a
classic VM charge a second time. Model applicable minimum billing and serving idle
time. Databricks states job-serverless infrastructure startup is not billed, so
do not convert all wall-clock setup time into billable execution.

## Improvements to evaluate

1. Daily OHLCV does not gain useful freshness from hourly extraction. Prefer a run
   after the daily source update; 365 runs/year is 24 times fewer than 8,760.
2. Use synthetic/sampled data during development. Reserve full runs for milestones.
3. Keep Import dashboards from repeatedly waking a warehouse; coordinate refresh.
4. Minimize warehouse idle time, record actual DBUs and set usage alerts where supported.
5. Stage expensive windows once, MERGE only changes, and optimize layout only when
   measured read savings justify maintenance.
6. If full-history calculation becomes expensive at enterprise scale, derive changed
   tickers/date ranges from durable change tracking, including the full dependent
   rolling-window horizon. Retain correction/backfill tests when changing strategies.
