# Phase 3 validation record

## Local checks

Command: `python -m unittest discover -s tests -v` with requirements-dev installed.
**38 tests passed** on 2026-10-25. Python compilation and `git diff --check` also passed.

- 26 analytical SQL tests: independent expected returns/means/standard deviations,
  trading-observation windows, null/invalid prices and volume, correction/backfill
  propagation, pairwise monthly correlations, key stability and profile fallback.
- 8 schema/orchestration/logging contracts: ordered layers, upstream-failure gating,
  Gold retry with no pending Silver batches, backfill propagation, isolated schemas,
  PK/FK generation, serial job and aggregate/delete counters.
- 4 actual audit SQL tests: latest successful retry in the same job id, latest
  failed attempt, per-target completion, and latest-job precedence.

DuckDB executes the production analytical SELECTs independently. This does not
attest Spark/Delta compatibility on its own; the following workspace run does.

## Real Databricks Gold integration

[Successful test run](https://dbc-43683058-46b3.cloud.databricks.com/jobs/1032188141586824/runs/982463585840930?o=7474657227153277).
Export: [82_test_gold.html](../run-evidence/82_test_gold.html), download/open in a browser.

Passed on Free Edition serverless using synthetic data in unique scratch schemas:

- Phase 2 audit-table migration to new metrics.
- Date/Ticker dimensions, 130 daily observations and reconciled monthly groups.
- MA7/MA30, adjusted returns, volatility warm-up and prior-volume baseline.
- Missing-profile fallback without losing price observations.
- Identical replay: zero inserts/updates/deletes; full fingerprints including timestamps unchanged.
- Historical correction: downstream windows updated; Type 1 reclassification applied.
- A new alphabetically earlier ticker retained all prior numeric keys.
- Deletion reconciliation retained allocated dimension keys.
- Empty-source failure preserved existing Gold, logged FAILED, and recovered on retry.
- All successful-run scratch schemas were removed. Two scratch environments from
  the initial unsupported-writer-option attempts were also cleaned up; production was excluded.

The first test caught a Spark Connect writer-alias compatibility issue. SQL CTAS
replaced that staging write and the complete corrected suite then passed.

## Real end-to-end incremental run

[Successful pipeline run](https://dbc-43683058-46b3.cloud.databricks.com/jobs/1088660592403482/runs/364924820876832?o=7474657227153277).
Export: [04_run_pipeline.html](../run-evidence/04_run_pipeline.html).

Batch `20261025T162210Z_incremental`: 503 symbols, 11,063 extracted price rows,
503 company profiles, 8.5 MB raw data. All were uploaded with the manifest last.

| Result | Count |
|---|---:|
| Bronze price rows in new batch | 11,063 |
| New Silver price observations | 5,030 |
| Revised Silver price observations | 346 |
| Rejected price records | 2 |
| Updated Silver company profiles | 503 |
| Gold daily fact rows | 1,230,497 |
| Gold monthly groups | 59,004 |
| Observations represented by monthly groups | 1,230,497 |

Measured Spark pipeline elapsed time: **296.817286 seconds**; summed logged
step durations: **224.012162 seconds**. This batch catches up since Phase 2 and
includes the first Gold materialization. It is not a steady-state daily median.
These durations are not billed DBUs. Actual monetary use is the no-cost Free Edition.

## Full-scale unchanged-input rerun

[Successful rerun](https://dbc-43683058-46b3.cloud.databricks.com/jobs/1088660592403482/runs/290709907986053?o=7474657227153277).
Export: [04_pipeline_idempotency.html](../run-evidence/04_pipeline_idempotency.html).

No new batch was landed. The complete entry point skipped completed Bronze/Silver
work and refreshed Gold from the same Silver snapshots. Gold recorded **0 inserted,
0 updated and 0 deleted rows** over the full 1,230,497-row dataset. Measured elapsed
time was **122.714604 seconds**, with **97.976209 seconds** summed step time.
This is a steady-state Gold/no-arrival benchmark, not a full weekday extraction run.

## Read evidence

[Evidence run](https://dbc-43683058-46b3.cloud.databricks.com/jobs/95255830185193/runs/436619686310315?o=7474657227153277).
Exports: [93_phase3_evidence.html](../run-evidence/93_phase3_evidence.html) and
[captured initial physical plan](../run-evidence/93_monthly_aggregation_plan.txt).

The 90-day analytical read took **1.105, 0.915 and 0.941 seconds** over repeated
runs; caching can affect these results. OPTIMIZE was not requested, so no before/after
speedup is claimed. The monthly query ran, and EXPLAIN shows column pruning,
partial/final grouping and a Photon shuffle. This is an initial planned execution
graph. Soban subsequently supplied the actual runtime graph and matching performance
summary, saved under docs/report/images. The graph shows a 328 ms table scan,
123 ms partial grouping, 38 ms shuffle, 2 ms later grouping, and the notebook's
1,000-row preview limit. The supplied summary reports 1.312 seconds and 21.20 MB read.

## Remaining submission evidence

- Instructor acceptance of the supplied Query Profile screenshot is confirmed by Soban.
- Saad's real PBIX, dashboard screenshot and relationship-model screenshot.
- Rebuild the final PDF after those inputs are present; the builder rejects a
  final-labelled output while required evidence is missing.
