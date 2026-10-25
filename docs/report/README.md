# Architecture report

Default output is a **DRAFT** until all required evidence is supplied. The explicit
`--submission` mode below publishes the completed engineering scope with BI pending.
The Phase 1/2 evidence in docs/run-evidence is historical;
it is not proof that the new Gold code ran successfully.

## Build

Install the development dependencies in a virtual environment, then:

```powershell
python scripts/build_report.py
python scripts/build_report.py --final
```

Draft output: `output/pdf/MarketPulse_Architecture_Report_DRAFT.pdf`.
Final output: `output/pdf/MarketPulse_Architecture_Report.pdf`.
The script is the editable source for the report. It generates architecture and
star-schema diagrams directly as vectors and embeds actual screenshots when supplied.

## Current submission version

Soban requested a report submission before the delayed Power BI deliverable.
Run `python scripts/build_report.py --submission` to create
`output/pdf/MarketPulse_Architecture_Report.pdf` for the completed engineering scope.
It requires all real engineering evidence, labels Power BI as pending on the cover
and footers, and documents the outstanding BI work in Section 9. It does not pretend
the overall Phase 3 submission is complete. `--final` still requires the actual
PBIX and dashboard/model screenshots for a fully completed package.

## Evidence to enter

Edit evidence.json. Paths are **repository-relative**, e.g.
`docs/report/images/gold-query-profile.png`. Keep screenshots readable and redact
identities/tokens that are unnecessary for grading. Add images to `images/`.

- workspace_run_id: a completed successful 03 or 04/job run.
- gold_test_run_passed: true only after notebook 82 prints its success message.
- gold_row_counts: daily_rows, monthly_rows, aggregated_rows from notebook 93.
- typical_incremental_run: run_id, wall_seconds and summed_step_seconds from 93
  for a **normal incremental run**, not the full-load run or an idempotency demo.
  Record billed_dbus only if an actual usage record exists. measured_cost_usd is
  zero for a confirmed no-cost Free Edition run; use billing/credit records for paid runs.
- dag.kind: query_profile or spark_ui; image: screenshot path; analysis: a short
  explanation of the operators and actual metrics visible in that screenshot.
  instructor_accepted_query_profile must be true if submitting a query profile.
- power_bi: actual PBIX path, dashboard image, and Model-view image from Saad.
- finops: actual scenario region, per-run enterprise cost, monthly storage/BI/other
  costs, the dated official pricing source URL and calculation assumptions.
  Enter explicit zero for excluded costs and explain the exclusion; null means unknown.

Enterprise per-run cost must account for the chosen architecture: billed DBUs
times SKU price, plus VM charges where separately billed, startup/minimum billing,
idle warehouse time, and retries. Do not add classic VM charges to a serverless
price that already includes infrastructure. Hourly 24/7 means **8,760 runs/year**;
yearly estimate = 8,760 * per_run + 12 * (storage + BI + other monthly costs).
If workload scales up or hourly runs are cheaper than daily runs, model that
explicitly rather than blindly extrapolating. Use pricing for the selected region
on the date evaluated. Free Edition's $0 bill does not imply a $0 enterprise estimate.

## Finalization gate

`--final` refuses to produce a final-labelled PDF unless the required measurements,
passing-test confirmation, accepted DAG evidence, real PBIX and screenshots exist.
It cannot verify screenshots or attestations on your behalf: review the completed
report before submission. Do not set a missing field to a made-up value to bypass it.

Saad owns Power BI; Soban owns Phase 3 engineering and report. Earlier phases
were completed jointly by Soban and Saad. The final report records that split.
