# Phase 3 - what Soban and Saad do next

Gold has been run successfully in your Databricks workspace. The isolated Gold
tests passed, and the complete pipeline processed the fresh 11,063-row batch.
Gold now has 1,230,497 daily rows and 59,004 monthly groups. Exports with actual
outputs are saved in docs/run-evidence. Power BI is Saad's part.

**Your immediate remaining work:** give Saad [the connection details](../../bi/CONNECTION.md)
and [his guide](POWER_BI_HANDOFF.md), then collect his PBIX and dashboard/model screenshots.
Your [DAG screenshot](DAG_SCREENSHOT.md) has been added to the report and instructor acceptance is confirmed.
The report is a DRAFT until the
remaining real screenshots and PBIX are supplied. The steps below let you reproduce
the completed engineering work or run future batches; you do not need to redo Phase 1/2.

## Soban: first run

1. Get these repository changes into your Databricks Git folder (or import the new files).
2. Open **03_silver_to_gold**. Select **Serverless**. Leave `catalog=workspace`.
3. Click **Run all**. If it cannot find `marketpulse`, put the full path to the repo's
   `src` folder in `source_root`, e.g. `/Workspace/Users/<you>/MarketPulse/src`.
4. When it prints **SUCCESS. Gold is ready for Power BI**, export the notebook as HTML
   **with outputs** to `docs/run-evidence/03_silver_to_gold.html`.
5. Run **82_test_gold**, also on Serverless, and export with outputs after it prints
   **ALL GOLD INTEGRATION CHECKS PASSED**. It uses separate test schemas.
6. Send Saad the SQL warehouse hostname, HTTP path and this repo's
   [Power BI guide](POWER_BI_HANDOFF.md). Saad uses his own authorized sign-in;
   never put passwords/tokens in GitHub or screenshots.

If a notebook fails, share its error. Do not repeat full source extraction just to test Gold.

## Soban: every new batch after that

Use **04_run_pipeline** instead of running 01 and 02 separately. It runs all layers,
including Gold, in order. With no batch/date parameters it picks up new batches.
For intentional backfills, set the same batch/date parameters used in Phase 2.
Gold always recomputes complete rolling history from current Silver.

To install the reproducible serverless job from your laptop:

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile marketpulse
databricks bundle validate --profile marketpulse
databricks bundle deploy --profile marketpulse
databricks bundle run marketpulse_pipeline --profile marketpulse
databricks bundle summary --profile marketpulse
```

The bundle runs setup, then 04, with one shared run id and no overlapping job runs.
It uploads source code, never `.env` or landing data. Deployment does not extract data.

For a single command that extracts, uploads and runs the deployed job:

```powershell
python scripts/run_pipeline.py --job-id <id-from-bundle-summary>
```

This is suitable for a daily Windows Task Scheduler task on a laptop that is on and
connected. No recurring task has been installed automatically. Do not schedule
independent 01/02/03 runs that could overlap. The CLI runner uploads all complete
local batches before starting the job and stops if extraction or upload fails.

## Soban: screenshots and report

1. Run **93_phase3_evidence** after a normal successful incremental 04/job run.
   Put that run id in `pipeline_run_id`. Save its HTML with outputs.
2. Open the query profile under the aggregation cell. Screenshot the graph and
   record its actual operators, timings and scan/shuffle statistics.
3. Instructor acceptance of the supplied Query Profile is already confirmed and recorded.
4. Optionally set `optimize=true` in 93 and run again for clustering/OPTIMIZE evidence.
   Keep both before and after measurements; improvements are not guaranteed.
5. Add screenshots under `docs/report/images/` and fill `docs/report/evidence.json`
   using actual results. The evidence notebook gives counts and timings. It does
   not supply paid-cloud DBUs or enterprise prices.
6. Ask Saad for `MarketPulse.pbix`, a dashboard screenshot and a model screenshot.
7. Rebuild the report: `python scripts/build_report.py`. Once all evidence is
   populated, `python scripts/build_report.py --final` checks required fields and
   creates the final PDF. See [report instructions](../report/README.md).

## Saad

Follow [POWER_BI_HANDOFF.md](POWER_BI_HANDOFF.md). Build the three proposal charts,
add KPIs and pipeline/error visuals, configure the relationships, and save the PBIX.
Refresh only after Soban confirms the entire pipeline run succeeded.

## Submission

- Repository: https://github.com/Soban06/MarketPulse (push the final reviewed changes).
- Saad's `bi/MarketPulse.pbix`.
- `output/pdf/MarketPulse_Architecture_Report.pdf`, generated with `--final`.
- Keep actual notebook exports and screenshots in the repo as evidence.

The present local checks do not certify that Gold has run in Databricks. Do not
submit the DRAFT PDF as the completed Phase 3 report.
