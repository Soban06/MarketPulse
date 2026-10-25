"""Export actual successful job outputs and Gold counts into the report inputs.

Requires the existing Databricks CLI profile and SDK. This never sets instructor
acceptance, fabricates a screenshot, or fills enterprise pricing assumptions.
"""

import argparse
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState, ViewsToExport
from databricks.sdk.service.sql import StatementState

ROOT = Path(__file__).resolve().parents[1]


def export_notebook(client, task_run_id, filename):
    exported = client.jobs.export_run(run_id=task_run_id, views_to_export=ViewsToExport.CODE)
    views = exported.views or []
    if not views or not views[0].content:
        raise RuntimeError("The job did not return a notebook export")
    content = views[0].content
    if not content.lstrip().startswith(("<!DOCTYPE", "<html", "<HTML")):
        import base64
        content = base64.b64decode(content).decode("utf-8")
    path = ROOT / "docs/run-evidence" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"Exported {path.relative_to(ROOT)}")
    return path


def notebook_stdout(path):
    """Databricks HTML embeds a percent-encoded JSON notebook model in base64."""
    import base64
    import re
    from urllib.parse import unquote
    match = re.search(r"var __DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", path.read_text(encoding="utf-8"))
    model = json.loads(unquote(base64.b64decode(match.group(1)).decode("utf-8")))
    for command in model.get("commands", []):
        for item in (command.get("results") or {}).get("data", []):
            if item.get("type") in ("ansi", "text"):
                yield item.get("data", "")


def successful_run(client, run_id):
    run = client.jobs.get_run(run_id=run_id)
    if not run.state or run.state.result_state != RunResultState.SUCCESS:
        raise RuntimeError(f"Run {run_id} has not succeeded: {run.state}")
    return run


def query(client, warehouse_id, statement):
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        import time
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(str(response.status.error))
    columns = [c.name for c in response.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in (response.result.data_array or [])]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="marketpulse")
    parser.add_argument("--pipeline-run-id", type=int)
    parser.add_argument("--test-run-id", type=int)
    parser.add_argument("--evidence-run-id", type=int)
    parser.add_argument("--rerun-id", type=int, help="Successful unchanged-input pipeline run")
    parser.add_argument("--warehouse-id", default="ce6164dbb634f66f")
    args = parser.parse_args()
    client = WorkspaceClient(profile=args.profile)
    path = ROOT / "docs/report/evidence.json"
    evidence = json.loads(path.read_text())
    if args.evidence_run_id:
        run = successful_run(client, args.evidence_run_id)
        task = next(t for t in run.tasks if t.task_key == "evidence")
        exported = export_notebook(client, task.run_id, "93_phase3_evidence.html")
        evidence["diagnostics_job_url"] = run.run_page_url
        for text in notebook_stdout(exported):
            if text.startswith("PHYSICAL PLAN"):
                import re
                redacted = re.sub(r"s3://[^\s\]]+", "<managed Delta storage path redacted>", text)
                redacted = "\n".join(line.rstrip() for line in redacted.splitlines()).rstrip()+"\n"
                (ROOT / "docs/run-evidence/93_monthly_aggregation_plan.txt").write_text(redacted, encoding="utf-8")
            if text.lstrip().startswith('{') and 'benchmark_before_seconds' in text:
                result, _ = json.JSONDecoder().raw_decode(text.lstrip())
                evidence['benchmark_before_seconds'] = result['benchmark_before_seconds']
                evidence['benchmark_after_seconds'] = result['benchmark_after_seconds']
    if args.rerun_id:
        run = successful_run(client, args.rerun_id)
        task = next(t for t in run.tasks if t.task_key == "pipeline")
        export_notebook(client, task.run_id, "04_pipeline_idempotency.html")
        rows = query(client, args.warehouse_id, f"""
            SELECT timestampdiff(MICROSECOND,min(start_time),max(end_time))/1000000.0 AS wall_seconds,
                   sum(duration_seconds) AS summed_step_seconds,
                   coalesce(sum(CASE WHEN layer='silver_to_gold' THEN rows_inserted END),0) AS gold_rows_inserted,
                   coalesce(sum(CASE WHEN layer='silver_to_gold' THEN rows_updated END),0) AS gold_rows_updated,
                   coalesce(sum(CASE WHEN layer='silver_to_gold' THEN rows_deleted END),0) AS gold_rows_deleted,
                   count(DISTINCT CASE WHEN layer='raw_to_bronze' THEN batch_id END) AS new_batches
            FROM workspace.ops.pipeline_execution_logs WHERE pipeline_run_id='{run.run_id}'
        """)[0]
        evidence['steady_state_refresh'] = {k:float(v) if 'seconds' in k else int(v) for k,v in rows.items()}
        evidence['steady_state_refresh']['run_id'] = str(run.run_id)
        if any(evidence['steady_state_refresh'][k] for k in ('gold_rows_inserted','gold_rows_updated','gold_rows_deleted')):
            raise RuntimeError("Unchanged-input Gold replay wrote business changes; inspect before claiming idempotency")
    if args.test_run_id:
        run = successful_run(client, args.test_run_id)
        task = next(t for t in run.tasks if t.task_key == "gold_tests")
        export_notebook(client, task.run_id, "82_test_gold.html")
        evidence["gold_test_run_passed"] = True
        evidence["gold_test_job_run_id"] = str(run.run_id)
    if args.pipeline_run_id:
        run = successful_run(client, args.pipeline_run_id)
        task = next(t for t in run.tasks if t.task_key == "pipeline")
        export_notebook(client, task.run_id, "04_run_pipeline.html")
        evidence["workspace_run_id"] = str(run.run_id)
        counts = query(client, args.warehouse_id, """
            SELECT (SELECT count(*) FROM workspace.gold.fact_daily_market) AS daily_rows,
                   (SELECT count(*) FROM workspace.gold.fact_monthly_market) AS monthly_rows,
                   (SELECT sum(observation_count) FROM workspace.gold.fact_monthly_market) AS aggregated_rows
        """)[0]
        evidence["gold_row_counts"] = {k:int(v) for k,v in counts.items()}
        # Numeric job id comes from the typed CLI argument, never arbitrary SQL.
        timing = query(client, args.warehouse_id, f"""
            SELECT timestampdiff(MICROSECOND,min(start_time),max(end_time))/1000000.0 AS wall_seconds,
                   sum(duration_seconds) AS summed_step_seconds
            FROM workspace.ops.pipeline_execution_logs
            WHERE pipeline_run_id='{run.run_id}' AND layer <> 'source_to_raw'
        """)[0]
        evidence["typical_incremental_run"].update(
            run_id=str(run.run_id), wall_seconds=float(timing['wall_seconds']),
            summed_step_seconds=float(timing['summed_step_seconds']))
        evidence["pipeline_job_url"] = run.run_page_url
    path.write_text(json.dumps(evidence, indent=2)+"\n", encoding="utf-8")
    print("Saved actual workspace evidence. Screenshots, billing and BI still require their real inputs.")


if __name__ == "__main__":
    main()
