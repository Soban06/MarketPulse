"""Local orchestration and schema contract checks; no remote workspace mutation."""

import ast
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from marketpulse import pipeline
from marketpulse.audit import StepLog, EXECUTION_LOG_SCHEMA
from marketpulse.config import PipelineConfig
from marketpulse.schemas import GOLD_TABLES, create_table_ddl


class PipelineContractTests(unittest.TestCase):
    def test_upstream_failure_prevents_gold(self):
        with patch.object(pipeline.raw_to_bronze, "run", side_effect=RuntimeError("failed source")), \
             patch.object(pipeline.gold, "run") as gold:
            with self.assertRaises(RuntimeError):
                pipeline.run(None, PipelineConfig())
            gold.assert_not_called()

    def test_silver_failure_prevents_gold(self):
        with patch.object(pipeline.raw_to_bronze, "run", return_value=[]), \
             patch.object(pipeline.silver_prices, "run", side_effect=RuntimeError("bad silver")), \
             patch.object(pipeline.gold, "run") as gold:
            with self.assertRaises(RuntimeError):
                pipeline.run(None, PipelineConfig())
            gold.assert_not_called()

    def test_gold_runs_when_silver_has_no_pending_batches(self):
        calls = []
        def stage(name):
            def run(*args, **kwargs):
                calls.append((name, kwargs["pipeline_run_id"]))
                return []
            return run
        with patch.object(pipeline.raw_to_bronze, "run", side_effect=stage("bronze")), \
             patch.object(pipeline.silver_prices, "run", side_effect=stage("prices")), \
             patch.object(pipeline.silver_tickers, "run", side_effect=stage("tickers")), \
             patch.object(pipeline.gold, "run", side_effect=stage("gold")):
            pipeline.run(None, PipelineConfig(), pipeline_run_id="shared")
        self.assertEqual([("bronze","shared"),("prices","shared"),("tickers","shared"),("gold","shared")], calls)

    def test_backfill_mode_is_passed_to_gold(self):
        with patch.object(pipeline.raw_to_bronze, "run", return_value=[]), \
             patch.object(pipeline.silver_prices, "run", return_value=[]), \
             patch.object(pipeline.silver_tickers, "run", return_value=[]), \
             patch.object(pipeline.gold, "run", return_value=[]) as gold:
            pipeline.run(None, PipelineConfig(), start_date="2020-01-01")
            self.assertEqual("backfill", gold.call_args.kwargs["load_type"])

    def test_gold_foreign_keys_and_config_are_isolated(self):
        cfg = PipelineConfig(gold_schema="test_gold")
        ddl = "\n".join(create_table_ddl(s, cfg) for s in GOLD_TABLES)
        self.assertNotIn("workspace.gold.", ddl)
        self.assertIn("REFERENCES workspace.test_gold.dim_ticker (ticker_key)", ddl)
        self.assertIn("REFERENCES workspace.test_gold.dim_date (date_key)", ddl)
        self.assertEqual(2, ddl.count("FOREIGN KEY (ticker_key)"))

    def test_all_test_notebook_schema_loops_are_isolated(self):
        # Adding gold to PipelineConfig.schemas must not make legacy test cleanup
        # target the production default. Inspect actual constructor arguments.
        for file in (ROOT/"notebooks").glob("*.py"):
            tree = ast.parse(file.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=="PipelineConfig":
                    keywords = {k.arg:k.value for k in node.keywords}
                    if file.name.startswith(("80_","81_","82_","91_")):
                        if "gold_schema" in keywords:
                            value=keywords["gold_schema"]
                            if isinstance(value,ast.Constant):
                                self.assertNotEqual("gold",value.value,file.name)
                        if file.name.startswith(("82_","91_")):
                            self.assertIn("gold_schema",keywords,file.name)

    def test_job_is_serial_and_passes_source_root(self):
        bundle = yaml.safe_load((ROOT/"databricks.yml").read_text())
        job = bundle["resources"]["jobs"]["marketpulse_pipeline"]
        self.assertEqual(1, job["max_concurrent_runs"])
        self.assertEqual([{"task_key":"setup"}], job["tasks"][1]["depends_on"])
        for task in job["tasks"]:
            self.assertIn("source_root",task["notebook_task"]["base_parameters"])

    def test_step_log_accepts_aggregate_and_delete_metrics(self):
        from datetime import datetime,timezone
        log = StepLog("id","run","silver_to_gold","incremental",None,None,None,
                      datetime.now(timezone.utc),None)
        log.record(rows_aggregated=3,rows_deleted=2)
        row=dict(zip(EXECUTION_LOG_SCHEMA.fieldNames(),log.as_row()))
        self.assertEqual(3,row['rows_aggregated'])
        self.assertEqual(2,row['rows_deleted'])


if __name__=="__main__":
    unittest.main()
