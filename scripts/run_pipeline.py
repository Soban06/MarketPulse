"""Local daily runner: extract -> upload -> invoke the deployed Databricks Job.

Usage: python scripts/run_pipeline.py --job-id 123456
       python scripts/run_pipeline.py --job-id 123456 --skip-extract
Run `databricks bundle summary --profile marketpulse` to find the job id.
No credentials are stored in this repository. SDK uses the existing CLI profile.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--profile", default="marketpulse")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--skip-extract", action="store_true", help="Upload existing complete batches only")
    parser.add_argument("--period", help="Override source lookback, e.g. 10y for periodic reconciliation")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if not args.skip_extract:
        command = [sys.executable, str(root / "extract/ingest.py"), "--mode", args.mode]
        if args.period:
            command += ["--period", args.period]
        subprocess.run(command, cwd=root, check=True)
    subprocess.run([sys.executable, str(root / "extract/upload.py"), "--profile", args.profile,
                    "--catalog", args.catalog], cwd=root, check=True)
    from databricks.sdk import WorkspaceClient
    from datetime import timedelta
    client = WorkspaceClient(profile=args.profile)
    waiter = client.jobs.run_now(job_id=args.job_id, job_parameters={"catalog": args.catalog})
    print(f"Databricks run started: {waiter.run_id}", flush=True)
    result = waiter.result(timeout=timedelta(hours=3))
    from databricks.sdk.service.jobs import RunResultState
    if result.state.result_state != RunResultState.SUCCESS:
        raise RuntimeError(f"Pipeline did not succeed: {result.state.result_state}; {result.run_page_url}")
    print(f"SUCCESS - Gold is ready. {result.run_page_url}")


if __name__ == "__main__":
    main()
