"""
MarketPulse -- upload local landing batches to the raw Volume in Databricks.

    python extract/upload.py                                   # every complete local batch not yet uploaded
    python extract/upload.py --batch-id 20261010T132844Z_full  # one or more specific batches
    python extract/upload.py --batch-id <id> --overwrite       # re-upload a batch that is already there

Authenticates with a Databricks CLI profile, created once with:

    databricks auth login --host <workspace-url> --profile marketpulse

manifest.json is uploaded last, mirroring ingest.py: a batch in the Volume
without a manifest is incomplete, and raw-to-bronze ignores it. On
--overwrite the remote manifest is deleted first for the same reason.
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

REPO_ROOT = Path(__file__).resolve().parents[1]
LANDING_ROOT = REPO_ROOT / "data" / "landing"
sys.path.insert(0, str(REPO_ROOT / "src"))

from marketpulse.config import PipelineConfig  # noqa: E402

MANIFEST = "manifest.json"


def local_batches() -> list[str]:
    return sorted(p.parent.name for p in LANDING_ROOT.glob(f"*/{MANIFEST}"))


def remote_exists(w: WorkspaceClient, path: str) -> bool:
    try:
        w.files.get_metadata(path)
        return True
    except NotFound:
        return False


def upload_file(w: WorkspaceClient, local: Path, remote: str):
    with open(local, "rb") as f:
        w.files.upload(remote, f, overwrite=True)


def upload_batch(w: WorkspaceClient, cfg: PipelineConfig, batch_id: str, workers: int) -> bool:
    local_dir = LANDING_ROOT / batch_id
    remote_dir = cfg.batch_path(batch_id)
    files = sorted(p for p in local_dir.rglob("*") if p.is_file() and p.name != MANIFEST)
    total_bytes = sum(p.stat().st_size for p in files)
    print(f"  {batch_id}: {len(files)} files, {total_bytes / 1e6:.1f} MB -> {remote_dir}")

    if remote_exists(w, f"{remote_dir}/{MANIFEST}"):
        w.files.delete(f"{remote_dir}/{MANIFEST}")  # mark incomplete while files are replaced
    for sub in sorted({p.parent.relative_to(local_dir).as_posix() for p in files}):
        w.files.create_directory(remote_dir if sub == "." else f"{remote_dir}/{sub}")

    started, failed = time.time(), []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(upload_file, w, p, f"{remote_dir}/{p.relative_to(local_dir).as_posix()}"): p
            for p in files
        }
        for n, future in enumerate(as_completed(futures), start=1):
            try:
                future.result()
            except Exception as e:
                failed.append(futures[future])
                print(f"    FAILED {futures[future].relative_to(local_dir)}: {e}")
            if n % 100 == 0:
                print(f"    {n}/{len(files)} files")

    if failed:
        print(f"  {len(failed)} files failed; manifest NOT uploaded, so the batch stays incomplete. Re-run to retry.")
        return False

    upload_file(w, local_dir / MANIFEST, f"{remote_dir}/{MANIFEST}")
    print(f"  done in {time.time() - started:.0f}s")
    return True


def main():
    parser = argparse.ArgumentParser(description="Upload local landing batches to the Databricks raw Volume")
    parser.add_argument("--batch-id", nargs="+", help="Batch folder name(s) under data/landing (default: all new)")
    parser.add_argument("--overwrite", action="store_true", help="Re-upload batches that are already complete remotely")
    parser.add_argument("--catalog", default=PipelineConfig.catalog, help="Unity Catalog catalog (default: workspace)")
    parser.add_argument("--profile", default="marketpulse", help="Databricks CLI profile (default: marketpulse)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel file uploads")
    args = parser.parse_args()

    cfg = PipelineConfig(catalog=args.catalog)
    try:
        w = WorkspaceClient(profile=args.profile)
    except ValueError as e:
        raise SystemExit(
            f"Could not authenticate with profile '{args.profile}': {e}\n"
            f"Run once: databricks auth login --host <workspace-url> --profile {args.profile}"
        )

    available = local_batches()
    if args.batch_id:
        missing = [b for b in args.batch_id if b not in available]
        if missing:
            raise SystemExit(f"Not complete local batches (no manifest.json): {' '.join(missing)}")
        todo = args.batch_id
    else:
        todo = available

    if not args.overwrite:
        already = [b for b in todo if remote_exists(w, f"{cfg.batch_path(b)}/{MANIFEST}")]
        for b in already:
            print(f"  {b}: already in {cfg.landing_root}, skipping (use --overwrite to replace)")
        todo = [b for b in todo if b not in already]

    if not todo:
        print("Nothing to upload.")
        return

    print(f"Uploading {len(todo)} batch(es) to {cfg.landing_root}")
    results = [upload_batch(w, cfg, b, args.workers) for b in todo]
    if not all(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
