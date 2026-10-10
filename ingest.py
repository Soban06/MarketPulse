"""
MarketPulse -- Bronze layer ingestion (Yahoo Finance via yfinance + Wikipedia).

Every run lands raw JSON in its own folder so runs can be replayed in order:

    data/bronze/<run_id>/                 run_id = <UTC stamp>_<mode>, e.g. 20261010T120000Z_full
        sp500_constituents.json           Wikipedia S&P 500 table (GICS sector/sub-industry, date added, CIK, ...)
        prices/<TICKER>.json              daily OHLC, Adj Close, Volume, Dividends, Stock Splits
        company_info/<TICKER>.json        yfinance Ticker.info (sector, industry, market cap, PE, ...)
        manifest.json                     written last -- a run without it is incomplete and is not loaded

Modes:
    full          historical baseline for every ticker (default --period 10y)
    incremental   daily run. Re-pulls the last few weeks (default --period 1mo) rather than
                  just the newest day, because Yahoo rewrites past Adj Close values after
                  dividends and splits. load_silver.py MERGEs on (ticker, date) so those
                  revisions reach the warehouse.

Usage:
    python ingest.py --mode full
    python ingest.py --mode incremental
    python ingest.py --mode incremental --tickers AAPL MSFT     # subset, handy for testing
    python ingest.py --mode full --period max --skip-info

Ticker universe defaults to the current S&P 500 (~503 symbols) scraped from Wikipedia.
"""

import argparse
import io
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

BRONZE_ROOT = Path("data/bronze")
WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HTTP_HEADERS = {"User-Agent": "MarketPulse/1.0 (university data analytics project)"}

PRICE_COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
DEFAULT_PERIOD = {"full": "10y", "incremental": "1mo"}
SECONDS_BETWEEN_BATCHES = 2
RETRY_BACKOFF_SECONDS = [5, 20, 60]


def to_yahoo_symbol(symbol: str) -> str:
    # Wikipedia uses class-share dots (BRK.B), Yahoo uses dashes (BRK-B)
    return symbol.strip().upper().replace(".", "-")


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=_json_default)


def _json_default(o):
    if hasattr(o, "item"):  # numpy scalars
        return o.item()
    return str(o)


# ---------------------------------------------------------------- constituents

def fetch_constituents(run_dir: Path) -> list[dict]:
    """Scrape the S&P 500 table from Wikipedia; fall back to the latest saved copy."""
    try:
        resp = requests.get(WIKI_SP500_URL, headers=HTTP_HEADERS, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text))
        table = next(t for t in tables if "Symbol" in t.columns and "GICS Sector" in t.columns)
        records = table.astype(object).where(table.notna(), None).to_dict(orient="records")
        print(f"    {len(records)} constituents from Wikipedia")
    except Exception as e:
        previous = sorted(BRONZE_ROOT.glob("*/sp500_constituents.json"))
        if not previous:
            raise RuntimeError(f"Could not fetch S&P 500 list and no saved copy exists: {e}")
        with open(previous[-1], encoding="utf-8") as f:
            records = json.load(f)
        print(f"    Wikipedia fetch failed ({e}); reusing {previous[-1]}")

    write_json(run_dir / "sp500_constituents.json", records)
    return records


# ---------------------------------------------------------------------- prices

def download_batch(tickers: list[str], period: str) -> dict[str, pd.DataFrame]:
    for attempt, backoff in enumerate([0] + RETRY_BACKOFF_SECONDS):
        time.sleep(backoff)
        try:
            df = yf.download(
                tickers, period=period, group_by="ticker", auto_adjust=False,
                actions=True, threads=True, progress=False,
            )
            break
        except Exception as e:  # rate limits, network blips
            print(f"    batch attempt {attempt + 1} failed: {e}")
    else:
        return {}

    frames = {}
    if df is None or df.empty:
        return frames
    for ticker in tickers:
        if isinstance(df.columns, pd.MultiIndex):
            if ticker not in df.columns.get_level_values(0):
                continue
            sub = df[ticker]
        else:
            sub = df
        sub = sub.dropna(how="all", subset=[c for c in PRICE_COLS if c in sub.columns])
        if not sub.empty:
            frames[ticker] = sub
    return frames


def frame_to_records(ticker: str, df: pd.DataFrame) -> list[dict]:
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df.insert(0, "Ticker", ticker)
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")


def ingest_prices(tickers: list[str], period: str, run_dir: Path, batch_size: int):
    saved, rows = [], 0
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]

    def save(frames):
        nonlocal rows
        for ticker, df in frames.items():
            records = frame_to_records(ticker, df)
            write_json(run_dir / "prices" / f"{ticker}.json", records)
            saved.append(ticker)
            rows += len(records)

    for i, batch in enumerate(batches):
        print(f"    [{i + 1}/{len(batches)}] downloading {len(batch)} tickers (period={period})")
        save(download_batch(batch, period))
        if i < len(batches) - 1:
            time.sleep(SECONDS_BETWEEN_BATCHES)

    # one more try for anything that dropped out of its batch
    missing = [t for t in tickers if t not in saved]
    if missing:
        print(f"    retrying {len(missing)} missing tickers individually")
        for ticker in missing:
            save(download_batch([ticker], period))
            time.sleep(1)

    failed = [t for t in tickers if t not in saved]
    return sorted(saved), failed, rows


# ---------------------------------------------------------------- company info

def fetch_info(ticker: str) -> dict:
    last_error = None
    for backoff in [0] + RETRY_BACKOFF_SECONDS:
        time.sleep(backoff)
        try:
            info = yf.Ticker(ticker).info
            if info and len(info) > 5:
                return info
            last_error = "empty info payload"
        except Exception as e:
            last_error = e
    raise RuntimeError(last_error)


def ingest_company_info(tickers: list[str], run_dir: Path, workers: int):
    saved, failed = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_info, t): t for t in tickers}
        for n, future in enumerate(as_completed(futures), start=1):
            ticker = futures[future]
            try:
                info = future.result()
                info["_ticker"] = ticker
                info["_fetched_at"] = datetime.now(timezone.utc).isoformat()
                write_json(run_dir / "company_info" / f"{ticker}.json", info)
                saved.append(ticker)
            except Exception as e:
                print(f"    info FAILED for {ticker}: {e}")
                failed.append(ticker)
            if n % 100 == 0:
                print(f"    company info: {n}/{len(tickers)}")
    return sorted(saved), sorted(failed)


# ------------------------------------------------------------------------- run

def dir_size_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def run(mode: str, period: str, tickers: list[str] | None, skip_info: bool,
        batch_size: int, workers: int):
    started = datetime.now(timezone.utc)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{mode}"
    run_dir = BRONZE_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    print(f"Run {run_id} -> {run_dir}")

    print("[1/3] S&P 500 constituents")
    constituents = fetch_constituents(run_dir)
    if not tickers:
        tickers = sorted({to_yahoo_symbol(r["Symbol"]) for r in constituents})
    else:
        tickers = sorted({to_yahoo_symbol(t) for t in tickers})

    print(f"[2/3] Prices + dividends/splits for {len(tickers)} tickers")
    price_ok, price_failed, price_rows = ingest_prices(tickers, period, run_dir, batch_size)

    info_ok, info_failed = [], []
    if skip_info:
        print("[3/3] Company info skipped")
    else:
        print(f"[3/3] Company info for {len(tickers)} tickers")
        info_ok, info_failed = ingest_company_info(tickers, run_dir, workers)

    manifest = {
        "run_id": run_id,
        "mode": mode,
        "period": period,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "tickers_requested": len(tickers),
        "prices": {"tickers_saved": len(price_ok), "rows": price_rows, "failed": price_failed},
        "company_info": {"skipped": skip_info, "tickers_saved": len(info_ok), "failed": info_failed},
        "constituents": len(constituents),
        "size_bytes": dir_size_bytes(run_dir),
    }
    write_json(run_dir / "manifest.json", manifest)

    print(f"\nDone. {price_rows:,} price rows for {len(price_ok)} tickers, "
          f"{len(info_ok)} company profiles, {manifest['size_bytes'] / 1e6:.1f} MB in {run_dir}")
    if price_failed:
        print(f"Price download failed for: {' '.join(price_failed)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land raw S&P 500 market data in the bronze layer")
    parser.add_argument(
        "--mode", choices=["full", "incremental"], required=True,
        help="full = historical baseline, incremental = re-pull the last few weeks",
    )
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override the ticker universe (default: current S&P 500 constituents)",
    )
    parser.add_argument(
        "--period", default=None,
        help="yfinance period string (e.g. 10y, max, 1mo). Defaults: full=10y, incremental=1mo.",
    )
    parser.add_argument("--skip-info", action="store_true", help="Skip the company info pull")
    parser.add_argument("--batch-size", type=int, default=50, help="Tickers per yf.download call")
    parser.add_argument("--workers", type=int, default=4, help="Parallel company info requests")
    args = parser.parse_args()

    run(
        mode=args.mode,
        period=args.period or DEFAULT_PERIOD[args.mode],
        tickers=args.tickers,
        skip_info=args.skip_info,
        batch_size=args.batch_size,
        workers=args.workers,
    )
