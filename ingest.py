"""
yfinance stock data ingestion script — Bronze layer landing.

Replaces the Alpha Vantage TIME_SERIES_DAILY approach: Alpha Vantage's
free tier now gates outputsize=full behind a premium plan, so we use
yfinance instead, which gives full historical OHLCV for free with no
API key and no meaningful rate limit.

Usage:
    python ingest_yf.py --mode full --tickers AAPL MSFT GOOGL AMZN TSLA NVDA META JPM
    python ingest_yf.py --mode incremental --tickers AAPL MSFT GOOGL AMZN TSLA NVDA META JPM

--period can override the default (5y for full, 5d for incremental),
e.g. --period 10y if you want a bigger historical baseline.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

BRONZE_ROOT = Path("data/bronze")
SECONDS_BETWEEN_CALLS = 1  # polite pause, not a hard requirement


def fetch_and_save(ticker: str, period: str, mode: str):
    df = yf.download(ticker, period=period, progress=False, auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"No data returned for {ticker} (period={period})")

    # yfinance can return MultiIndex columns even for a single ticker
    # depending on version -- flatten to plain column names.
    if isinstance(df.columns, __import__("pandas").MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.reset_index()
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    records = df.to_dict(orient="records")

    subdir = "full_load" if mode == "full" else "incremental"
    out_dir = BRONZE_ROOT / subdir
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{ticker}_{mode}_{stamp}.json"

    with open(out_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    return out_path, len(records)


def run(tickers: list[str], mode: str, period: str):
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Fetching {mode} load for {ticker} (period={period})...")
        try:
            path, n = fetch_and_save(ticker, period, mode)
            print(f"    saved {n} daily records -> {path}")
        except Exception as e:
            print(f"    FAILED for {ticker}: {e}")

        if i < len(tickers) - 1:
            time.sleep(SECONDS_BETWEEN_CALLS)

    print("\nDone. Raw JSON landed under data/bronze/.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest stock data from Yahoo Finance via yfinance")
    parser.add_argument(
        "--mode", choices=["full", "incremental"], required=True,
        help="full = historical baseline, incremental = latest few days",
    )
    parser.add_argument(
        "--tickers", nargs="+", required=True,
        help="Space-separated ticker symbols, e.g. AAPL MSFT GOOGL",
    )
    parser.add_argument(
        "--period", default=None,
        help="yfinance period string (e.g. 5y, 1mo, 5d). Defaults to 5y for full, 5d for incremental.",
    )
    args = parser.parse_args()

    period = args.period or ("5y" if args.mode == "full" else "5d")
    run(args.tickers, args.mode, period)