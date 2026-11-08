"""One entry point for Raw -> Bronze -> Silver -> Gold, with one shared run id."""

import uuid

from marketpulse import gold, raw_to_bronze, silver_prices, silver_tickers


def run(spark, cfg, *, batch_ids=None, from_batch=None, to_batch=None,
        start_date=None, end_date=None, pipeline_run_id=None):
    run_id = pipeline_run_id or str(uuid.uuid4())
    batches = dict(batch_ids=batch_ids, from_batch=from_batch, to_batch=to_batch)
    logs = raw_to_bronze.run(spark, cfg, **batches, pipeline_run_id=run_id)
    logs += silver_prices.run(spark, cfg, **batches, start_date=start_date,
                              end_date=end_date, pipeline_run_id=run_id)
    logs += silver_tickers.run(spark, cfg, **batches, pipeline_run_id=run_id)
    # Always run Gold: this also repairs a previous failed Gold refresh when
    # Bronze/Silver have already completed and now have no pending batches.
    load_type = ("backfill" if any((batch_ids, from_batch, to_batch, start_date, end_date))
                 else "full" if any(log.load_type == "full" for log in logs) else "incremental")
    logs += gold.run(spark, cfg, pipeline_run_id=run_id, load_type=load_type)
    return logs
