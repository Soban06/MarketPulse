"""Gold SELECT statements. Pure Python so analytical SQL can also be tested locally.

Arguments are validated, fully qualified identifiers supplied by PipelineConfig,
or generated temporary-view names. User values must never be interpolated here.
"""


def ticker_query(prices: str, profiles: str, existing: str) -> str:
    """Retain all allocated keys; assign consecutive keys only to previously unseen symbols."""
    return f"""
        WITH universe AS (
            SELECT ticker FROM {prices}
            UNION SELECT ticker FROM {profiles}
            UNION SELECT ticker FROM {existing}
        ), new_keys AS (
            SELECT u.ticker,
                   CAST((SELECT coalesce(max(ticker_key), 0) FROM {existing})
                     + row_number() OVER (ORDER BY u.ticker) AS BIGINT) AS ticker_key
            FROM universe u LEFT JOIN {existing} e ON u.ticker = e.ticker
            WHERE e.ticker IS NULL
        ), keys AS (
            SELECT ticker, ticker_key FROM {existing}
            UNION ALL SELECT ticker, ticker_key FROM new_keys
        )
        SELECT k.ticker_key, k.ticker,
               coalesce(p.company_name, e.company_name, k.ticker) AS company_name,
               coalesce(p.gics_sector, p.yf_sector, e.sector, 'Unknown') AS sector,
               coalesce(p.gics_sub_industry, p.yf_industry, e.industry, 'Unknown') AS industry,
               coalesce(p.exchange, e.exchange, 'Unknown') AS exchange,
               coalesce(p.currency, e.currency, 'Unknown') AS currency,
               coalesce(p.country, e.country, 'Unknown') AS country,
               coalesce(p.is_sp500_member, e.is_sp500_member, false) AS is_sp500_member,
               coalesce(p.info_as_of, e.info_as_of) AS info_as_of
        FROM keys k
        LEFT JOIN {profiles} p ON k.ticker = p.ticker
        LEFT JOIN {existing} e ON k.ticker = e.ticker
    """


def daily_query(prices: str, tickers: str) -> str:
    """Compute windows on full history BEFORE any dashboard date filter.

Bad/non-finite/non-positive prices and negative volume become NULL analytical
values, preserving the Silver observation without producing misleading metrics.
No IGNORE NULLS is used: missing adjusted prices interrupt adjacent returns.
"""
    clean_prices = ", ".join(
        f"CASE WHEN {c} > 0 AND NOT isnan({c}) AND abs({c}) < CAST('Infinity' AS DOUBLE) "
        f"THEN {c} END AS {c}" for c in ("open", "high", "low", "close", "adj_close")
    )
    return f"""
        WITH clean AS (
            SELECT ticker, date AS trade_date, {clean_prices},
                   CASE WHEN volume >= 0 THEN volume END AS volume, batch_id
            FROM {prices}
        ), previous AS (
            SELECT *, lag(adj_close) OVER (PARTITION BY ticker ORDER BY trade_date) AS previous_adj_close
            FROM clean
        ), returns AS (
            SELECT *, adj_close / nullif(previous_adj_close, 0) - 1 AS daily_return
            FROM previous
        ), rolling AS (
            SELECT *,
                CASE WHEN count(close) OVER w7 = 7 THEN avg(close) OVER w7 END AS ma_7,
                CASE WHEN count(close) OVER w30 = 30 THEN avg(close) OVER w30 END AS ma_30,
                CASE WHEN count(daily_return) OVER w30 = 30
                     THEN stddev_samp(daily_return) OVER w30 END AS volatility_30,
                CASE WHEN count(volume) OVER prior30 = 30
                     THEN avg(volume) OVER prior30 END AS avg_volume_prior_30
            FROM returns
            WINDOW w7 AS (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
                   w30 AS (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
                   prior30 AS (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)
        )
        SELECT d.ticker_key,
               CAST(year(r.trade_date) * 10000 + month(r.trade_date) * 100 + day(r.trade_date) AS BIGINT) AS date_key,
               r.trade_date, r.open, r.high, r.low, r.close, r.adj_close, r.volume,
               r.daily_return, abs(r.daily_return) AS abs_daily_return,
               r.ma_7, r.ma_30, r.volatility_30,
               r.volatility_30 * sqrt(252.0) AS annualized_volatility_30,
               r.avg_volume_prior_30,
               r.volume / nullif(r.avg_volume_prior_30, 0) AS relative_volume,
               r.batch_id
        FROM rolling r JOIN {tickers} d ON r.ticker = d.ticker
    """


def monthly_query(daily: str) -> str:
    """A genuine aggregate for analytical reads; correlations are pairwise complete."""
    return f"""
        WITH grouped AS (
            SELECT ticker_key, CAST(date_trunc('MONTH', trade_date) AS DATE) AS month_start,
                   count(*) AS observation_count, CAST(sum(volume) AS BIGINT) AS total_volume,
                   avg(abs_daily_return) AS avg_abs_daily_return,
                   CASE WHEN stddev_samp(volume) FILTER (WHERE abs_daily_return IS NOT NULL) > 0
                              AND stddev_samp(abs_daily_return) FILTER (WHERE volume IS NOT NULL) > 0
                        THEN corr(volume, abs_daily_return) END AS volume_return_correlation,
                   CASE WHEN stddev_samp(relative_volume) FILTER (WHERE abs_daily_return IS NOT NULL) > 0
                              AND stddev_samp(abs_daily_return) FILTER (WHERE relative_volume IS NOT NULL) > 0
                        THEN corr(relative_volume, abs_daily_return) END AS relative_volume_return_correlation,
                   count(CASE WHEN volume IS NOT NULL AND abs_daily_return IS NOT NULL THEN 1 END) AS volume_return_pairs,
                   count(CASE WHEN relative_volume IS NOT NULL AND abs_daily_return IS NOT NULL THEN 1 END) AS relative_volume_return_pairs,
                   min(trade_date) AS first_observation, max(trade_date) AS last_observation
            FROM {daily} GROUP BY ticker_key, CAST(date_trunc('MONTH', trade_date) AS DATE)
        )
        SELECT ticker_key,
               CAST(year(month_start) * 10000 + month(month_start) * 100 + 1 AS BIGINT) AS date_key,
               month_start, observation_count, total_volume, avg_abs_daily_return,
               volume_return_correlation, relative_volume_return_correlation,
               volume_return_pairs, relative_volume_return_pairs, first_observation, last_observation
        FROM grouped
    """
