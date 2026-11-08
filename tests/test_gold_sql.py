"""Execute the production analytical SELECTs in DuckDB, without mocking results.

DuckDB is an independent SQL engine, NOT proof of Spark/Delta compatibility.
Only the Spark `exchange` identifier is quoted for DuckDB's reserved keyword.
The real Delta integration checks live in notebooks/82_test_gold.py.
Run: python -m unittest discover -s tests -v
"""

import math
import statistics
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from marketpulse.gold_sql import daily_query, monthly_query, ticker_query


class GoldAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute("""CREATE TABLE prices (
            ticker VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, adj_close DOUBLE, volume BIGINT, batch_id VARCHAR)""")
        self.db.execute("CREATE TABLE tickers (ticker VARCHAR, ticker_key BIGINT)")
        self.db.execute("INSERT INTO tickers VALUES ('AAA', 1), ('BBB', 2)")
        self.days = []
        day = date(2026, 1, 1)
        while len(self.days) < 65:
            if day.weekday() < 5:
                self.days.append(day)
            day += timedelta(days=1)
        records = [(t, d, 100.0+i, 102.0+i, 99.0+i, 101.0+i,
                    80.0+i, (i+1)*100, "20260101T000000Z_full")
                   for t in ("AAA", "BBB") for i, d in enumerate(self.days)]
        self.db.executemany("INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", records)

    def tearDown(self):
        self.db.close()

    def daily(self):
        cursor = self.db.execute(daily_query("prices", "tickers"))
        names = [x[0] for x in cursor.description]
        return sorted([dict(zip(names, r)) for r in cursor.fetchall()], key=lambda r: (r["ticker_key"], r["trade_date"]))

    def test_grain_and_calendar_keys(self):
        rows = self.daily()
        self.assertEqual(130, len(rows))
        self.assertEqual(130, len({(r['ticker_key'], r['date_key']) for r in rows}))
        self.assertEqual(20260101, rows[0]["date_key"])

    def test_first_return_is_null_for_each_ticker(self):
        rows = self.daily()
        self.assertIsNone(rows[0]["daily_return"])
        self.assertIsNone(rows[65]["daily_return"])

    def test_return_uses_adjusted_price(self):
        self.assertAlmostEqual(81/80-1, self.daily()[1]["daily_return"])

    def test_absolute_return(self):
        self.db.execute("UPDATE prices SET adj_close=20 WHERE ticker='AAA' AND date=?", [self.days[10]])
        row = self.daily()[10]
        self.assertLess(row["daily_return"], 0)
        self.assertAlmostEqual(abs(20/89-1), row["abs_daily_return"])

    def test_ma7_requires_full_window(self):
        rows = self.daily()
        self.assertIsNone(rows[5]["ma_7"])
        self.assertAlmostEqual(statistics.mean(range(101, 108)), rows[6]["ma_7"])

    def test_ma30_matches_close_and_not_adjusted_close(self):
        rows = self.daily()
        self.assertIsNone(rows[28]["ma_30"])
        self.assertAlmostEqual(statistics.mean(range(101, 131)), rows[29]["ma_30"])

    def test_volatility_requires_30_returns_not_30_prices(self):
        rows = self.daily()
        self.assertIsNone(rows[29]["volatility_30"])
        expected = statistics.stdev((80+i)/(79+i)-1 for i in range(1, 31))
        self.assertAlmostEqual(expected, rows[30]["volatility_30"])
        self.assertAlmostEqual(expected*math.sqrt(252), rows[30]["annualized_volatility_30"])

    def test_volume_baseline_excludes_current(self):
        rows = self.daily()
        self.assertIsNone(rows[29]["relative_volume"])
        self.assertAlmostEqual(1550, rows[30]["avg_volume_prior_30"])
        self.assertAlmostEqual(3100/1550, rows[30]["relative_volume"])

    def test_windows_are_trading_observations_across_weekends(self):
        self.assertEqual(7, (self.days[6]-self.days[1]).days)
        self.assertAlmostEqual(104, self.daily()[6]["ma_7"])

    def test_tickers_do_not_share_window_history(self):
        self.db.execute("UPDATE prices SET close=1000 WHERE ticker='BBB'")
        rows = self.daily()
        self.assertAlmostEqual(104, rows[6]["ma_7"])
        self.assertAlmostEqual(1000, rows[71]["ma_7"])

    def test_zero_volume_baseline_is_null(self):
        self.db.execute("UPDATE prices SET volume=0 WHERE ticker='AAA' AND date<?", [self.days[30]])
        self.assertIsNone(self.daily()[30]["relative_volume"])

    def test_invalid_prices_do_not_make_infinite_returns(self):
        for bad in (0.0, -5.0, float('nan'), float('inf')):
            with self.subTest(price=bad):
                self.db.execute("UPDATE prices SET adj_close=? WHERE ticker='AAA' AND date=?", [bad, self.days[10]])
                rows = self.daily()
                self.assertIsNone(rows[10]["daily_return"])
                self.assertIsNone(rows[11]["daily_return"])

    def test_invalid_close_interrupts_moving_average(self):
        self.db.execute("UPDATE prices SET close=-1 WHERE ticker='AAA' AND date=?", [self.days[10]])
        rows = self.daily()
        self.assertIsNone(rows[10]["close"])
        self.assertIsNone(rows[16]["ma_7"])
        self.assertIsNotNone(rows[17]["ma_7"])

    def test_missing_volume_interrupts_baseline(self):
        self.db.execute("UPDATE prices SET volume=NULL WHERE ticker='AAA' AND date=?", [self.days[5]])
        self.assertIsNone(self.daily()[30]["relative_volume"])

    def test_negative_volume_is_null(self):
        self.db.execute("UPDATE prices SET volume=-10 WHERE ticker='AAA' AND date=?", [self.days[5]])
        self.assertIsNone(self.daily()[5]["volume"])

    def test_correction_recalculates_next_return_and_following_windows(self):
        before = self.daily()
        self.db.execute("UPDATE prices SET close=200, adj_close=150 WHERE ticker='AAA' AND date=?", [self.days[35]])
        after = self.daily()
        self.assertNotEqual(before[36]["daily_return"], after[36]["daily_return"])
        self.assertNotEqual(before[40]["ma_7"], after[40]["ma_7"])
        self.assertNotEqual(before[63]["volatility_30"], after[63]["volatility_30"])
        self.assertEqual(before[34], after[34])

    def test_backfill_recalculates_later_observations(self):
        self.db.execute("DELETE FROM prices WHERE ticker='AAA' AND date=?", [self.days[10]])
        before = self.daily()
        self.db.execute("INSERT INTO prices VALUES ('AAA', ?, 110, 112, 109, 111, 90, 1100, 'backfill')", [self.days[10]])
        after = self.daily()
        self.assertEqual(len(before)+1, len(after))
        self.assertAlmostEqual(91/90-1, after[11]["daily_return"])

    def test_unchanged_query_is_deterministic(self):
        self.assertEqual(self.daily(), self.daily())

    def monthly(self):
        self.db.execute("CREATE TABLE daily AS " + daily_query("prices", "tickers"))
        cursor = self.db.execute(monthly_query("daily"))
        names = [x[0] for x in cursor.description]
        return [dict(zip(names, r)) for r in cursor.fetchall()]

    def test_monthly_counts_and_volumes_reconcile(self):
        rows = self.monthly()
        self.assertEqual(130, sum(r['observation_count'] for r in rows))
        self.assertEqual(2*sum((i+1)*100 for i in range(65)), sum(r['total_volume'] for r in rows))
        self.assertTrue(all(r['date_key'] % 100 == 1 for r in rows))

    def test_monthly_correlation_matches_independent_statistics(self):
        row = next(r for r in self.monthly() if r['ticker_key']==1 and r['month_start']==date(2026,1,1))
        indices = [i for i,d in enumerate(self.days) if d.month==1 and i>0]
        expected = statistics.correlation([(i+1)*100 for i in indices], [(80+i)/(79+i)-1 for i in indices])
        self.assertAlmostEqual(expected, row['volume_return_correlation'])
        self.assertEqual(len(indices), row['volume_return_pairs'])

    def test_constant_volume_correlation_is_null(self):
        self.db.execute("UPDATE prices SET volume=100")
        self.assertTrue(all(r['volume_return_correlation'] is None for r in self.monthly()))

    def test_monthly_no_returns_has_null_correlation(self):
        self.db.execute("DELETE FROM prices WHERE date<>?", [self.days[0]])
        for row in self.monthly():
            self.assertIsNone(row['volume_return_correlation'])
            self.assertEqual(0, row['volume_return_pairs'])


class TickerKeyTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute('CREATE TABLE prices (ticker VARCHAR)')
        self.db.execute('''CREATE TABLE profiles (ticker VARCHAR, company_name VARCHAR, gics_sector VARCHAR,
            yf_sector VARCHAR, gics_sub_industry VARCHAR, yf_industry VARCHAR, "exchange" VARCHAR,
            currency VARCHAR, country VARCHAR, is_sp500_member BOOLEAN, info_as_of TIMESTAMP)''')
        self.db.execute('''CREATE TABLE existing (ticker_key BIGINT, ticker VARCHAR, company_name VARCHAR,
            sector VARCHAR, industry VARCHAR, "exchange" VARCHAR, currency VARCHAR, country VARCHAR,
            is_sp500_member BOOLEAN, info_as_of TIMESTAMP)''')

    def tearDown(self):
        self.db.close()

    def query(self):
        # EXCHANGE is a DuckDB keyword; quoting is the sole SQL adaptation.
        sql = ticker_query('prices', 'profiles', 'existing').replace('p.exchange', 'p."exchange"').replace('e.exchange', 'e."exchange"').replace('AS exchange', 'AS "exchange"')
        return self.db.execute(sql).fetchall()

    def test_missing_profile_gets_description_and_key(self):
        self.db.execute("INSERT INTO prices VALUES ('AAA')")
        row = self.query()[0]
        self.assertEqual((1, 'AAA', 'AAA', 'Unknown', 'Unknown'), row[:5])

    def test_old_keys_survive_lexically_earlier_new_ticker(self):
        self.db.execute("INSERT INTO existing VALUES (7, 'ZZZ', 'Zed', 'Tech', 'Software', 'X', 'USD', 'US', true, NULL)")
        self.db.execute("INSERT INTO prices VALUES ('AAA'), ('ZZZ')")
        keys = {r[1]: r[0] for r in self.query()}
        self.assertEqual({'ZZZ':7, 'AAA':8}, keys)

    def test_no_deletion_of_previously_assigned_key(self):
        self.db.execute("INSERT INTO existing VALUES (7, 'ZZZ', 'Zed', 'Tech', 'Software', 'X', 'USD', 'US', true, NULL)")
        self.assertEqual(7, self.query()[0][0])

    def test_gics_classification_has_priority(self):
        self.db.execute("INSERT INTO profiles VALUES ('AAA', 'Acme', 'GICS', 'Yahoo', 'Sub', 'Industry', 'X', 'USD', 'US', true, NULL)")
        self.assertEqual('GICS', self.query()[0][3])


if __name__ == '__main__':
    unittest.main()
