"""Exercise the production batch-completion SQL against multiple job attempts."""

import sys
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from marketpulse.audit import AuditLogger


class SQLResult:
    def __init__(self,rows):
        self.rows=rows

    def collect(self):
        return [{"batch_id":r[0]} for r in self.rows]


class SQLSession:
    def __init__(self,db):
        self.db=db

    def sql(self,statement,args):
        for key in args:
            statement=statement.replace(":"+key,"$"+key)
        return SQLResult(self.db.execute(statement,args).fetchall())


class AuditRetrySQLTests(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect()
        self.db.execute("""CREATE TABLE logs (log_id VARCHAR,pipeline_run_id VARCHAR,
            layer VARCHAR,batch_id VARCHAR,target_table VARCHAR,start_time TIMESTAMP,status VARCHAR)""")
        self.audit=AuditLogger.__new__(AuditLogger)
        self.audit.spark=SQLSession(self.db)
        self.audit.table="logs"

    def tearDown(self):
        self.db.close()

    def add(self,id,run,target,time,status):
        self.db.execute("INSERT INTO logs VALUES (?,?,'bronze_to_silver','batch',?,?,?)",[id,run,target,time,status])

    def test_successful_retry_with_same_job_id_completes_batch(self):
        self.add('1','job','fact','2026-01-01 00:00:00','FAILED')
        self.add('2','job','fact','2026-01-01 00:01:00','SUCCESS')
        self.assertEqual({'batch'},self.audit.completed_batches('bronze_to_silver'))

    def test_latest_failed_attempt_is_still_pending(self):
        self.add('1','job','fact','2026-01-01 00:00:00','SUCCESS')
        self.add('2','job','fact','2026-01-01 00:01:00','FAILED')
        self.assertEqual(set(),self.audit.completed_batches('bronze_to_silver'))

    def test_target_success_cannot_complete_other_target(self):
        self.add('1','job','quarantine','2026-01-01 00:00:00','SUCCESS')
        self.add('2','job','fact','2026-01-01 00:01:00','RUNNING')
        self.assertEqual(set(),self.audit.completed_batches('bronze_to_silver'))
        self.assertEqual({'batch'},self.audit.completed_batches('bronze_to_silver','quarantine'))
        self.assertEqual(set(),self.audit.completed_batches('bronze_to_silver','fact'))

    def test_latest_job_supersedes_old_failure(self):
        self.add('1','old','fact','2026-01-01 00:00:00','FAILED')
        self.add('2','new','fact','2026-01-02 00:00:00','SUCCESS')
        self.assertEqual({'batch'},self.audit.completed_batches('bronze_to_silver'))


if __name__=='__main__':
    unittest.main()
