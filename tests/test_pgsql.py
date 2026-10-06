# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import itertools
import os
import threading
from types import SimpleNamespace
from unittest import TestCase, skipUnless
from unittest.mock import MagicMock, patch

import psycopg2.errors

from fileshovel import __main__ as fileshovel_main
from fileshovel.pgsql import InsertError, PgLineInserter


def make_options(**overrides):
	options = SimpleNamespace(
		columns=["a", "b"],
		pg_server_name_column=None,
		pg_server_name_value="host",
		pg_csv_offset_column="csv_offset",
		pg_csv_line_column=None,
		pg_rows_per_commit=10,
		pg_threads=0,
		pg_schema=None,
		pg_table="t",
		pg_connection_string="",
		add_missing_columns=False,
		csv_null_text=None,
		wait_time=0,
		verbose=0,
	)
	for k, v in overrides.items():
		setattr(options, k, v)
	return options


def make_inserter(options):
	with patch.object(PgLineInserter, "connect_database"):
		return PgLineInserter(options)


def without_rendering():
	"""Literal quoting needs a live connection; tests of control flow skip it."""
	return patch.object(PgLineInserter, "_render_insert", return_value=("", ([], [])))


class PrepareRowTest(TestCase):

	def test_everyExtraColumnCombination_prepareRow_oneValuePerColumn(self):
		for line_column, server_column in itertools.product((None, "csv_line"), (None, "server")):
			with self.subTest(line_column=line_column, server_column=server_column):
				inserter = make_inserter(make_options(
					pg_csv_line_column=line_column,
					pg_server_name_column=server_column,
				))
				row = inserter._prepare_row((["1", "2"], 7, 42))
				self.assertEqual(len(row), len(inserter.columns))


class ConnectTest(TestCase):

	def test_connectionStringSetsTimeout_connect_keepsItAndAddsOthers(self):
		inserter = make_inserter(make_options(pg_connection_string="host=db connect_timeout=99"))

		with patch("psycopg2.connect") as connect:
			inserter.connect_database()

		(dsn,), kwargs = connect.call_args
		self.assertEqual(dsn, "host=db connect_timeout=99")
		self.assertNotIn("connect_timeout", kwargs)
		self.assertIn("keepalives_idle", kwargs)


class ConflictTest(TestCase):

	def test_someRowsAlreadyPresent_insertBatch_warnsWithCount(self):
		inserter = make_inserter(make_options())
		inserter._connection.cursor.return_value.__enter__.return_value.rowcount = 7
		batch = [(i + 1, i * 10, ["x", "y", i * 10]) for i in range(10)]

		with without_rendering(), self.assertLogs("fileshovel.pgsql", "WARNING") as logs:
			inserter._insert_batch(batch)

		self.assertIn("3 of 10 rows at offsets 0-90", logs.output[0])


class InsertFailureTest(TestCase):

	def _run_main(self, options, rows_before_idle, rows_after_idle, failing_execute):
		def reader(last_offset, on_idle):
			for i in range(rows_before_idle + rows_after_idle):
				if i == rows_before_idle:
					on_idle()
				yield ["x", "y"], i + 1, i * 10

		options.get_csv_file_reader = reader
		connection = MagicMock()
		cursor = connection.cursor.return_value.__enter__.return_value
		cursor.rowcount = options.pg_rows_per_commit
		execute = cursor.execute
		calls = []

		def fake_execute(sql):
			calls.append(sql)
			if len(calls) == failing_execute:
				raise psycopg2.errors.InvalidTextRepresentation("rejected")

		execute.side_effect = fake_execute
		result = {}

		def run():
			result["status"] = fileshovel_main.main()

		with patch.object(fileshovel_main, "FileShovelOptions", return_value=options), \
				patch.object(PgLineInserter, "connect_database", return_value=connection), \
				patch.object(PgLineInserter, "get_last_offset_from_database", return_value=0), \
				without_rendering(), \
				self.assertLogs("fileshovel.main", "ERROR") as logs:
			thread = threading.Thread(target=run, daemon=True)
			thread.start()
			thread.join(timeout=10)

		self.assertFalse(thread.is_alive(), "main() did not return after the insert failed")
		self.assertEqual(result["status"], 1)
		return logs.output

	def test_bulkPhase_firstInsertFails_exitsWithContext(self):
		output = self._run_main(make_options(pg_threads=1), 1000, 0, failing_execute=1)
		self.assertEqual(len(output), 1)
		self.assertIn("insert into t failed for offsets 0-90, lines 1-10", output[0])
		self.assertIn("InvalidTextRepresentation", output[0])

	def test_tailPhase_insertAfterIdleFails_exits(self):
		output = self._run_main(make_options(pg_threads=1), 25, 5, failing_execute=4)
		self.assertIn("offsets 250-290, lines 26-30", output[0])

	def test_synchronous_insertFails_exits(self):
		output = self._run_main(make_options(pg_threads=0), 1000, 0, failing_execute=2)
		self.assertIn("offsets 100-190", output[0])

	def test_shortRow_exitsWithOffset(self):
		options = make_options(columns=["a", "b", "c"])
		output = self._run_main(options, 1, 0, failing_execute=0)
		self.assertIn("row at offset 0, line 1", output[0])


@skipUnless(os.environ.get("FILESHOVEL_TEST_DSN"), "set FILESHOVEL_TEST_DSN to run against PostgreSQL")
class DatabaseTest(TestCase):

	def setUp(self):
		self.inserter = PgLineInserter(make_options(
			columns=["caller", "dir"],
			pg_table="cdr_test",
			pg_csv_line_column="csv_line",
			pg_server_name_column="server_name",
			pg_connection_string=os.environ["FILESHOVEL_TEST_DSN"],
		))
		with self.inserter._connection.cursor() as cursor:
			cursor.execute("CREATE TYPE pg_temp.direction AS ENUM ('inbound')")
			cursor.execute(
				"CREATE TEMP TABLE cdr_test (caller text, dir pg_temp.direction, csv_offset bigint,"
				" csv_line int, server_name text)"
			)

	def tearDown(self):
		self.inserter._connection.close()

	def test_rejectedEnumValue_insert_namesRowAndColumnWithoutValue(self):
		batch = [
			(1, 0, self.inserter._prepare_row((["été", "inbound"], 1, 0))),
			(2, 20, self.inserter._prepare_row((["5550001234", "sideways"], 2, 20))),
		]

		with self.assertRaises(InsertError) as raised:
			self.inserter._insert_batch(batch)

		message = str(raised.exception)
		self.assertIn("row at offset 20, line 2, column dir", message)
		self.assertNotIn("sideways", message)
