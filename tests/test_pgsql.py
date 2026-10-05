# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import itertools
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from fileshovel.pgsql import PgLineInserter


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
	)
	for k, v in overrides.items():
		setattr(options, k, v)
	return options


def make_inserter(options):
	with patch.object(PgLineInserter, "get_last_offset_from_database", return_value=0):
		return PgLineInserter(options)


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
