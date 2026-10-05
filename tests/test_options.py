# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import os
import tempfile
from unittest import TestCase
from unittest.mock import patch

from fileshovel.options import ConfigError, FileShovelOptions


class YamlConfigTest(TestCase):

	def setUp(self):
		self.scratch = tempfile.TemporaryDirectory(dir=os.path.dirname(__file__), prefix="scratch-")
		self.config = os.path.join(self.scratch.name, "fileshovel.yaml")
		self.csv = os.path.join(self.scratch.name, "Master.csv")

	def tearDown(self):
		self.scratch.cleanup()

	def _load(self, yaml: str, *argv: str) -> FileShovelOptions:
		with open(self.config, "w") as f:
			f.write(yaml)
		with patch("sys.argv", ["fileshovel", "-c", self.config] + list(argv) + [self.csv]):
			return FileShovelOptions()

	def test_unknownKey_load_refusesNamingIt(self):
		with self.assertRaisesRegex(ConfigError, "pg_csv_line_colum"):
			self._load("pg_csv_line_colum: csv_line\n")

	def test_removedKey_load_warnsAndIgnores(self):
		with self.assertLogs("fileshovel.options", "WARNING") as logs:
			self._load("uuid_column: aleg_uuid\n")
		self.assertIn("uuid_column", logs.output[0])

	def test_quotedNumber_load_convertedByOptionType(self):
		self.assertEqual(self._load('pg_rows_per_commit: "10000"\n').pg_rows_per_commit, 10000)

	def test_commandLine_overridesYaml(self):
		self.assertEqual(self._load("pg_rows_per_commit: 5\n", "--pg-rows-per-commit", "7").pg_rows_per_commit, 7)

	def test_numericWatch_watchDelay_keepsValue(self):
		self.assertEqual(self._load("watch: '2.5'\n").watch_delay, 2.5)

	def test_noColumnsAndEmptyFile_columns_refuses(self):
		open(self.csv, "w").close()
		options = self._load("pg_table: t\n")
		with self.assertRaises(ConfigError):
			options.columns
