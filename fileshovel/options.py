# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import argparse
import logging
import platform
import re
import sys
from typing import List, Optional, TextIO

from fileshovel.csvreader import CsvReader
from fileshovel.lineio import TellableLineIO

log = logging.getLogger("fileshovel.options")

REMOVED_KEYS = {"csv_date_format", "csv_index_every_nth_line", "date_column", "index_file", "uuid_column"}


class ConfigError(Exception):
	pass


class FileShovelOptions:
	def __init__(self):
		self.args = argparse.Namespace()
		self._first_row = None
		self.parse_args()

		if self.dump_config:
			self.dump_config_as_yaml(sys.stdout)
			sys.exit(0)

	def _get_first_row(self) -> List[str]:
		csv_file = self.get_csv_file(for_header=True)

		try:
			for line, _, _ in CsvReader(csv_file, delimiter=self.csv_delimiter):
				return line
			raise ConfigError("%s has no header line and no columns are configured" % self.csv_file)
		finally:
			csv_file.close()

	def parse_args(self):
		config_parser = argparse.ArgumentParser(add_help=False)
		config_parser.add_argument("-c", "--config", default=None, type=str)
		config_args, _ = config_parser.parse_known_args()

		parser = argparse.ArgumentParser(
			prog="fileshovel",
		)
		dests = set()

		def add(*names, **kwargs):
			dests.add(parser.add_argument(*names, **kwargs).dest)

		add("-c", "--config", default=None, type=str,
			help=FileShovelOptions.config.__doc__)
		add("--columns", type=str, default=None,
			help=FileShovelOptions.columns.__doc__)
		add("--encoding", type=str, default="UTF-8",
			help=FileShovelOptions.encoding.__doc__)
		add("--wait-time", type=float, default=0.0,
			help=FileShovelOptions.wait_time.__doc__)
		add("--csv-regex-search", type=str, default=None,
			help=FileShovelOptions.csv_regex_search.__doc__)
		add("--csv-regex-replace", type=str, default=None,
			help=FileShovelOptions.csv_regex_replace.__doc__)
		add("-d", "--csv-delimiter", type=str, default=",",
			help=FileShovelOptions.csv_delimiter.__doc__)
		add("--add-missing-columns", default=False, action="store_true",
			help=FileShovelOptions.add_missing_columns.__doc__)
		add("--csv-skip-lines", type=int, default=None,
			help=FileShovelOptions.csv_skip_lines.__doc__)
		add("--csv-null-text", type=str, default="null",
			help=FileShovelOptions.csv_null_text.__doc__)
		add("--pg-connection-string", type=str)
		add("--pg-rows-per-commit", type=int, default=1000)
		add("--pg-schema", type=str)
		add("--pg-table", type=str)
		add("--pg-server-name-column", type=str)
		add("--pg-server-name-value", type=str)
		add("--pg-csv-offset-column", type=str)
		add("--pg-csv-line-column", type=str)
		add("--pg-threads", type=int, default=1,
			help=FileShovelOptions.pg_threads.__doc__)
		add("--dump-config", default=False, action="store_true",
			help=FileShovelOptions.dump_config.__doc__)
		add("--verbose", "-v", action="count", default=2,
			help="-v for INFO, -vv for DEBUG; 0 CRITICAL, 1 ERROR, 2 WARNING (default) in YAML.")
		add("-w", "--watch", default="inotify", type=str,
			help=FileShovelOptions.watch.__doc__)
		add("csv_file", type=str,
			help=FileShovelOptions.csv_file.__doc__)

		if config_args.config:
			parser.set_defaults(**self.read_config_from_yaml(config_args.config, dests))

		parser.parse_args(namespace=self.args)

	@property
	def config(self) -> str:
		"""YAML configuration file"""
		return self.args.config

	@property
	def dump_config(self) -> bool:
		"""dump a YAML configuration file of selected options"""
		return self.args.dump_config

	@staticmethod
	def read_config_from_yaml(path: str, dests: set) -> dict:
		from ruamel.yaml import YAML

		with open(path, "r") as config_file:
			yaml_config = YAML(typ="safe").load(config_file)

		for key in sorted(set(yaml_config) & REMOVED_KEYS):
			log.warning("ignoring removed option %s in %s", key, path)
			del yaml_config[key]

		unknown = sorted(set(yaml_config) - dests)
		if unknown:
			raise ConfigError("unknown options in %s: %s" % (path, ", ".join(unknown)))

		return yaml_config

	def dump_config_as_yaml(self, output: TextIO):
		config_keys = (x for x in dir(self.args) if x[0] != "_")
		config = {k: getattr(self.args, k) for k in config_keys}
		from ruamel.yaml import YAML
		yaml = YAML()
		yaml.indent()
		yaml.dump(config, output)

	@property
	def add_missing_columns(self) -> bool:
		""""if a column is missing at the end of row, add null fields"""
		return self.args.add_missing_columns

	@property
	def verbose(self) -> int:
		return self.args.verbose

	@property
	def columns(self) -> List[str]:
		"""comma separated columns if header is missing from csv, example: col1,col2,col3..."""
		if self.args.columns:
			return self.args.columns.split(',')
		else:
			return self.header

	@property
	def encoding(self) -> str:
		"""encoding used to read CSV file"""
		return self.args.encoding

	@property
	def wait_time(self) -> float:
		"""wait time in seconds between commits (default 0.0)"""
		return self.args.wait_time

	@property
	def header(self) -> List[str]:
		if self._first_row is None:
			self._first_row = self._get_first_row()
		return self._first_row

	@property
	def watch(self) -> str:
		"""wait for changes --watch=no|inotify|[delay in seconds]"""
		return self.args.watch

	@property
	def watch_delay(self) -> float:
		"""seconds between polls, 0 when not watching"""
		if self.watch in ("no", "0", "false"):
			return 0
		if self.watch == "inotify":
			return 1
		try:
			return float(self.watch)
		except ValueError:
			raise ConfigError("watch must be no, inotify or a delay in seconds, not %r" % self.watch) from None

	@property
	def csv_regex_search(self):
		"""apply the specified Python regex to each lines"""
		if self.args.csv_regex_search:
			return re.compile(bytes(self.args.csv_regex_search, self.encoding))

	@property
	def csv_regex_replace(self) -> str:
		"""apply the specified Python regex to each lines"""
		return self.args.csv_regex_replace

	@property
	def csv_delimiter(self) -> str:
		"""delimiter character between fields"""
		return self.args.csv_delimiter

	@property
	def csv_null_text(self) -> str:
		"""text to show for added null fields"""
		return self.args.csv_null_text

	@property
	def csv_skip_lines(self) -> int:
		"""how many lines to skip, by default 0 if csv-columns is set, otherwise 1"""
		if self.args.csv_skip_lines is None:
			if self.args.columns is None:
				return 1
			else:
				return 0
		else:
			return int(self.args.csv_skip_lines)

	@property
	def pg_connection_string(self) -> str:
		"""connection string for postgresql"""
		return self.args.pg_connection_string

	@property
	def pg_rows_per_commit(self) -> int:
		"""how many rows to insert at a time"""
		return self.args.pg_rows_per_commit

	@property
	def pg_schema(self) -> str:
		"""schema to store data"""
		return self.args.pg_schema

	@property
	def pg_table(self) -> str:
		"""table to store data"""
		return self.args.pg_table

	@property
	def pg_server_name_column(self) -> Optional[str]:
		"""column in --pg-table to store server name"""
		return self.args.pg_server_name_column

	@property
	def pg_server_name_value(self) -> str:
		"""value for server-name column, defaults to hostname"""
		return self.args.pg_server_name_value or platform.node()

	@property
	def pg_csv_offset_column(self) -> str:
		"""column in --pg-table to store offset as bigint"""
		return self.args.pg_csv_offset_column

	@property
	def pg_csv_line_column(self) -> str:
		"""column in --pg-table to store the CSV line, counted from where reading started"""
		return self.args.pg_csv_line_column

	@property
	def pg_threads(self) -> int:
		"""1 to insert on a worker thread while catching up, 0 to always insert synchronously"""
		if self.args.pg_threads not in (0, 1):
			raise ConfigError("pg_threads must be 0 or 1, parallel writers commit out of order and break resume")
		return self.args.pg_threads

	@property
	def csv_file(self) -> str:
		"""CSV filename to follow"""
		return self.args.csv_file

	def get_csv_file(self, for_header=False, on_idle=None) -> TellableLineIO:
		return TellableLineIO(
			self.args.csv_file,
			"r",
			self.encoding,
			0 if for_header else self.csv_skip_lines,
			watch=0 if for_header else self.watch_delay,
			use_inotify=self.watch == "inotify",
			regex_search=self.csv_regex_search,
			regex_replace=bytes(self.csv_regex_replace, self.encoding) if self.csv_regex_replace else None,
			on_idle=on_idle,
		)

	def get_csv_file_reader(self, last_offset=0, on_idle=None):
		return CsvReader(
			self.get_csv_file(on_idle=on_idle),
			last_offset=last_offset,
			delimiter=self.csv_delimiter,
		)
