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


class FileShovelOptions:
	def __init__(self):
		self.args = argparse.Namespace()
		self.parse_args()

		if self.dump_config:
			self.dump_config_as_yaml(sys.stdout)
			sys.exit(0)

		self._first_row = self._get_first_row()

	def _get_first_row(self) -> List[str]:
		reader = self.get_csv_file_reader(for_header=True)

		try:
			line = next(iter(reader))
			return line
		finally:
			reader.csv_file.close()

	def parse_args(self):
		parser = argparse.ArgumentParser(
			prog="fileshovel",
		)
		parser.add_argument("-c", "--config", default=None, type=str,
							help=FileShovelOptions.config.__doc__)

		if "--help" not in sys.argv:
			parser.parse_known_args(namespace=self.args)

			if self.args.config:
				self.read_config_from_yaml()

		parser.add_argument("--columns", type=str, default=None,
							help=FileShovelOptions.columns.__doc__)
		parser.add_argument("--date-column", type=str, default=None,
							help=FileShovelOptions.date_column.__doc__)
		parser.add_argument("--encoding", type=str, default="UTF-8",
							help=FileShovelOptions.encoding.__doc__)
		parser.add_argument("--wait-time", type=float, default=0.0,
							help=FileShovelOptions.wait_time.__doc__)
		parser.add_argument("--uuid-column", type=str, default=None,
							help=FileShovelOptions.uuid_column.__doc__)
		parser.add_argument("--csv-regex-search", type=str, default=None,
							help=FileShovelOptions.csv_regex_search.__doc__)
		parser.add_argument("--csv-regex-replace", type=str, default=None,
							help=FileShovelOptions.csv_regex_replace.__doc__)
		parser.add_argument("--csv-date-format", type=str, default="%Y-%m-%d %H:%M:%S",
							help=FileShovelOptions.csv_date_format.__doc__)
		parser.add_argument("-d", "--csv-delimiter", type=str, default=",",
							help=FileShovelOptions.csv_delimiter.__doc__)
		parser.add_argument("--csv-index-every-nth-line", type=int, default=None,
							help=FileShovelOptions.csv_index_every_nth_line.__doc__)
		parser.add_argument("--add-missing-columns", type=bool, default=False,
							help=FileShovelOptions.add_missing_columns.__doc__)
		parser.add_argument("--csv-skip-lines", type=int, default=None,
							help=FileShovelOptions.csv_skip_lines.__doc__)
		parser.add_argument("--csv-null-text", type=str, default="null",
							help=FileShovelOptions.csv_null_text.__doc__)
		parser.add_argument("--pg-connection-string", type=str)
		parser.add_argument("--pg-rows-per-commit", type=int, default=1000)
		parser.add_argument("--pg-schema", type=str)
		parser.add_argument("--pg-table", type=str)
		parser.add_argument("--pg-server-name-column", type=str)
		parser.add_argument("--pg-server-name-value", type=str)
		parser.add_argument("--pg-csv-offset-column", type=str)
		parser.add_argument("--pg-csv-line-column", type=str)
		parser.add_argument("--pg-threads", type=int, default=1)
		parser.add_argument("--dump-config", default=False, action="store_true",
							help=FileShovelOptions.dump_config.__doc__)
		parser.add_argument("-i", "--index-file", default=None, type=str,
							help=FileShovelOptions.index_file.__doc__)
		parser.add_argument("-w", "--watch", default="inotify", type=str,
							help=FileShovelOptions.watch.__doc__)
		parser.add_argument("csv_file", type=str,
							help=FileShovelOptions.csv_file.__doc__)
		parser.parse_args(namespace=self.args)

	@property
	def config(self) -> str:
		"""YAML configuration file"""
		return self.args.config

	@property
	def dump_config(self) -> bool:
		"""dump a YAML configuration file of selected options"""
		return self.args.dump_config

	def read_config_from_yaml(self):
		if self.config:
			from ruamel.yaml import YAML
			yaml_config = YAML(typ="safe").load(open(self.config, "r"))
			for key in yaml_config:
				if hasattr(self.args, key) is False:
					setattr(self.args, key, yaml_config[key])

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
	def columns(self) -> List[str]:
		"""comma separated columns if header is missing from csv, example: col1,col2,col3..."""
		if self.args.columns:
			return self.args.columns.split(',')
		else:
			return self.header

	@property
	def date_column(self) -> int:
		"""column to index for date, default is first column"""
		if self.args.date_column is None:
			return 0
		else:
			return self.columns.index(self.args.date_column) - 1

	@property
	def date_column_name(self) -> str:
		"""date column as string"""
		return self.args.date_column

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
		assert self._first_row is not None
		return self._first_row

	@property
	def watch(self) -> str:
		"""wait for changes --wait=no|inotify|[delay in seconds]"""
		return self.args.watch

	@property
	def uuid_column(self) -> Optional[int]:
		"""column to index for uuid, default is None"""
		if self.args.uuid_column:
			return self.columns.index(self.args.uuid_column) - 1

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
	def csv_date_format(self) -> str:
		"""date format string according to strptime:
		https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes"""
		return self.args.csv_date_format

	@property
	def csv_delimiter(self) -> str:
		"""delimiter character between fields"""
		return self.args.csv_delimiter

	@property
	def csv_index_every_nth_line(self) -> Optional[int]:
		"""index the CSV file storing an offset every nth lines"""
		return self.args.csv_index_every_nth_line

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
		"""column in --pg-table to store CSV current line as int"""
		return self.args.pg_csv_line_column

	@property
	def pg_threads(self) -> int:
		"""how many parallel SQL threads to run"""
		return self.args.pg_threads

	@property
	def csv_file(self) -> str:
		"""CSV filename to follow"""
		return self.args.csv_file

	@property
	def index_file(self) -> str:
		"""index file, default is CSV_FILE.index"""
		if self.args.index_file is None:
			return self.args.csv_file + ".index"
		else:
			return self.args.index_file

	def get_csv_file(self, for_header=False) -> TellableLineIO:
		return TellableLineIO(
			self.args.csv_file,
			"r",
			self.encoding,
			self.csv_skip_lines if for_header is False else 0,
			self.csv_index_every_nth_line,
			watch=self.watch not in ("no", "0", "false"),
			use_inotify=self.watch == "inotify",
			regex_search=self.csv_regex_search,
			regex_replace=bytes(self.csv_regex_replace, self.encoding) if self.csv_regex_replace else None,
		)

	def get_csv_file_reader(self, for_header=False, last_offset=0):
		return CsvReader(
			self.get_csv_file(for_header),
			last_offset=last_offset,
			delimiter=self.csv_delimiter,
		)
