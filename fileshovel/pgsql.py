# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import logging
import time
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

import psycopg2
from psycopg2.extensions import libpq_version, parse_dsn
from psycopg2.sql import Identifier, SQL, Literal

from fileshovel.options import FileShovelOptions

log = logging.getLogger("fileshovel.pgsql")

# Applied only where the connection string sets nothing, so a dead server fails instead of hanging.
CONNECTION_DEFAULTS = {
	"connect_timeout": 10,
	"keepalives": 1,
	"keepalives_idle": 30,
	"keepalives_interval": 10,
	"keepalives_count": 3,
}

# (line, offset, values)
Row = Tuple[int, int, list]
# (start index of each value in the statement, (row index, column index) of each value)
ValueMap = Tuple[List[int], List[Tuple[int, int]]]


class InsertError(Exception):
	pass


class PgLineInserter:

	def __init__(self, options: FileShovelOptions):
		self._options = options
		self.server_name_column = None
		self.server_name_value = options.pg_server_name_value
		self.offset_column = Identifier(options.pg_csv_offset_column)
		extra_names = [options.pg_csv_offset_column]

		if options.pg_csv_line_column:
			extra_names.append(options.pg_csv_line_column)

		if options.pg_server_name_column and self.server_name_value:
			self.server_name_column = Identifier(options.pg_server_name_column)
			extra_names.append(options.pg_server_name_column)

		self.extra_columns = [Identifier(x) for x in extra_names]
		self.column_names = list(options.columns) + extra_names
		self.columns = [Identifier(x) for x in self.column_names]

		if options.pg_schema:
			self.table = SQL(".").join([Identifier(options.pg_schema), Identifier(options.pg_table)])
			self.table_name = "%s.%s" % (options.pg_schema, options.pg_table)
		else:
			self.table = Identifier(options.pg_table)
			self.table_name = options.pg_table

		self._connection = self.connect_database()
		self._pending = []
		self._in_flight = None
		self._executor = ThreadPoolExecutor(max_workers=1) if options.pg_threads else None

	def connect_database(self):
		dsn = self._options.pg_connection_string
		given = parse_dsn(dsn)
		defaults = {k: v for k, v in CONNECTION_DEFAULTS.items() if k not in given}

		if libpq_version() >= 120000 and "tcp_user_timeout" not in given:
			defaults["tcp_user_timeout"] = 60000

		return psycopg2.connect(dsn, **defaults)

	def get_last_offset_from_database(self) -> int:
		sql = SQL("SELECT coalesce(max({0}), 0) FROM {1}").format(self.offset_column, self.table)

		if self.server_name_column:
			sql += SQL(" WHERE {0}={1}").format(self.server_name_column, Literal(self.server_name_value))

		with self._connection.cursor() as cursor:
			cursor.execute(sql)
			offset, = cursor.fetchone()

		self._connection.commit()
		return offset

	def add_row(self, line: list, current_line: int, current_line_offset: int):
		try:
			values = self._prepare_row((line, current_line, current_line_offset))
		except IndexError as e:
			raise InsertError("row at offset %d, line %d: %s" % (current_line_offset, current_line, e)) from None

		self._pending.append((current_line, current_line_offset, values))

		if len(self._pending) >= self._options.pg_rows_per_commit:
			self._submit()

	def flush(self):
		"""Insert pending rows and, after catch-up, stop using the worker thread."""
		if self._executor:
			self._wait()
			self._executor.shutdown()
			self._executor = None
			log.info("caught up, inserting synchronously from now on")

		if self._pending:
			self._submit()

	def _submit(self):
		batch, self._pending = self._pending, []

		if self._executor:
			self._wait()
			self._in_flight = self._executor.submit(self._insert_batch, batch)
		else:
			self._insert_batch(batch)

	def _wait(self):
		if self._in_flight:
			in_flight, self._in_flight = self._in_flight, None
			in_flight.result()

	def _prepare_row(self, item):
		line, current_line, current_line_offset = item

		if len(line) + len(self.extra_columns) < len(self.columns):
			if self._options.add_missing_columns:
				while len(line) + len(self.extra_columns) < len(self.columns):
					line.append(None)
			else:
				raise IndexError("Row has %d columns while table has %d." % (len(line), len(self.columns)))

		if self._options.csv_null_text is not None:
			for i, field in enumerate(line):
				if field == self._options.csv_null_text:
					line[i] = None

		line.append(current_line_offset)

		if self._options.pg_csv_line_column:
			line.append(current_line)

		if self.server_name_column:
			line.append(self.server_name_value)

		return line

	def _insert_batch(self, batch: List[Row]):
		first_line, first_offset, _ = batch[0]
		last_line, last_offset, _ = batch[-1]
		sql, value_map = self._render_insert(batch)

		try:
			with self._connection.cursor() as cursor:
				cursor.execute(sql)
				inserted = cursor.rowcount
			self._connection.commit()
		except psycopg2.Error as e:
			raise InsertError("insert into %s failed for offsets %d-%d, lines %d-%d: %s%s" % (
				self.table_name, first_offset, last_offset, first_line, last_line, describe_error(e),
				self._locate_value(e, batch, value_map),
			)) from None

		if inserted < len(batch):
			log.warning("%d of %d rows at offsets %d-%d already in %s, dropped by ON CONFLICT",
				len(batch) - inserted, len(batch), first_offset, last_offset, self.table_name)

		log.info("committed %d rows, offsets %d-%d, lines %d-%d", len(batch), first_offset, last_offset, first_line, last_line)
		time.sleep(self._options.wait_time)

	def _render_insert(self, batch: List[Row]) -> Tuple[str, ValueMap]:
		"""Render the statement and where each value starts in it."""
		head = SQL("INSERT INTO {0} ({1}) VALUES ").format(self.table, SQL(",").join(self.columns))
		pieces = [head.as_string(self._connection)]
		position = len(pieces[0])
		starts, cells = [], []

		for row_index, (_, _, row) in enumerate(batch):
			for column_index, value in enumerate(row):
				if column_index:
					separator = ","
				else:
					separator = ",(" if row_index else "("

				literal = Literal(value).as_string(self._connection)
				position += len(separator)
				starts.append(position)
				cells.append((row_index, column_index))
				pieces += [separator, literal]
				position += len(literal)

			pieces.append(")")
			position += 1

		pieces.append(" ON CONFLICT DO NOTHING")
		return "".join(pieces), (starts, cells)

	def _locate_value(self, e: psycopg2.Error, batch: List[Row], value_map: ValueMap) -> str:
		"""Name the row and column of a rejected literal from the server's error position."""
		starts, cells = value_map
		if not e.diag.statement_position:
			return ""

		index = bisect_right(starts, int(e.diag.statement_position) - 1) - 1
		if index < 0:
			return ""

		row_index, column_index = cells[index]
		line, offset, _ = batch[row_index]
		if column_index < len(self.column_names):
			column = "column %s" % self.column_names[column_index]
		else:
			column = "value %d of %d columns" % (column_index + 1, len(self.column_names))
		return ", row at offset %d, line %d, %s" % (offset, line, column)


def describe_error(e: psycopg2.Error) -> str:
	"""Name the failure without the server's message text, which quotes rejected values."""
	if e.pgcode is None:
		return "%s: %s" % (type(e).__name__, str(e).strip())

	parts = ["%s (SQLSTATE %s)" % (type(e).__name__, e.pgcode)]
	for name in ("column_name", "datatype_name", "constraint_name"):
		value = getattr(e.diag, name)
		if value:
			parts.append("%s %s" % (name.split("_")[0], value))
	return ", ".join(parts)
