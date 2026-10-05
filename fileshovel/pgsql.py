# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

import psycopg2
from psycopg2.sql import Identifier, SQL, Literal

from fileshovel.options import FileShovelOptions

log = logging.getLogger("fileshovel.pgsql")

# (line, offset, values)
Row = Tuple[int, int, list]


class InsertError(Exception):
	pass


class PgLineInserter:

	def __init__(self, options: FileShovelOptions):
		self._options = options
		self.server_name_column = options.pg_server_name_column
		self.server_name_value = options.pg_server_name_value
		self.offset_column = Identifier(options.pg_csv_offset_column)
		self.extra_columns = [self.offset_column]

		if options.pg_csv_line_column:
			self.extra_columns.append(Identifier(options.pg_csv_line_column))

		if self.server_name_column and self.server_name_value:
			self.server_name_column = Identifier(self.server_name_column)
			self.extra_columns.append(self.server_name_column)

		self.columns = [Identifier(x) for x in options.columns] + self.extra_columns

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
		return psycopg2.connect(self._options.pg_connection_string)

	def get_last_offset_from_database(self) -> int:
		with self.connect_database() as pg_connection:
			c = pg_connection.cursor()

			if self.server_name_column:
				sql = SQL("SELECT {0} FROM {1} WHERE {2}={3} ORDER BY {4} DESC LIMIT 1").format(
					self.offset_column,
					self.table,
					self.server_name_column,
					Literal(self.server_name_value),
					self.offset_column,
				)
			else:
				sql = SQL("SELECT {0} FROM {1} ORDER BY {2} DESC LIMIT 1").format(
					self.offset_column,
					self.table,
					self.offset_column,
				)

			sql = sql.as_string(pg_connection)
			c.execute(sql)

			if c.rowcount == 0:
				ret = 0
			else:
				ret = next(c)[0]

			return ret

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
		values = SQL(",").join(
			SQL("(") + SQL(",").join(Literal(x) for x in row) + SQL(")") for _, _, row in batch
		)
		sql = SQL("INSERT INTO {0} ({1}) VALUES {2} ON CONFLICT DO NOTHING").format(
			self.table,
			SQL(",").join(self.columns),
			values,
		)

		try:
			with self._connection.cursor() as cursor:
				cursor.execute(sql)
			self._connection.commit()
		except psycopg2.Error as e:
			raise InsertError("insert into %s failed for offsets %d-%d, lines %d-%d: %s" % (
				self.table_name, first_offset, last_offset, first_line, last_line, describe_error(e),
			)) from None

		log.info("committed %d rows, offsets %d-%d, lines %d-%d", len(batch), first_offset, last_offset, first_line, last_line)
		time.sleep(self._options.wait_time)


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
