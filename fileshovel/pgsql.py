# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import logging
import time
from queue import Queue
from threading import Event, Thread
from typing import List

import psycopg2
from psycopg2.sql import Identifier, SQL, Literal

from fileshovel.options import FileShovelOptions

log = logging.getLogger("fileshovel.pgsql")


class PgLineInserter:

	def __init__(self, options: FileShovelOptions):
		self._options = options
		self.server_name_column = options.pg_server_name_column
		self.server_name_value = options.pg_server_name_value
		self.offset_column = Identifier(options.pg_csv_offset_column)
		self.extra_columns = [self.offset_column]
		self.insert_queue = Queue(maxsize=options.pg_rows_per_commit if options.pg_threads > 0 else 0)
		self.sql_threads = list(self.start_sql_threads(options.pg_threads))
		self.sql_thread_dead = Event()

		if options.pg_csv_line_column:
			self.extra_columns.append(Identifier(options.pg_csv_line_column))

		if self.server_name_column and self.server_name_value:
			self.server_name_column = Identifier(self.server_name_column)
			self.extra_columns.append(self.server_name_column)

		self.columns = [Identifier(x) for x in options.columns] + self.extra_columns

		if options.pg_schema:
			self.table = SQL(".").join([Identifier(options.pg_schema), Identifier(options.pg_table)])
		else:
			self.table = Identifier(options.pg_table)

		self.last_offset = self.get_last_offset_from_database()

		if options.pg_threads > 0:
			for t in self.sql_threads:
				t.start()

	def connect_database(self):
		return psycopg2.connect(self._options.pg_connection_string)

	def start_sql_threads(self, how_many: int) -> List[Thread]:
		for i in range(how_many):
			yield Thread(name="sql_thread%d" % i, target=self._insert_rows, args=(self.insert_queue,))

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
		if self.sql_thread_dead.is_set():
			raise RuntimeError("SQL thread died.")
		self.insert_queue.put((line, current_line, current_line_offset), block=True)

	def pre_commit(self):
		pass

	def post_commit(self):
		pass

	def done(self):
		if self._options.pg_threads > 0:
			for _ in self.sql_threads:
				self.insert_queue.put(None)
			for t in self.sql_threads:
				t.join()
		else:
			self.insert_queue.put(None)
			self._insert_rows(self.insert_queue)

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

		if self._options.pg_csv_offset_column:
			line.append(current_line)

		if self.server_name_column:
			line.append(self.server_name_value)

		return SQL("(") + SQL(",").join((Literal(x) for x in line)) + SQL(")")

	def _insert_rows(self, row_queue: Queue):
		columns = self.columns
		table = self.table
		rows_per_commit = self._options.pg_rows_per_commit

		try:
			log.info("connecting")
			pg_connection = self.connect_database()
			cursor = pg_connection.cursor()
			insert_format = SQL("INSERT INTO {0} ({1}) VALUES {2}")
			do_nothing = SQL(" ON CONFLICT DO NOTHING")
			ending = False
			log.info("connected")
			values = []

			while True:
				item = row_queue.get()

				if item is None:
					ending = True
				else:
					values.append(self._prepare_row(item))

				if len(values) > 0 and (len(values) > rows_per_commit or ending is True or self.insert_queue.qsize() == 0):
					composed = insert_format.format(
						table,
						SQL(",").join(columns),
						SQL(",").join(values),
					) + do_nothing
					sql = composed.as_string(pg_connection)
					log.debug("inserting %d rows", len(values))
					cursor.execute(sql)
					values.clear()
					self.pre_commit()
					pg_connection.commit()
					self.post_commit()
					time.sleep(self._options.wait_time)

				row_queue.task_done()

				if ending:
					break

		finally:
			self.sql_thread_dead.set()
